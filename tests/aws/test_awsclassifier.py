import json
from unittest import mock

import pytest
import requests

from eodhp_utils.aws.egress_classifier import AWSIPClassifier, EgressClass

FAKE_DATA = {
    "prefixes": [
        # In-region prefixes (eu-west-2)
        {
            "service": "AMAZON",
            "region": "eu-west-2",
            "ip_prefix": "10.1.0.0/16",
            "ipv6_prefix": "2001:db8::/32",
        },
        # Other-region prefix
        {
            "service": "AMAZON",
            "region": "us-east-1",
            "ip_prefix": "192.168.0.0/16",
            "ipv6_prefix": None,
        },
    ]
}


@pytest.fixture
def ip_data_file(tmp_path):
    path = tmp_path / "ips.json"
    path.write_text(json.dumps(FAKE_DATA))
    return str(path)


def test_ipv4_and_ipv6_classification(requests_mock):
    requests_mock.get("https://ip-ranges.amazonaws.com/ip-ranges.json", json=FAKE_DATA)

    clf = AWSIPClassifier(current_region="eu-west-2")

    # IPv4 tests
    assert clf.classify("10.1.2.3") == EgressClass.REGION
    assert clf.classify("192.168.1.1") == EgressClass.INTERREGION
    assert clf.classify("8.8.8.8") == EgressClass.INTERNET

    # IPv6 tests
    assert clf.classify("2001:db8::1") == EgressClass.REGION
    assert clf.classify("2001:db9::1") == EgressClass.INTERNET


def test_aws_ip_file_loaded_and_used(requests_mock):
    requests_mock.get("https://ip-ranges.amazonaws.com/ip-ranges.json", json=FAKE_DATA)

    clf = AWSIPClassifier(current_region="eu-west-2")
    assert clf.classify("10.1.2.3") == EgressClass.REGION


def test_fallback_file_loaded_and_used_when_aws_fails(requests_mock, ip_data_file):
    requests_mock.get(
        "https://ip-ranges.amazonaws.com/ip-ranges.json", exc=requests.exceptions.ConnectTimeout
    )

    clf = AWSIPClassifier(cache_file=ip_data_file, current_region="eu-west-2")
    assert clf.classify("10.1.2.3") == EgressClass.REGION


def test_bundled_file_loaded_and_used_when_aws_fails_and_no_cache(requests_mock):
    requests_mock.get(
        "https://ip-ranges.amazonaws.com/ip-ranges.json", exc=requests.exceptions.ConnectTimeout
    )

    clf = AWSIPClassifier(cache_file=".nonexistent-file", current_region="eu-west-2")
    assert clf.classify("3.4.12.4") == EgressClass.INTERREGION


def test_bundled_file_loaded_and_used_when_aws_fails_and_invalid_cache(requests_mock, tmp_path):
    requests_mock.get(
        "https://ip-ranges.amazonaws.com/ip-ranges.json", exc=requests.exceptions.ConnectTimeout
    )

    testfile = str(tmp_path / "test-file.json")
    with open(testfile, "w") as f:
        f.write("not json")

    clf = AWSIPClassifier(cache_file=testfile, current_region="eu-west-2")
    assert clf.classify("3.4.12.4") == EgressClass.INTERREGION


def test_fallback_file_loaded_and_used_when_aws_fails_second_time(requests_mock, tmp_path):
    requests_mock.get("https://ip-ranges.amazonaws.com/ip-ranges.json", json=FAKE_DATA)
    cachefile = str(tmp_path / "cache-file.json")

    clf = AWSIPClassifier(cache_file=cachefile, current_region="eu-west-2")
    assert clf.classify("10.1.2.3") == EgressClass.REGION

    requests_mock.get(
        "https://ip-ranges.amazonaws.com/ip-ranges.json", exc=requests.exceptions.ConnectTimeout
    )
    clf = AWSIPClassifier(cache_file=cachefile, current_region="eu-west-2")
    assert clf.classify("10.1.2.3") == EgressClass.REGION


def test_aws_data_refetched_after_24_hours(requests_mock, tmp_path):
    requests_mock.get("https://ip-ranges.amazonaws.com/ip-ranges.json", json=FAKE_DATA)
    cachefile = str(tmp_path / "cache-file.json")

    with mock.patch("eodhp_utils.aws.egress_classifier.time.time") as time_mock:
        time_mock.return_value = 1000000
        clf = AWSIPClassifier(cache_file=cachefile, current_region="eu-west-2")
        assert clf.classify("10.1.2.3") == EgressClass.REGION

        requests_mock.get("https://ip-ranges.amazonaws.com/ip-ranges.json", json={"prefixes": []})

        time_mock.return_value = 1000000 + 86399
        assert clf.classify("10.1.2.3") == EgressClass.REGION

        time_mock.return_value = 1000000 + 86401
        assert clf.classify("10.1.2.3") == EgressClass.INTERNET
