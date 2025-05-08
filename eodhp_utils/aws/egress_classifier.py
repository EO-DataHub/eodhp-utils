import json
import logging
import os
import random
import time
from enum import Enum
from os.path import dirname
from typing import Optional, Tuple

import boto3.session
import requests
import SubnetTree

logger = logging.getLogger(__name__)

ONE_DAY = 86400


class EgressClass(Enum):
    REGION = "EGRESS-REGION"
    INTERREGION = "EGRESS-INTERREGION"
    INTERNET = "EGRESS-INTERNET"


class AWSIPClassifier:
    """
    An AWSIPClassifier can classify an IP address according to how AWS will charge for egress to
    that IP. It does this by reading AWS's IP list and determining whether the traffic is
    within-region, cross-region or to the internet.

    If no current region is given then the code will attempt to detect one. Currently that's via
    the AWS_DEFAULT_REGION environment variable which is set automatically in EKS. A ValueError
    is raised if that is not possible.

    The IP ranges data from AWS will be saved in /var/cache/eodhp if that's mounted and used if
    the AWS endpoint is not available. If that file is also not available then a bundled copy
    in this package will be used.

    Note that:
      - This cannot differentiate cross-AZ traffic from intra-AZ traffic.
      - Charges vary depending on service and direction.
      - There may be NAT charges as well.

    Example usage:
        classifier = AWSIPClassifier()
        egress_class = classifier.classify("52.93.153.170")  # Returns an EgressClass
    """

    aws_ip_data_url: str
    current_region: str
    fallback_file: str

    def __init__(
        self,
        current_region: str = None,
        url: str = "https://ip-ranges.amazonaws.com/ip-ranges.json",
        cache_file: str = "/var/cache/eodhp/eodhp-utils/ip-ranges.json",
    ):
        if current_region is None:
            # This uses AWS_DEFAULT_REGION from the environment.
            #
            # Fetching http://169.254.169.254/latest/meta-data/placement/availability-zone is an
            # alternative if this proves inadequate.
            current_region = boto3.session.Session().region_name

            if not current_region:
                raise ValueError("Either current_region or AWS_DEFAULT_REGION must be set.")

        self.aws_ip_data_url = url
        self.current_region = current_region
        self.fallback_file = cache_file

        self._build_tree()

    def _build_tree(self):
        ip_data = (
            self._get_ip_data_from_aws()
            or self._get_cached_ip_data()
            or self._get_bundled_ip_data()
        )

        self.current_tree, self.aws_tree = self.build_trees(ip_data, self.current_region)
        self.load_time = time.time()

    def _get_ip_data_from_aws(self) -> Optional[dict]:
        logger.debug("Fetching AWS IP address file from %s", self.aws_ip_data_url)

        try:
            response = requests.get(self.aws_ip_data_url)
            response.raise_for_status()

            logger.info("Fetched AWS IP address file from %s", self.aws_ip_data_url)
            result_data = response.json()
            assert isinstance(result_data, dict)

            self._write_cache_file(response.content)

            return result_data
        except requests.RequestException:
            logger.exception("Failed to fetch AWS IP address file at %s", self.aws_ip_data_url)
            return None

    def _get_cached_ip_data(self) -> Optional[dict]:
        try:
            with open(self.fallback_file, "r") as f:
                ip_data = json.load(f)
                return ip_data if isinstance(ip_data, dict) else None
        except (json.JSONDecodeError, IOError):
            logger.warning(
                "Invalid or inaccessible AWS IP ranges cache file, %s. Ignoring.",
                self.fallback_file,
            )
            return None

    def _get_bundled_ip_data(self) -> dict:
        bundled_file = dirname(__file__) + os.sep + "ip-ranges.json"
        with open(bundled_file, "r") as f:
            raw = f.read()
            return json.loads(raw)

    def _write_cache_file(self, content: bytes):
        if not os.access(dirname(self.fallback_file), 0):
            logger.warning("Cache directory %s does not exist - not caching", self.fallback_file)
            return

        tmpfile = self.fallback_file + "-new-" + str(random.random())
        with open(tmpfile, "wb") as f:
            f.write(content)

        # This is atomic in Unix so nothing can see a partial file.
        os.replace(tmpfile, self.fallback_file)

        logging.info("Wrote cache file %s", self.fallback_file)

    @staticmethod
    def build_trees(
        ip_data: dict, current_region: str
    ) -> Tuple[SubnetTree.SubnetTree, SubnetTree.SubnetTree]:
        """
        Builds two SubnetTree objects:
        - current_tree: ranges for the specified current_region
        - aws_tree: ranges for all other regions

        Includes both IPv4 (ip_prefix) and IPv6 (ipv6_prefix).
        """
        current_tree = SubnetTree.SubnetTree()
        aws_tree = SubnetTree.SubnetTree()
        for prefix in ip_data.get("prefixes", []):
            region = prefix.get("region", "")

            # IPv4
            cidr4 = prefix.get("ip_prefix")
            if cidr4:
                if region == current_region:
                    current_tree[cidr4] = EgressClass.REGION.value
                else:
                    aws_tree[cidr4] = EgressClass.INTERREGION.value

            # IPv6
            cidr6 = prefix.get("ipv6_prefix")
            if cidr6:
                if region == current_region:
                    current_tree[cidr6] = EgressClass.REGION.value
                else:
                    aws_tree[cidr6] = EgressClass.INTERREGION.value

        return current_tree, aws_tree

    def classify(self, client_ip: str) -> EgressClass:
        if self.load_time < time.time() - ONE_DAY:
            self._build_tree()

        if client_ip in self.current_tree:
            return EgressClass.REGION
        elif client_ip in self.aws_tree:
            return EgressClass.INTERREGION
        else:
            return EgressClass.INTERNET
