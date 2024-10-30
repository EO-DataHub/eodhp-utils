.PHONY: dockerbuild dockerpush test testonce ruff black lint isort pre-commit-check requirements-update requirements setup
VERSION ?= latest
IMAGENAME = eodhp-utils
DOCKERREPO ?= public.ecr.aws/n1b3o1k2/ukeodhp

test:
	./venv/bin/ptw tests

testonce:
	./venv/bin/pytest

ruff:
	./venv/bin/ruff check .

black:
	./venv/bin/black .

isort:
	./venv/bin/isort . --profile black

validate-pyproject:
	validate-pyproject pyproject.toml

lint: ruff black isort validate-pyproject

requirements.txt: venv pyproject.toml
	./venv/bin/pip-compile

requirements-dev.txt: venv pyproject.toml
	./venv/bin/pip-compile --extra dev -o requirements-dev.txt

requirements: requirements.txt requirements-dev.txt

requirements-update: venv
	./venv/bin/pip-compile -U
	./venv/bin/pip-compile --extra dev -o requirements-dev.txt -U

venv:
	# You may need: sudo apt install python3.12-dev python3.12-venv
	python3.12 -m venv venv --upgrade-deps
	./venv/bin/pip3 install pip-tools

.make-venv-installed: venv requirements.txt requirements-dev.txt
	./venv/bin/pip3 install -r requirements.txt -r requirements-dev.txt
	touch .make-venv-installed

.git/hooks/pre-commit:
	./venv/bin/pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

setup: venv requirements .make-venv-installed .git/hooks/pre-commit
