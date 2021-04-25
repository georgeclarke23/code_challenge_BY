-include .env
export

help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "build - package"

all: default

default: clean deps-dev deps tests fmt run

.venv:
	if [ ! -e ".venv/bin/activate_this.py" ] ; then virtualenv --clear .venv ; fi

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr src/libs.zip
	rm -fr src/jobs.zip

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '.pytest_cache' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

deps: .venv
	. .venv/bin/activate && pip install -U -r requirements.txt -t ./src/libs

deps-dev: .venv
	. .venv/bin/activate && pip install -U -r requirements-dev.txt

fmt:
	. .venv/bin/activate && pylint -r n src/main.py src/jobs

test:
	. .venv/bin/activate && pytest ./tests/* -vv

build: clean deps
	cd ./src && zip -x main.py -x \*libs\* -r ./jobs.zip .
	cd ./src/libs && zip -r ../libs.zip .

run/spark: build
	cd src && spark-submit --py-files jobs.zip,libs.zip main.py --job code_challenge

run: clean .venv
	. .venv/bin/activate && cd src &&  python3 main.py --job code_challenge

docker/run: clean
	docker-compose up --build