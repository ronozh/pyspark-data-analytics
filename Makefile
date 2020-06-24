.PHONY: help
help:  ## print help for makefile
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\-]*: *.*## *" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

venv:  ## create a python3 venv and pip install requirements
	python3.8 -m venv venv
	$(MAKE) install

install:  ## pip install requirements.txt in venv
	. ./venv/bin/activate; \
	pip install -U pip; \
	pip install -r requirements.txt