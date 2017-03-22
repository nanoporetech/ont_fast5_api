ifndef PYTHON
PYTHON=$(shell which python3)
endif
LATEST_TAG := $(shell git tag --list release_* | sort -V | tail -n 1 | sed "s/release[-_]\([\.0-9]*\)/\1/g")
VERSION_CODE=${shell lsb_release -cs}

all:
	@echo "make clean - remove build dirs and pyc files"
	@echo "make doc - build Sphinx docs. Requires the ability to find dependency libraries (e.g. dragonet / chimaera)"
	@echo "make test - run tests"
	@echo "make get_latest_tag - look up the latest release and check out that version"
	@echo "make builddeb - generate a deb package"
	@echo "make develop - set up repo for local use"
	@echo "make develop_uninstall - remove easy_install hook for the repo"

clean:
	$(PYTHON) setup.py clean
	rm -rf deb_dist ont_fast5_api.egg-info dist ont_fast5_api-*.tar.gz
	find . -name '*.pyc' -delete

doc:
	cd docs && $(MAKE) api && $(MAKE) html VERSION=$(LATEST_TAG)

test:
	$(PYTHON) -m unittest discover

get_latest_tag:
	git checkout release_$(LATEST_TAG)

builddeb: clean
	$(PYTHON) setup.py --command-packages=stdeb.command sdist_dsc --debian-version 1~$(VERSION_CODE) bdist_deb

develop:
	$(PYTHON) setup.py develop --user || $(PYTHON) setup.py develop

develop_uninstall:
	$(PYTHON) setup.py develop --user --uninstall || $(PYTHON) setup.py develop --uninstall

.PHONY : all clean doc test get_latest_tag builddeb develop develop_uninstall
