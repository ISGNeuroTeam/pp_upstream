.SILENT:

SHELL = /bin/bash


.PHONY: clean clean_build clean_venv clean_conda clean_pack install_conda install_conda_pack clean_commands

VERSION := $(shell cat setup.py | grep version | head -n 1 | sed -re "s/[^\"']+//" | sed -re "s/[\"',]//g")
BRANCH := $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')

CONDA = conda/miniconda/bin/conda
ENV_PYTHON = venv/bin/python3.9


all:
	echo -e "Required section:\n\
 pack - build archive\n\
 clean - clean all addition file, virtual environment directory, output archive file\n\
 test - run all tests\n\
Addition section:\n\
 venv -  create python virtual environment for develop \n\
 install_conda - install miniconda in local directory \n\
 install_conda_pack - install conda pack in local directory\n\
"
conda/miniconda.sh:
	echo Download Miniconda
	mkdir -p conda
	wget https://repo.anaconda.com/miniconda/Miniconda3-py39_23.1.0-1-Linux-x86_64.sh -O conda/miniconda.sh; \

conda/miniconda: conda/miniconda.sh
	bash conda/miniconda.sh -b -p conda/miniconda; \

install_conda: conda/miniconda

conda/miniconda/bin/conda-pack: conda/miniconda
	conda/miniconda/bin/conda install conda-pack -c conda-forge  -y

install_conda_pack: conda/miniconda/bin/conda-pack

clean_conda:
	rm -rf ./conda

venv: conda/miniconda
	echo Create environment
	$(CONDA) create --copy -p venv -y
	$(CONDA) install -p venv python==3.9.7 -y
	$(CONDA) install -p venv wheel==0.37.1 -y
	$(ENV_PYTHON) -m pip install --no-input -r ./requirements.txt  --extra-index-url http://s.dev.isgneuro.com/repository/ot.platform/simple --trusted-host s.dev.isgneuro.com

clean_venv:
	rm -rf ./venv

test: venv
	echo Run unittests

venv.tar.gz: venv conda/miniconda/bin/conda-pack
	rm -rf ./venv.tar.gz
	./conda/miniconda/bin/conda-pack -p ./venv -o ./venv.tar.gz

build: venv.tar.gz
	mkdir -p ./build
	cp -r pp_upstream/commands/* ./build/
	mkdir -p ./build/upstream_venv
	tar -xzf venv.tar.gz -C ./build/upstream_venv
	cp pp_upstream/config.py ./build/upstream_venv/config.py
	cp pp_upstream/*.yaml ./build/upstream_venv/
	cp *.md ./build/upstream_venv/

commands.tar.gz:
	mkdir -p ./build_commands
	cp -r pp_upstream/commands/* ./build_commands/
	cd build_commands; tar czf ../commands.tar.gz *

clean_commands:
	rm -rf ./build_commands
	rm -f ./commands.tar.gz

clean_build:
	rm -rf ./build

pack: build
	rm -f pp_upstream-*.tar.gz
	echo Create archive \"pp_upstream-$(VERSION)-$(BRANCH).tar.gz\"
	cd build; tar czf ../pp_upstream-$(VERSION)-$(BRANCH).tar.gz *

clean_pack:
	rm -rf ./venv.tar.gz
	rm -rf ./pp_upstream*.tar.gz

clean: clean_venv clean_build clean_pack clean_conda clean_commands
