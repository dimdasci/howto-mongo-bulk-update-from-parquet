#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
VIRTUALENV_DIR := ".venv"
PYTHON_INTERPRETER = python3

VALID_TARGET_ENV := dev qa staging prod

#################################################################################
# SETUP                                                                         #
#################################################################################

## Set up python interpreter environment
create_environment:
	$(PYTHON_INTERPRETER) -m venv $(VIRTUALENV_DIR)
	@echo ">>> New virtualenv created. Activate with:\nsource $(VIRTUALENV_DIR)/bin/activate"

## Install all Python Dependencies
install:
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

.PHONY: create_environment install

#################################################################################
# DEVELOPMENT COMMANDS                                                          #
#################################################################################

## Delete all compiled Python files

clean: clean_py clean_cdk

clean_py:
	find . -type f -name '._*' -delete
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

clean_cdk:
	rm -Rf ./cdk.out/asset*

## Format using Black
format: 
	isort src iac tests --profile black
	black src iac tests

## Lint using flake8
lint:
	flake8 src iac

## Run tests using pytest
tests:
	pytest tests/

.PHONY: clean clean_py clean_cdk_out format lint tests

#################################################################################
# DEPLOYMENT COMMANDS                                                           #
#################################################################################


# validate if target_env is set and has a valid value
validate_target_env:
ifndef target_env
	$(error target_env is not set. Please provide the target_env argument, e.g., make deploy target_env=dev)
endif
ifeq (,$(filter $(target_env),$(VALID_TARGET_ENV)))
	$(error Invalid target_env. Allowed values are: $(VALID_TARGET_ENV))
endif
	$(eval ACCOUNT := $(shell aws sts get-caller-identity --query "Account" --output text --profile $(target_env)))
	$(eval REGION := $(shell aws configure get region --profile $(target_env)))
	@echo "Deploying to $(target_env) environment in region $(REGION) and account $(ACCOUNT)"


auth_docker: validate_target_env
	@echo "   ... Authenticating docker to ECR"
	@aws ecr get-login-password --region $(REGION) --profile $(target_env) \
	| docker login \
		--username AWS \
		--password-stdin $(ACCOUNT).dkr.ecr.$(REGION).amazonaws.com

deploy: auth_docker
# set environment variable JSII_SILENCE_WARNING_DEPRECATED_NODE_VERSION=1 to suppress warning
	@echo "   ... AWS CDK deploy started"
	@JSII_SILENCE_WARNING_DEPRECATED_NODE_VERSION=1 cdk deploy \
	--profile $(target_env) -c target_env=$(target_env) '**'

destroy: validate_target_env
	cdk destroy --profile $(target_env) -c target_env=$(target_env) '**'
	rm -Rf ./cdk.out/asset*

sync_assets: validate_target_env
	aws s3 sync ./assets s3://$(target_env) \
		--delete --profile $(target_env) --exclude ".*" --exclude "*/.*"

.PHONY: validate_target_env auth_docker deploy destroy sync_assets