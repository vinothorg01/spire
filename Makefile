.PHONY: clean lint requirements update_settings_schema update_settings_synctos3 update_settings_syncfroms3

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = cn-falcon
PROFILE = default
PROJECT_NAME = falcon
PYTHON_INTERPRETER = python3

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install Python Dependencies
requirements: test_environment
	pip install -U pip
	pip install -r requirements.txt
	pip install -r requirements-local.txt

## Update the settings and Protobuf schemas for Falcon
update_settings_schema:
	$(PYTHON_INTERPRETER) falcon/update_settings_schema.py --schema --mode=$(MODE) falcon/settings.yaml

# sync_to_s3 does not need a MODE, MODE entered for settings.yaml upload
update_settings_synctos3:
	$(PYTHON_INTERPRETER) falcon/update_settings_schema.py --mode=$(MODE) --sync_to_s3 falcon/settings.yaml

# sync_from_s3 does not need a MODE, MODE entered for settings.yaml upload
update_settings_syncfroms3:
	$(PYTHON_INTERPRETER) falcon/update_settings_schema.py --mode=$(MODE) --sync_from_s3 falcon/settings.yaml

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -name .DS_Store -delete
	find . -path "*/__pycache__/*" -delete
	find . -type d -name '__pycache__' -empty -delete
	find . -path "*/.ipynb_checkpoints/*" -delete
	find . -type d -name '.ipynb_checkpoints' -empty -delete

## Lint using flake8
lint:
	flake8 src

## Upload Data to S3
# Deprecated using update_settings_schema_sync_to_s3
# sync_data_to_s3:
# ifeq (default,$(PROFILE))
# 	aws s3 sync data/ s3://$(BUCKET)/codedata/
# 	aws s3 sync models/ s3://$(BUCKET)/models/
# else
# 	aws s3 sync data/ s3://$(BUCKET)/codedata/ --profile $(PROFILE)
# 	aws s3 sync models/ s3://$(BUCKET)/models/ --profile $(PROFILE)
# endif

## Download Data from S3
# Deprecated using update_settings_schema_sync_from_s3
# sync_data_from_s3:
# ifeq (default,$(PROFILE))
# 	aws s3 sync s3://$(BUCKET)/codedata/ data/
# 	aws s3 sync s3://$(BUCKET)/models/ models/
# else
# 	aws s3 sync s3://$(BUCKET)/codedata/ data/ --profile $(PROFILE)
# 	aws s3 sync s3://$(BUCKET)/models/ models/ --profile $(PROFILE)
# endif

## Set up python interpreter environment
create_environment:
ifeq (True,$(HAS_CONDA))
		@echo ">>> Detected conda, creating conda environment."
ifeq (3,$(findstring 3,$(PYTHON_INTERPRETER)))
	conda create --name $(PROJECT_NAME) python=3.6 ipykernel
else
	conda create --name $(PROJECT_NAME) python=2.7 ipykernel
endif
		@echo ">>> New conda env created. Activate with:\nsource activate $(PROJECT_NAME)"
else
	@pip install -q virtualenv virtualenvwrapper
	@echo ">>> Installing virtualenvwrapper if not already intalled.\nMake sure the following lines are in shell startup file\n\
	export WORKON_HOME=$$HOME/.virtualenvs\nexport PROJECT_HOME=$$HOME/Devel\nsource /usr/local/bin/virtualenvwrapper.sh\n"
	@bash -c "source `which virtualenvwrapper.sh`;mkvirtualenv $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER)"
	@echo ">>> New virtualenv created. Activate with:\nworkon $(PROJECT_NAME)"
endif

## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
