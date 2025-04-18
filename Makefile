SHELL:=/bin/bash
install:
	source ./.venv/bin/activate && \
	pip install -r requirements.txt

run:
	source ./.venv/bin/activate && \
	jupyter notebook --no-browser

# CREATE VENV
# python -m venv .venv

# START VENV
# source ./.venv/bin/activate

# STOP VENV
# deactivate
