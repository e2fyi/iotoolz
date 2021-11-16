.FORCE:

requirements:
	poetry export -f requirements.txt --output requirements.txt --extras all
	poetry export -f requirements.txt --output requirements-dev.txt --dev --extras all

docs: .FORCE requirements
	poetry run mkdocs build

check:
	poetry run isort -c iotoolz
	poetry run black --check iotoolz

check-codes:
	set -e
	poetry run flake8
	# WIP
	poetry run pylint iotoolz --disable=missing-function-docstring,missing-class-docstring,missing-function-docstring,missing-module-docstring,duplicate-code
	poetry run mypy iotoolz
	poetry run bandit -r iotoolz/ -x *_test.py
	# poetry run safety check

test: check check-codes
	set -e
	poetry run pytest --cov

coveralls: test
	poetry run coveralls

serve-docs: docs
	cd site/  && poetry run python -m http.server 8000

format:
	poetry run autoflake -i -r ./iotoolz --remove-all-unused-imports --ignore-init-module-imports --expand-star-imports
	poetry run isort iotoolz
	poetry run black iotoolz
