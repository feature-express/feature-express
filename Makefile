# Variables
PYTHON_VERSION = 3.7
QUICKTYPE_FLAGS = --python-version $(PYTHON_VERSION) -s schema
QUICKTYPE_OUTPUT_DIR = fexpress-py/fexpress/sdk
JSON_SCHEMAS = observation_dates_config event query_config event_scope_config

# Phony Targets
.PHONY: sdk python website

# Rules
sdk: $(addprefix $(QUICKTYPE_OUTPUT_DIR)/, $(addsuffix .py, $(JSON_SCHEMAS)))

$(QUICKTYPE_OUTPUT_DIR)/%.py: $(QUICKTYPE_OUTPUT_DIR)/%.json
	datamodel-codegen --input $< --output $@ --output-model-type dataclasses.dataclass
	quicktype -o $@ $(QUICKTYPE_FLAGS) $<

$(QUICKTYPE_OUTPUT_DIR)/%.json:
	cargo run --bin generate_json_schemas

python:
	rm -f target/wheels/*.whl
	cd fexpress-py && maturin build --release -i python
	pip install target/wheels/*.whl --force-reinstall
	cd fexpress-py && pytest tests
	cp README.md fexpress-py/

python_debug:
	cd fexpress-py && maturin develop

python_debug_docker:
	bash -c "cd /app/ && source .venv/bin/activate && cd fexpress-py && maturin develop"

python_profile_docker:
	bash -c "cd /app/ && source .venv/bin/activate && cd examples/kaggle_notebooks/weather && py-spy record --native -n -o /app/artifacts/profile.svg -- python feature-express-weather.py"

python_publish:
	cd fexpress-py && maturin publish

python_profile:
	cd examples/kaggle_notebooks/weather && sudo py-spy record -n -o profile.svg -- python feature-express-weather.py

website:
	echo "Pulling the notebcd ook from Kaggle"
	cd examples/kaggle_notebooks/ && kaggle kernels pull paweljankiewicz/feature-express-weather -p weather
	echo "Converting to markdown"
	cd examples/kaggle_notebooks/weather/ && jupyter nbconvert --to markdown --execute feature-express-weather.ipynb
	echo "Copying to the docs as an example"
	cp examples/kaggle_notebooks/weather/feature-express-weather.md website/docs/examples/weather.md
	sed -i '' '/<style scoped>/,/<\/style>/d' website/docs/examples/weather.md
	sed -i '' 's/style="[^"]*"//g' website/docs/examples/weather.md
	cd website && npm run build && cd build && gsutil -m rsync -r . gs://feature-express-website

before_push:
	cargo fmt
	cargo test
