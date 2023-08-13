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
	# this fixes unquoted references to classes
	#sed -i '' 's/Dict\[str, Value\],/Dict[str, "Value"],/g' $@
	#sed -i '' 's/ValueWithAlias,/"ValueWithAlias",/g' $@
	quicktype -o $@ $(QUICKTYPE_FLAGS) $<

$(QUICKTYPE_OUTPUT_DIR)/%.json:
	cargo run --bin generate_json_schemas

python:
	cd fexpress-py && maturin build --release -i python
	pip install target/wheels/*.whl --force-reinstall
	cd fexpress-py && pytest tests
	cp README.md fexpress-py/

python_publish:
	cd fexpress-py && maturin publish

website:
	cd website && npm run build && cd build && gsutil -m cp -r . gs://feature-express-website
