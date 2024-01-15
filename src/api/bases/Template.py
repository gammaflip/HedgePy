import json
import jsonschema
from pathlib import Path
from src import config
from typing import Optional


ROOT = Path(config.PROJECT_ENV["ROOT"]) / "src" / "api" / "templates"
VALIDATOR = jsonschema.Draft202012Validator


def validate(schema: dict, instance: Optional[dict] = None) -> bool:
    valid = True

    if instance:
        try:
            jsonschema.validate(
                instance=instance,
                schema=schema,
                cls=VALIDATOR,
                format_checker=VALIDATOR.FORMAT_CHECKER
            )
        except jsonschema.exceptions.ValidationError as e:
            valid = False

    else:
        try:
            VALIDATOR.check_schema(schema)
        except jsonschema.exceptions.SchemaError as e:
            valid = False

    return valid


def get_template(template: str) -> dict:
    with open(ROOT / f"{template}.json") as file:
        template = json.load(file)

    if validate(template):
        return template

    else:
        raise ValueError("Template failed validation")


def get_schema(template: str, schema: str) -> dict:
    with open(ROOT / template / f"{schema}.json") as file:
        schema = json.load(file)

    template_schema = get_template(template)

    if validate(template_schema, schema):
        return schema

    else:
        raise ValueError("Schema failed validation")


def get_templates() -> dict:
    return dict(
        zip(
            map(lambda x: x.stem, ROOT.glob("*.json")),
            map(lambda x: get_template(x.stem), ROOT.glob("*.json"))
        )
    )


def get_schemas(template: str) -> dict:
    return dict(
        zip(
            map(lambda x: x.stem, (ROOT / template).glob("*.json")),
            map(lambda x: get_schema(template, x.stem), (ROOT / template).glob("*.json"))
        )
    )
