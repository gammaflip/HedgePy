import json
import jsonschema
from pathlib import Path
from src import config
from collections import UserDict


TEMPLATE_DIR = Path(config.PROJECT_ENV["ROOT"]) / "src" / "api" / "templates"
with open(TEMPLATE_DIR / "meta" / "template.json") as fp:
    TEMPLATE_SCHEMA = json.load(fp)

_VALIDATOR = jsonschema.Draft202012Validator

try:
    _VALIDATOR.check_schema(TEMPLATE_SCHEMA)
except jsonschema.exceptions.SchemaError as e:
    raise ValueError("Meta template schema failed validation:"
                     f"{e.message}")


def load(template: str) -> UserDict:
    with open(TEMPLATE_DIR / f"{template}.json") as fp:
        return json.load(fp)


def validate(template: UserDict) -> bool:
    try:
        jsonschema.validate(
            instance=template,
            schema=TEMPLATE_SCHEMA,
            cls=_VALIDATOR,
            format_checker=_VALIDATOR.FORMAT_CHECKER
        )
        return True
    except jsonschema.exceptions.ValidationError:
        return False


class Template(UserDict):
    def __init__(self, name: str):
        self._name = name
        template = load(name)
        if not validate(template):
            raise ValueError("Template failed validation")
        super().__init__(template)

    def __getitem__(self, key: int) -> dict:
        return self.data["template"][key]

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __len__(self) -> int:
        return len(self.data["template"])

    def __iter__(self):
        return iter(self.data["template"])

    @property
    def name(self) -> str:
        return self._name


def templates() -> dict:
    template_names = tuple(map(lambda t: getattr(t, 'stem'), TEMPLATE_DIR.glob("*.json")))
    return dict(zip(template_names, map(Template, template_names)))
