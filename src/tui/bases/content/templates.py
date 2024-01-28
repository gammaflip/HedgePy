import json
from src import config
from src.api.bases import Template
from pathlib import Path
from typing import Iterable
from textual import on
from textual.reactive import reactive
from textual.app import ComposeResult
from textual.widgets import TabPane, TextArea, DirectoryTree
from textual.containers import Horizontal, Vertical


class TemplateTabTree(DirectoryTree):
    def __init__(self):
        super().__init__(path=Path(config.PROJECT_ENV['ROOT']) / 'src' / 'api' / 'templates')

    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return filter(lambda p: p.suffix == '.json', paths)


class TemplateTabTextArea(TextArea):
    def __init__(self):
        super().__init__(language="json")


class TemplateTab(TabPane):
    def __init__(self):
        super().__init__('templates')

    def compose(self) -> ComposeResult:
        with Horizontal(id="TemplateTabOuterLayout"):
            yield TemplateTabTree()
            with Vertical(id="TemplateTabInnerLayout"):
                yield TemplateTabTextArea()

    @on(DirectoryTree.FileSelected)
    def load_template(self, message: DirectoryTree.FileSelected):
        template_name = message.path.stem
        template = Template.get_template(template_name)
        text = json.dumps(template, indent=4)
        text_area = self.query_one(TemplateTabTextArea)
        text_area.load_text(text)
