from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from common.utils.singleton import Singleton
from monitoring.service.i_template_renderer.template_renderer import ITemplateRenderer


class Jinja2TemplateRenderer(ITemplateRenderer, metaclass=Singleton):
    """
    Jinja2로 템플릿을 렌더링하는 구현체.
    """

    DEFAULT_TEMPLATE_PATH = Path(__file__).parent / "template"

    def __init__(self):
        self.env = Environment(loader=FileSystemLoader(self.DEFAULT_TEMPLATE_PATH))

    def render(self, template_name: str, context: dict) -> str:
        tpl = self.env.get_template(template_name)
        return tpl.render(**context)
