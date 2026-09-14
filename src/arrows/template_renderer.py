from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template


class TemplateRenderer:
    def __init__(self, scripts_folder_path):
        self.scripts_folder_path = scripts_folder_path
        self.scripts_folder = Path(self.scripts_folder_path)

    def render_template(self, filename, **kwargs):
        # Templates are SQL and email bodies authored by the caller, not untrusted
        # input, so HTML autoescaping would corrupt them.
        env = Environment(loader=FileSystemLoader(self.scripts_folder))  # noqa: S701
        template = env.get_template(filename)
        return template.render(**kwargs)
    
    
def render_template(file_path, **kwargs):
    with open(file_path) as file:
        content = Template(file.read()).render(**kwargs)
    return content