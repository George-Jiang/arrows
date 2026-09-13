from jinja2 import Template, Environment, FileSystemLoader
from pathlib import Path


class TemplateRenderer():
    def __init__(self, scripts_folder_path):
        self.scripts_folder_path = scripts_folder_path
        self.scripts_folder = Path(self.scripts_folder_path)

    def render_template(self, filename, **kwargs):
        env = Environment(loader=FileSystemLoader(self.scripts_folder))
        template = env.get_template(filename)
        return template.render(**kwargs)
    
    
def render_template(file_path, **kwargs):
    with open(file_path) as file:
        content = Template(file.read()).render(**kwargs)
    return content