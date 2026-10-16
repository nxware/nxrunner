
import json
from importlib import resources

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web


def load_resource(name):
    return resources.files('nxrunner').joinpath('resources').joinpath(name).read_text(encoding='utf-8')


class NxRunner(r.AutoDispatcher):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type = 'nxrunner'

    def ui_data(self):
        instances = [{'name': name, 'cls': type(runner).__name__} for name, runner in self.instances.items()]
        classes = [{'name': name, 'cls': str(cls)} for name, cls in self.classes.items() if name not in self.instances]
        return {
            'key': self.key,
            'instances': instances,
            'classes': classes,
            'links': [
                {'title': "Dispatcher", 'url': '/pysys/runner'},
                {'title': "Runner-UI", 'url': '/pysys/runner-ui'},
                {'title': "Registry", 'url': '/pysys/registry'},
                {'title': "Results", 'url': '/pysys/job-results'},
                {'title': "Proxy", 'url': '/pysys/create-proxy'}
            ]
        }

    def query_jumplinks(self):
        return [
            web.a("UI (v2)", '/pysys/ui'),
            *super().query_jumplinks()
        ]

    def ui_runner_page(self, params):
        """
          ?type=promptdb&promptdb=stats -> execute({type: promptdb, page: stats, ...})
        """
        t = params[self.key]
        try:
            result = self.execute({self.key: t, 'page': params.get(t, 'index'), **params})
        except Exception as e:
            result = self.fail(str(e))
        if isinstance(result, dict) and result.get('success') is not False and 'content' in result:
            return {'name': t, 'content': str(result['content'])}
        message = result.get('message', '') if isinstance(result, dict) else str(result)
        return {'name': t, 'error': str(message) or json.dumps(result, default=str)}

    def page_ui(self, params={}):
        page = self.ui_runner_page(params) if self.key in params else None
        data = json.dumps({**self.ui_data(), 'page': page}, default=str).replace('</', '<\\/')
        js = r.js_functions().replace('</', '<\\/')
        return load_resource('ui.html').replace('__JS__', js).replace('__DATA__', data)

    def part_index(self, p: base.Page, params={}):
        super().part_index(p, params)
        p.div(web.a("NxRunner UI", '/pysys/ui'))

    def setupRestApp(self, app: "flask.Flask"):
        super().setupRestApp(app)
        try:
            app.add_url_rule('/pysys/ui', 'nxrunner_ui',
                             view_func=lambda: self.page_ui(self.create_request_params()),
                             methods=['GET', 'POST'])
        except Exception as e:
            self.error(str(e))

