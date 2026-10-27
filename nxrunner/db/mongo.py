
import json
import glob
import os
import base64

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class MongoDB(r.BaseJobExecutor):
    """
     cfg mongo_url
    """
    MODULES = ['pymongo']

    def __init__(self, type='mongodb', connection_url=None, args: u.Args = None):
        super().__init__('mongodb')
        self.tick_count = 0
        self.ticker = None
        self.ticker_interval = 623
        self.define_vars('tick_count', 'ticker_interval')
        self.define_sig(d.PStr('op', 'insert_one'), d.PStr('db', 'main'), d.PStr('collection', 'log'))
        self.define_sig(d.PStr('op', 'find'), d.Param('db', 'str'), d.Param('collection', 'str'), d.Param('q', 'dict'))
        self.define_sig(d.PStr('op', 'jobs'))
        self.define_sig(d.PStr('op', 'collect_files'))
        self.define_sig(d.PStr('op', 'set_ticker'), d.PInt('value', 600))
        self.define_sig(d.PStr('op', 'stop_ticker'))
        self.define_sig(d.PStr('op', 'done_writer'), d.PStr('emitter', ''))
        self.define_sig(d.PStr('op', 'enqueue'), d.PStr('jobdef'))
        self.define_sig(d.PStr('op', 'delete_doc'), d.PStr('db'), d.PStr('collection'), d.PStr('id'))
        if args is None:
            args = u.Args()
        if connection_url is None:
            connection_url = args.get('mongo_url', "mongodb://localhost:27017/")
        import pymongo
        from bson import json_util
        self.json_util = json_util
        self.client = pymongo.MongoClient(connection_url)
        self.args = args
        self.ticker_interval = int(args.get('mongo_job_cron', self.ticker_interval))
        if 'mongo_job_cron' in args:
            self.ticker = self.periodic(self.ticker_interval, self.tick)

    def set_ticker_interval(self, value):
        self.emit_var_change('ticker_interval', value)
        self.ticker_interval = int(value)
        if self.ticker is not None:
            self.ticker.time_s = self.ticker_interval

    def execute_set_ticker(self, data: dict):
        self.set_ticker_interval(data['value'])
        if self.ticker is None:
            self.ticker = self.periodic(self.ticker_interval, self.tick)
        return self.success(ui_reload=True)

    def tick(self):
        self.tick_count += 1
        self.execute_jobs()

    def list_database_names(self):
        return self.client.list_database_names()

    def list_collection_names(self, db):
        return self.client[db].list_collection_names()

    def find(self, db, collection_name, q={}, **kwargs):
        mdb = self.client[db]
        res = mdb.get_collection(collection_name).find(q)
        if 'limit' in kwargs:
            res = res.limit(kwargs['limit'])
        return res

    def insert_one(self, db, collection_name, data):
        col = self.client[db][collection_name]
        x = col.insert_one(data)
        return x

    def delete_one(self, db, collection_name, q):
        col = self.client[db][collection_name]
        return col.delete_one(q)

    def part_index(self, p: b.Page, params={}):
        with p.section(h="Databases"):
            p.ul(map(lambda x: w.a(str(x), self.link(self.part_db, db=str(x))), self.list_database_names()))
        with p.section(h="nweb"):
            p(w.a("Jobs", self.link(self.part_npy)))
            p.prop("Ticker Active", self.ticker is not None)
            p(self.action_btn(dict(type=self.type, op='jobs', title="Fetch Jobs & Execute")))

    def create_done_writer(self, emitter: str = None):
        """
          Schreibt Ergebnisse nach nweb.results
          :param emitter: type eines Emitters, dessen on_emit-Event verwendet wird, sonst der 'done'-Runner
        """
        def process_done(data):
            self.insert_one('nweb', 'results', self.bson_safe(dict(data)))
            return self.success()
        root = self.getRoot().jobexecutor
        if emitter:
            if emitter not in root:
                return self.fail(f"Emitter {emitter} not found")
            root.get_runner(emitter).add_event_listener('on_emit', process_done)
        else:
            root.add_runner(r.LambdaRunner('done', process_done))
        return self.success()

    def execute_done_writer(self, data: dict):
        return self.create_done_writer(data.get('emitter'))

    def execute_insert_one(self, data):
        if 'row' in data:
            row = data['row']
        elif u.contains_like(data.keys(), r"row\_%"):
            row = u.dict_extract_by_prefix(data, 'row_')
        else:
            row = data
            row['debug_info'] = 'execute_insert_one from data'
        return self.success(result=str(self.insert_one(data['db'], data['collection'], row)))

    def execute_find(self, data):
        q = data.get('q', {})
        if isinstance(q, str):
            q = u.load_to_dict(q)
        cursor = self.find(data['db'], data['collection'], q, limit=int(data.get('limit', 100)))
        items = [{**doc, '_id': str(doc['_id'])} if '_id' in doc else doc for doc in cursor]
        return self.success(items=self.bson_safe(items), count=len(items))

    def execute_create_ticker(self, data):
        if self.ticker is None:
            self.ticker = self.periodic(self.ticker_interval, self.tick)
        return self.success(ui_reload=True)

    def execute_stop_ticker(self, data=dict()):
        if self.ticker is not None:
            self.ticker.stop()
            self.ticker = None
        return self.success(ui_reload=True)

    def execute_jobs(self, data=dict()):
        count = 0
        for job in self.find('nweb', 'jobs'):
            if self.process_job(job):
                count += 1
        return self.success(count=count, ui_reload=True)

    def execute_collect_files(self, data: dict):
        db = data.get('db', 'nweb')
        collection = data.get('collection', 'files')
        count = 0
        for filepath in glob.glob(data['path']):
            if self.mv_file(filepath, db, collection, data):
                count += 1
        return self.success(count=count)

    def mv_file(self, filepath: str, db: str, collection: str, data: dict = {}) -> None:
        bin_data = u.file_get_contents(filepath)
        root = self.getRoot().jobexecutor
        crypted = 'crypt' in root
        if crypted:
            f_data = root.execute(dict(type='crypt', op='encrypt', value=bin_data))['value']
        else:
            f_data = base64.b64encode(bin_data)
        if 'force_crypt' in data and not crypted:
            return False
        mdata = dict(
            file=f_data,
            name=os.path.basename(filepath),
            crypted=crypted
        )
        self.insert_one(db, collection, mdata)
        os.remove(filepath)
        return True

    def process_job(self, job: dict):
        executor: r.BaseJobExecutor = self.getRoot().jobexecutor
        if executor.canExecute(job):
            result = executor.execute(job)
            self.delete_one('nweb', 'jobs', dict(_id=job['_id']))
            if not isinstance(result, dict):
                result = dict(result=result)
            result = self.bson_safe(result)
            result.pop('_id', None)
            result['job_id'] = job['_id']
            self.insert_one('nweb', 'results', result)
            return True
        return False

    def bson_safe(self, value):
        """
          Werte, die BSON nicht kodieren kann, werden als Text gespeichert.
          Cursor werden als Liste der Dokumente gespeichert (max. 1000).
        """
        import datetime
        import itertools
        from bson import ObjectId
        from pymongo.cursor import Cursor
        if isinstance(value, dict):
            return {str(k): self.bson_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self.bson_safe(v) for v in value]
        if isinstance(value, Cursor):
            return [self.bson_safe(v) for v in itertools.islice(value, 1000)]
        if value is None or isinstance(value, (str, bool, int, float, bytes, datetime.datetime, ObjectId)):
            return value
        return str(value)

    def part_db(self, p: b.Page, params={}):
        dbname = params['db']
        p.h2(f"DB: {dbname}")
        p.ul(map(lambda x: w.a(x, self.link(self.part_col, db=dbname, col=x)), self.list_collection_names(dbname)))

    def part_col(self, p: b.Page, params={}):
        dbname = params['db']
        colname = params['col']
        limit = params.get('limit', 50)
        p.h2(f"DB: {dbname} Collection: {colname}")
        rows = list(self.find(dbname, colname, {}, limit=int(limit)))
        p.pre(json.dumps(rows, indent=2, default=self.json_util.default))

    def grid_rows(self, rows) -> list:
        """ Mongo-Dokumente fuer p.grid() serialisierbar machen """
        result = []
        for row in rows:
            item = {}
            for k, v in row.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    item[k] = v
                elif isinstance(v, (dict, list)):
                    item[k] = json.dumps(v, default=self.json_util.default)
                else:
                    item[k] = str(v)
            result.append(item)
        return result

    def grid_cols(self, rows: list) -> list:
        keys = []
        for row in rows:
            for k in row.keys():
                if k not in keys:
                    keys.append(k)
        return [w.Grid.col(k, sortable=True) for k in keys]

    def show_collection_grid(self, p: b.Page, db: str, collection: str, limit=100):
        rows = self.grid_rows(self.find(db, collection, {}, limit=limit))
        if len(rows) == 0:
            p.div("No Data")
        else:
            cols = self.grid_cols(rows)
            # tmpl wird mit str.format_map gefuellt: JSON-Klammern verdoppeln, nur {_id} bleibt Platzhalter
            job = json.dumps(dict(type=self.type, op='delete_doc', db=db, collection=collection, id='_ID_'))
            job = job.replace('{', '{{').replace('}', '}}').replace('_ID_', '{_id}')
            cols.append(w.Grid.col('_delete', title='', width=80,
                                   tmpl=f"<button onclick='exec_job({job})'>Delete</button>"))
            p.grid(rows, cols)

    def show_open_jobs(self, p: b.Page):
        with p.section(h="Open Jobs"):
            self.show_collection_grid(p, 'nweb', 'jobs')

    def show_results(self, p: b.Page):
        with p.section(h="Results"):
            self.show_collection_grid(p, 'nweb', 'results')

    def part_npy(self, p: b.Page, params={}):
        with p.section(h="npy"):
            p.div("Query Npy Jobs")
            if self.ticker is None:
                p(self.action_btn(dict(title="Execute Jobs now", type=self.type, op='jobs')))
                p(self.action_btn(dict(title="Create Periodic Job Processor", type=self.type, op='create_ticker')))
            else:
                p.prop("Ticker", "active")
                p.prop("Interval (s)", self.ticker_interval)
                p.prop("Tick Count", self.tick_count)
                p(self.action_btn(dict(title="Stop", type=self.type, op='stop_ticker')))
        self.show_open_jobs(p)
        self.show_results(p)
        with p.section(h="Queue Jobs"):
            for seconds in [30, 300, 3852]:
                job = dict(type=self.type, op='set_ticker', value=seconds)
                p(self.action_btn(dict(title=f"Queue Set Ticker {seconds}s", type=self.type, op='insert_one',
                                       db='nweb', collection='jobs', row=job)))
            p.ul([w.a("Queue custom Job", self.link(self.part_npy_queue))])
        with p.section(h="Result"):
            p.pre('', id='result')

    def part_npy_queue(self, p: b.Page, params={}):
        """
            Schreib einen Job in MongoDB
        """
        with p.section(h="Queue custom Job"):
            p(self.ui_form_job('jobdef', False))
            p.div(self.action_btn_parametric("Enqueue Job into MongoDB", dict(type=self.type, op='enqueue', jobdef='#jobdef')),
                  _class='ui_form_job_actions')

    def execute_enqueue(self, data: dict):
        """ Schreibt den Job aus jobdef (JSON) nach nweb.jobs """
        job = data.get('jobdef')
        if isinstance(job, str):
            if job.strip() == '':
                return self.fail('jobdef is empty')
            job = json.loads(job)
        if not isinstance(job, dict) or 'type' not in job:
            return self.fail('jobdef must be a job object with type')
        return self.success(result=str(self.insert_one('nweb', 'jobs', job).inserted_id))

    def execute_delete_doc(self, data: dict):
        from bson import ObjectId
        res = self.delete_one(data['db'], data['collection'], dict(_id=ObjectId(data['id'])))
        return self.success(deleted=res.deleted_count, ui_reload=True)
