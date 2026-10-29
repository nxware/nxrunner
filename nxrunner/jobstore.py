"""
Job-Speicher: Jobs fuer eine andere Instanz einstellen und ihre Ergebnisse per guid abholen (z.B. RemoteTerminal).

Ein Speicher ist ein Runner mit JobStoreMixin, der store_enqueue und store_result umsetzt; die Ops, ihre Parameter
und die Form der Antworten legt das Mixin fest:

    op=cenqueue jobdef={...} [guid] [target]   -> success, result (ID im Speicher)
    op=cresult guid [delete=1]                 -> success, pending=True | pending=False, result={...}

Zusagen eines Speichers:
    - Jobs werden verschluesselt abgelegt, guid und target duerfen im Klartext stehen
    - nur die Instanz target (nwebclient.nx.get_name()) fuehrt einen Job aus, ohne target jede
    - Jobs werden in der Reihenfolge des Einstellens ausgefuehrt
    - das Ergebnis ist ueber die guid des Jobs auffindbar, Ergebnisse anderer Jobs bleiben unberuehrt
"""
import json
import threading
import time
from typing import Optional, List

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import dev as d


class JobStoreMixin:

    JOB_STORE_OPS = ('cenqueue', 'cresult')

    def define_job_store_sigs(self):
        """ im __init__ des Runners aufrufen """
        self.define_sig(d.PStr('op', 'cenqueue'), d.PStr('jobdef'), d.PStr('guid', ''), d.PStr('target', ''))
        self.define_sig(d.PStr('op', 'cresult'), d.PStr('guid'), d.PInt('delete', 1))

    @classmethod
    def is_job_store(cls, runner) -> bool:
        """ Runner bietet die Ops des Vertrags an (mit oder ohne Mixin) """
        return all(callable(getattr(runner, 'execute_' + op, None)) for op in cls.JOB_STORE_OPS)

    # --- Verschluesselung ueber den CryptRunner crypt -------------------------------------------

    def has_crypt(self) -> bool:
        return 'crypt' in self.getRoot().jobexecutor

    def encrypt(self, value: str) -> str:
        res = self.getRoot().jobexecutor.execute(dict(type='crypt', op='encrypt', value=value))
        if not res.get('success', False):
            raise Exception(res.get('message', 'encrypt failed'))
        return res['value']

    def decrypt(self, c: str) -> str:
        res = self.getRoot().jobexecutor.execute(dict(type='crypt', op='decrypt', c=c))
        if not res.get('success', False):
            raise Exception(res.get('message', 'decrypt failed'))
        return res['value']

    # --- vom Speicher umzusetzen ----------------------------------------------------------------

    def store_enqueue(self, job: dict, guid: str = None, target: str = None) -> str:
        """
          Job verschluesselt ablegen, Fehler als Exception
          :return: ID im Speicher
        """
        raise NotImplementedError(type(self).__name__ + '.store_enqueue')

    def store_result(self, guid: str, delete: bool = True):
        """
          Ergebnis des Jobs mit guid (entschluesselt), Fehler als Exception
          :param delete: Ergebnis danach entfernen
          :return: Ergebnis-dict oder None, solange es noch keins gibt
        """
        raise NotImplementedError(type(self).__name__ + '.store_result')

    # --- Ops ------------------------------------------------------------------------------------

    def parse_jobdef(self, data: dict):
        """ :return: (job, None) oder (None, fehlermeldung) """
        job = data.get('jobdef')
        if isinstance(job, str):
            if job.strip() == '':
                return None, 'jobdef is empty'
            try:
                job = json.loads(job)
            except ValueError as e:
                return None, f'jobdef is no valid JSON: {e}'
        if not isinstance(job, dict) or 'type' not in job:
            return None, 'jobdef must be a job object with type'
        return job, None

    def execute_cenqueue(self, data: dict):
        """ Job aus jobdef (JSON) verschluesselt einstellen """
        job, error = self.parse_jobdef(data)
        if error is not None:
            return self.fail(error)
        try:
            job_id = self.store_enqueue(job, str(data.get('guid') or '') or None, str(data.get('target') or '') or None)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        return self.success(result=str(job_id))

    def execute_cresult(self, data: dict):
        """ Ergebnis anhand der guid, delete=1 (Standard) entfernt nur dieses Ergebnis """
        guid = str(data.get('guid', '')).strip()
        if guid == '':
            return self.fail('guid fehlt')
        delete = str(data.get('delete', '1')) not in ('0', 'false', 'False')
        try:
            result = self.store_result(guid, delete)
        except Exception as e:
            return self.fail(f'Ergebnis {guid}: {type(e).__name__}: {e}')
        if result is None:
            return self.success('pending', pending=True)
        return self.success('ok', pending=False, result=result)


class DirectJobStore(r.BaseJobExecutor, JobStoreMixin):
    """
      Job-Speicher fuer Tests: fuehrt Jobs beim Einstellen sofort lokal aus und haelt das Ergebnis im Speicher,
      ohne Datenbank und ohne Verschluesselung. target wird ignoriert.

        Config (runners):
            store: nxrunner.jobstore:DirectJobStore
            store: nxrunner.jobstore:DirectJobStore(delay=3)
            rterm: nxrunner.system.remote_terminal:RemoteTerminal(target="local", store="store", poll_interval=1)
    """

    TAGS = ['test']

    def __init__(self, type='direct_job_store', delay=None):
        """
        :param delay: Sekunden, nach denen ein Ergebnis abholbar ist (simuliertes Delay), None: sofort
        """
        super().__init__(type)
        self.delay = float(delay) if delay is not None else None
        self.lock = threading.Lock()
        self.results = {}  # guid -> (ts ab dem es abholbar ist, Ergebnis)
        self.define_vars('delay')
        self.define_job_store_sigs()

    def set_delay(self, value):
        self.delay = float(value) if value not in (None, '', 'None') else None

    def store_enqueue(self, job: dict, guid: str = None, target: str = None) -> str:
        guid = guid or u.guid()
        try:
            result = self.getRoot().jobexecutor.execute(dict(job))
        except Exception as e:
            result = self.fail(f'{type(e).__name__}: {e}')
        if not isinstance(result, dict):
            result = dict(result=result)
        with self.lock:
            self.results[guid] = (time.time() + (self.delay or 0), result)
        return guid

    def store_result(self, guid: str, delete: bool = True):
        with self.lock:
            entry = self.results.get(guid)
            if entry is None or time.time() < entry[0]:
                return None
            if delete:
                del self.results[guid]
            return entry[1]

    def part_index(self, p: b.Page, params={}):
        p.div("Job-Speicher fuer Tests: fuehrt Jobs sofort lokal aus.")
        p.prop("Delay (s)", self.delay)
        with self.lock:
            p.prop("Ergebnisse", len(self.results))


class NWebJobRunner(r.BaseJobExecutor, JobStoreMixin):
    """
        Laedt Jobs aus einer nweb-Gruppe herunter und fuehrt diese aus (frueher nxbot.nweb:NWebJobRunner)

        nxrunner.jobstore:NWebJobRunner()

      @nweb.json job_nweb
      @nweb.json job_fetch_group_id
      @nweb.json job_result_group_id

      Als Job-Speicher (JobStoreMixin): cenqueue legt {type: crypt, c, guid, target} als json-Dokument in group ab,
      das Ergebnis landet verschluesselt als {c, guid} in group_upload (Dokument-guid = guid des Jobs).
      Ohne Verschluesselung: op=enqueue jobdef [guid] [target] legt den Job im Klartext ab, op=result guid [delete=1]
      holt das (unverschluesselte) Ergebnis.
      Jobs mit target einer anderen Instanz (nwebclient.nx.get_name()) bleiben liegen.
    """
    CFG_FETCH_ID = 'job_fetch_group_id'
    CFG_RESULT_ID = 'job_result_group_id'

    def __init__(self, type='nweb_jobs', interval=181, nwebclient=None, group=None,
                 supported_types: Optional[List] = None, delete_jobs=True, limit=1, args: u.Args = None):
        super().__init__(type)
        from nwebclient import NWebClient, nx
        if args is None:
            args = u.Args()
        self.define_vars('limit', 'interval', 'group', 'counter', 'last_tick')
        self.define_job_store_sigs()
        self.define_sig(d.PStr('op', 'enqueue'), d.PStr('jobdef'), d.PStr('guid', ''), d.PStr('target', ''))
        self.define_sig(d.PStr('op', 'result'), d.PStr('guid'), d.PInt('delete', 1))
        if nwebclient is None:
            self.debug("Reading from nweb.json")
            nwebclient = NWebClient(args.get('job_nweb', None))
        elif isinstance(nwebclient, str):
            nwebclient = NWebClient(nwebclient)
        self.nweb = nwebclient
        self.group_upload = None
        if group is None:
            self.debug("Reading group from args " + str(args))
            group = args.get(self.CFG_FETCH_ID, None)
            self.group_upload = args.get(self.CFG_RESULT_ID, None)
        self.group = group
        self.instance_name = nx.get_name()
        self.supported_types = supported_types
        self.reject_types = None
        self.default_can_process = True
        self.delete_jobs = delete_jobs
        self.limit = limit
        self.counter = 0
        self.last_tick = 0
        self.interval = interval
        if interval is not None:
            self.thread = self.periodic(interval, self.tick)
        else:
            self.thread = None

    def __str__(self):
        return f'NWebJobRunner(count:{self.counter})'

    def tick(self):
        self.last_tick = int(time.time())
        self.debug("Fetching Jobs..." + str(self.group))
        # aelteste zuerst, damit Jobs in der Reihenfolge des Einstellens laufen
        docs = sorted(self.nweb.docs('group_id=' + str(self.group)), key=lambda doc: int(doc.id()))
        counter = 0
        for doc in docs:
            if self.limit is not None and counter >= int(self.limit):
                self.info("Job Download Limit, Stopping Download...")
                break
            if self.download(doc):
                counter += 1
        return self.success(count=counter)

    def canProcess(self, data):
        target = data.get('target')
        if target and target != self.instance_name:
            return False
        if self.reject_types is not None and 'type' in data:
            if data['type'] in self.reject_types:
                return False
        if self.supported_types is not None and 'type' in data:
            if data['type'] in self.supported_types:
                return True
        return self.default_can_process

    def download(self, doc) -> bool:
        """ :return: Job wurde ausgefuehrt """
        self.info("Start Download")
        data = json.loads(doc.content())
        if not self.canProcess(data):
            self.info("Not Processing Job")
            return False
        try:
            job = {k: v for k, v in data.items() if k != 'target'}
            result = self.owner().execute(job)
            self.counter += 1
            if not isinstance(result, dict):
                result = dict(result=result)
            if self.group_upload is not None:
                guid = data.get('guid') or result.get('guid')
                if data.get('type') == 'crypt':
                    result = dict(c=self.encrypt(json.dumps(result, default=str)))
                if guid:
                    # im Klartext, damit der Auftraggeber sein Ergebnis ohne Entschluesseln findet
                    result['guid'] = guid
                    self.nweb.createDoc('job_result', json.dumps(result, default=str), self.group_upload,
                                        kind='json', guid=guid)
                else:
                    self.nweb.createDoc('job_result', json.dumps(result, default=str), self.group_upload, kind='json')
            if not ('keep' in data and data['keep'] is True) and self.delete_jobs:
                self.nweb.deleteDoc(doc.id())
            return True
        except Exception as e:
            self.error(str(e))
            return False

    def add_type(self, t):
        if self.supported_types is None:
            self.supported_types = []
        self.supported_types.append(t)
        return self.success()

    def execute(self, data):
        if 'execute' in data:
            return self.tick()
        if 'add_type' in data:
            return self.add_type(data['add_type'])
        return super().execute(data)

    # --- Job-Speicher (JobStoreMixin) -------------------------------------------------------------

    def store_enqueue(self, job: dict, guid: str = None, target: str = None) -> str:
        """ verschluesselt als json-Dokument in group, guid und target im Klartext """
        if not self.has_crypt():
            raise RuntimeError("CryptRunner crypt not available")
        return self.enqueue_doc(dict(type='crypt', c=self.encrypt(json.dumps(job))), guid, target)

    def enqueue_doc(self, job: dict, guid: str = None, target: str = None) -> str:
        """ job als json-Dokument in group ablegen, :return: document_id """
        if self.group is None:
            raise RuntimeError(self.CFG_FETCH_ID + ' ist nicht gesetzt')
        job = dict(job, ts=time.time())
        if guid:
            job['guid'] = guid
        if target:
            job['target'] = target
        doc = self.nweb.createDoc('job', json.dumps(job), self.group, kind='json')
        if doc is None:
            raise RuntimeError('Dokument konnte nicht angelegt werden')
        return str(doc.id())

    def execute_enqueue(self, data: dict):
        """ wie cenqueue, aber unverschluesselt (ohne CryptRunner) """
        job, error = self.parse_jobdef(data)
        if error is not None:
            return self.fail(error)
        try:
            job_id = self.enqueue_doc(job, str(data.get('guid') or '') or None, str(data.get('target') or '') or None)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        return self.success(result=str(job_id))

    def execute_result(self, data: dict):
        """ wie cresult; unverschluesselte Ergebnisse liefert auch cresult, verschluesselte auch result """
        return self.execute_cresult(data)

    def store_result(self, guid: str, delete: bool = True):
        """ aus group_upload anhand der guid, c wird entschluesselt """
        if self.group_upload is None:
            raise RuntimeError(self.CFG_RESULT_ID + ' ist nicht gesetzt')
        for doc in self.nweb.docs('group_id=' + str(self.group_upload)):
            if str(doc.guid()).lower() != guid.lower():
                continue
            data = json.loads(doc.content())
            if 'c' in data:
                result = json.loads(self.decrypt(data['c']))
            else:
                result = {k: v for k, v in data.items() if k != 'guid'}
            if delete:
                self.nweb.deleteDoc(doc.id())
            return result
        return None

    def part_index(self, p: b.Page, params={}):
        with p.section(h="NWeb Job Downloader"):
            p.prop("Jobs", self.counter)
            p.prop("Last Tick", self.last_tick)
            p.prop("Limit", self.limit)
            p.prop("Group", self.group)
            p.prop("Result Group", self.group_upload)
            p.prop("Instanz (target)", self.instance_name)
            p.prop("Interval", self.interval)
            p.prop("Delete Jobs", self.delete_jobs)
            p.prop("supported_types", self.supported_types)
            p.prop("reject_types", self.reject_types)
            p.prop("CryptRunner crypt", self.has_crypt())
            p.form_input('add_type', "Add Job Type", id='add_type')
            p(self.action_btn_parametric("Add Type", dict(type=self.type, add_type='#add_type')))
            p(self.action_btn(dict(title="Run Now", type=self.type, execute=1)))
