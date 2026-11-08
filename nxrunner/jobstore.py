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
from html import escape as esc
from typing import Optional, List

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import dev as d
from nwebclient import web as w


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


class RemoteJobMixin:
    """
      Gegenstueck zum Job-Speicher: Jobs fuer die Instanz target einstellen (op=cenqueue, verschluesselt) und in
      einem Thread alle poll_interval Sekunden die Ergebnisse per guid abholen (op=cresult). Fuer Runner, die eine
      andere Instanz fernsteuern (z.B. RemoteTerminal).

      Im __init__ des Runners init_remote(...) aufrufen. Unterklassen setzen um (aufgerufen unter remote_lock):
          on_remote_result(meta, result)   Ergebnis eines Jobs (result der Ziel-Instanz, success=False bei Fehler dort)
          remote_changed()                 Zustand geaendert: gesendet, Ergebnis, Fehler, Timeout
          remote_job_label(job)            Anzeige des Jobs in remote_status(), None = nicht anzeigen

      Zustand: remote_pending {guid: meta}, remote_error, remote_last_update; meta enthaelt seq (laufende Nummer),
      guid, ts, label und die an send() uebergebenen Felder.
    """

    def init_remote(self, target='', store='mongodb', remote_type='', poll_interval=10, timeout=6000):
        """
        :param target: Name der Ziel-Instanz (nwebclient.nx.get_name() dort, i.d.R. der Hostname)
        :param store: type des Job-Speicher-Runners (JobStoreMixin)
        :param remote_type: type des Runners auf der Ziel-Instanz, an den die Jobs gehen
        :param poll_interval: Sekunden zwischen den Abfragen der Ergebnisse
        :param timeout: Sekunden, nach denen eine Antwort verworfen wird
        """
        self.target = target
        self.store = store
        self.remote_type = remote_type
        self.poll_interval = float(poll_interval)
        self.timeout = float(timeout)
        self.remote_lock = threading.Lock()
        self.remote_seq = 0          # laufende Nummer gesendeter Jobs
        self.remote_pending = {}     # guid -> meta
        self.remote_error = ''
        self.remote_last_update = None
        self.remote_poller = None
        self.store_ok = False        # store als Job-Speicher geprueft

    # --- von Unterklassen umzusetzen ------------------------------------------------------------

    def on_remote_result(self, meta: dict, result: dict):
        pass

    def remote_changed(self):
        pass

    def remote_job_label(self, job: dict) -> Optional[str]:
        return str(job.get('op', job.get('type', '')))

    # --- Job-Speicher ---------------------------------------------------------------------------

    def store_execute(self, data: dict) -> dict:
        return self.getRoot().jobexecutor.execute({'type': self.store, **data})

    def check_store(self) -> str:
        """ :return: Fehlermeldung, '' wenn store ein Job-Speicher ist (JobStoreMixin bzw. dessen Ops) """
        executor = self.getRoot().jobexecutor
        if self.store not in executor:
            return f'Job-Speicher {self.store} ist nicht geladen'
        get_runner = getattr(executor, 'get_runner', None)
        runner = get_runner(self.store) if get_runner is not None else None
        if runner is not None and not JobStoreMixin.is_job_store(runner):
            return f'{self.store} ({type(runner).__name__}) ist kein Job-Speicher, op=cenqueue/cresult fehlen'
        return ''

    def remote_send(self, job: dict, **meta) -> str:
        """
          Job fuer remote_type auf target verschluesselt in den Speicher schreiben, Fehler als Exception.
          :param meta: zusaetzliche Felder, die on_remote_result mit dem Ergebnis erhaelt
          :return: guid
        """
        if not self.target:
            raise RuntimeError('target (Name der Ziel-Instanz) ist nicht gesetzt')
        if not self.store_ok:
            error = self.check_store()
            if error:
                raise RuntimeError(error)
            self.store_ok = True
        guid = u.guid()
        res = self.store_execute(dict(op='cenqueue', jobdef={'type': self.remote_type, **job}, guid=guid,
                                      target=self.target))
        if not isinstance(res, dict) or not res.get('success', False):
            message = res.get('message', res) if isinstance(res, dict) else res
            raise RuntimeError(f'Job-Speicher {self.store}: {message}')
        with self.remote_lock:
            self.remote_seq += 1
            self.remote_pending[guid] = dict(meta, guid=guid, seq=self.remote_seq, ts=time.time(), label=self.remote_job_label(job))
            self.remote_error = ''
            self.remote_changed()
            self.start_remote_poller()
        return guid

    def start_remote_poller(self):
        """ unter remote_lock: Abfrage-Thread starten, falls keiner laeuft (er endet, wenn nichts mehr aussteht) """
        if self.remote_poller is None:
            self.remote_poller = threading.Thread(target=self.remote_poll_loop, daemon=True,
                                                  name='remote-' + str(getattr(self, 'type', '')))
            self.remote_poller.start()

    def remote_poll_loop(self):
        while True:
            time.sleep(self.poll_interval)
            with self.remote_lock:
                guids = list(self.remote_pending.keys())
                if not guids:
                    self.remote_poller = None
                    return
            for guid in guids:
                try:
                    self.remote_poll(guid)
                except Exception as e:
                    self.set_remote_error(f'Abfrage {guid}: {type(e).__name__}: {e}')

    def remote_poll(self, guid):
        res = self.store_execute(dict(op='cresult', guid=guid, delete=1))
        with self.remote_lock:
            meta = self.remote_pending.get(guid)
            if meta is None:
                return
            if not isinstance(res, dict) or not res.get('success', False):
                self.remote_error = str(res.get('message', res) if isinstance(res, dict) else res)
            elif res.get('pending', False):
                if time.time() - meta['ts'] <= self.timeout:
                    return
                del self.remote_pending[guid]
                self.remote_error = f'Keine Antwort von {self.target} nach {int(self.timeout)} s'
            else:
                del self.remote_pending[guid]
                self.remote_last_update = time.time()
                result = res.get('result') or {}
                if not result.get('success', False):
                    self.remote_error = str(result.get('message', 'Fehler auf ' + self.target))
                self.on_remote_result(meta, result)
            self.remote_changed()

    def set_remote_error(self, message):
        with self.remote_lock:
            self.remote_error = message
            self.remote_changed()

    def remote_status(self, max_len=80) -> str:
        """ unter remote_lock: Statuszeile aus target, ausstehenden Jobs, letztem Stand und Fehler """
        status = self.target or '(kein target)'
        if self.remote_pending:
            status += f' – wartet auf {len(self.remote_pending)} Antwort(en)'
            pending = sorted(self.remote_pending.values(), key=lambda m: m['seq'])
            text = ', '.join(m['label'] for m in pending if m['label'])
            if text:
                status += ': ' + (text if len(text) <= max_len else text[:max_len - 3] + '...')
        elif self.remote_last_update is not None:
            status += ' – Stand ' + time.strftime('%H:%M:%S', time.localtime(self.remote_last_update))
        if self.remote_error:
            status += ' – ' + self.remote_error
        return status


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


class RemoteRunner(r.BaseJobExecutor, RemoteJobMixin):
    """
      Runner einer anderen Instanz ueber einen Job-Speicher bedienen: Die Seite zeigt je Runner der Ziel-Instanz ein
      aufklappbares Feld mit dessen Signaturen; "Ausfuehren" oeffnet ein Formular fuer die Parameter und stellt den
      Job (verschluesselt, cenqueue) in den Job-Speicher. Die Ergebnisse zeigt die Seite "Ergebnisse".

      Die Runner der Ziel-Instanz kommen von {type: list_runners} (per Spec registrierte Runner) und runners,
      ihre Signaturen von {type: <runner>, op: get_signatures}.

        Config (runners):
            remote: nxrunner.jobstore:RemoteRunner(target="pi4")
            remote: nxrunner.jobstore:RemoteRunner(target="pi4", store="nweb_jobs", runners="terminal,backup")
            remote: nxrunner.jobstore:RemoteRunner(target="local", store="store", poll_interval=1)   # mit DirectJobStore

        Jobs:
            op=refresh                      Runner und Signaturen der Ziel-Instanz abrufen
            op=runners                      bekannte Runner mit Signaturen
            op=run jobdef={...}             Job an die Ziel-Instanz senden, liefert guid
            op=results                      gesendete Jobs mit Status und Ergebnis
            op=result guid                  ein Job mit Ergebnis
            op=clear_results [guid]         ein bzw. alle abgeschlossenen Ergebnisse entfernen
    """

    TAGS = ['remote']

    def __init__(self, type='remote_runner', target='', store='mongodb', runners='', poll_interval=10,
                 timeout=6000, max_results=200):
        """
        :param target: Name der Ziel-Instanz (nwebclient.nx.get_name() dort, i.d.R. der Hostname)
        :param store: type des Job-Speicher-Runners (JobStoreMixin)
        :param runners: zusaetzliche Runner der Ziel-Instanz (Komma-getrennt), die list_runners nicht liefert
        :param poll_interval: Sekunden zwischen den Abfragen der Ergebnisse
        :param timeout: Sekunden, nach denen eine Antwort verworfen wird
        :param max_results: Anzahl gespeicherter Jobs, aeltere abgeschlossene werden verworfen
        """
        super().__init__(type)
        self.init_remote(target, store, '', poll_interval, timeout)
        self.extra_runners = [t.strip() for t in str(runners or '').split(',') if t.strip()]
        self.max_results = int(max_results)
        self.remote_runners = {}  # type -> {'signatures': [...] | None, 'error': str}
        self.jobs = {}            # guid -> Eintrag (siehe add_job), in Reihenfolge des Sendens
        self.define_vars('target', 'store', 'max_results')
        self.define_sig(d.PStr('op', 'refresh')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'runners')).r(success='bool', runners='dict')
        self.define_sig(d.PStr('op', 'run'), d.PStr('jobdef')).r(success='bool', message='str', guid='str')
        self.define_sig(d.PStr('op', 'results')).r(success='bool', results='list')
        self.define_sig(d.PStr('op', 'result'), d.PStr('guid')).r(success='bool', result='dict')
        self.define_sig(d.PStr('op', 'clear_results'), d.PStr('guid', '')).r(success='bool', message='str')

    # --- RemoteJobMixin -------------------------------------------------------------------------

    def remote_job_label(self, job: dict):
        if job.get('type') == 'list_runners':
            return 'Runner abrufen'
        if job.get('op') == 'get_signatures':
            return 'Signaturen ' + str(job.get('type', ''))
        return self.job_label(job)

    @staticmethod
    def job_label(job: dict) -> str:
        label = str(job.get('type', ''))
        if job.get('op'):
            label += ' ' + str(job['op'])
        return label

    def on_remote_result(self, meta: dict, result: dict):
        """ unter remote_lock """
        kind = meta.get('kind')
        if kind == 'list':
            self.on_runner_list(result)
        elif kind == 'sigs':
            self.on_signatures(meta['runner'], result)
        elif kind == 'job':
            self.on_job_result(meta['guid'], result)

    def remote_changed(self):
        """ unter remote_lock: Jobs, deren Antwort verworfen wurde (Timeout), als timeout markieren """
        for guid, job in self.jobs.items():
            if job['status'] == 'pending' and guid not in self.remote_pending:
                job['status'] = 'timeout'
                job['done'] = time.time()

    # --- Runner und Signaturen der Ziel-Instanz -------------------------------------------------

    def on_runner_list(self, result: dict):
        """ unter remote_lock: Ergebnis von list_runners (ohne success), danach Signaturen anfordern """
        names = result.get('names')
        if not isinstance(names, list):
            self.remote_error = 'list_runners: ' + str(result.get('message', 'keine Runner'))
            names = []
        else:
            self.remote_error = ''
        names = sorted(set(map(str, names)) | set(self.extra_runners))
        for name in names:
            self.remote_runners.setdefault(name, {'signatures': None, 'error': ''})
        # remote_send nimmt remote_lock, daher im eigenen Thread
        threading.Thread(target=self.request_signatures, args=(names,), daemon=True).start()

    def request_signatures(self, names):
        for name in names:
            try:
                self.remote_send({'type': name, 'op': 'get_signatures'}, kind='sigs', runner=name)
            except Exception as e:
                self.set_remote_error(str(e))
                return

    def on_signatures(self, name: str, result: dict):
        entry = self.remote_runners.setdefault(name, {'signatures': None, 'error': ''})
        if result.get('success', False) and isinstance(result.get('signatures'), list):
            entry['signatures'] = result['signatures']
            entry['error'] = ''
        else:
            entry['error'] = str(result.get('message', 'keine Signaturen'))

    def execute_refresh(self, data):
        """ Runner der Ziel-Instanz (list_runners) und danach deren Signaturen abrufen """
        try:
            self.remote_send({'type': 'list_runners'}, kind='list')
        except Exception as e:
            self.set_remote_error(str(e))
            return self.fail(str(e))
        if self.extra_runners:
            threading.Thread(target=self.request_signatures, args=(list(self.extra_runners),), daemon=True).start()
        return self.success('Abfrage gesendet, wartet auf Antwort')

    def execute_runners(self, data):
        with self.remote_lock:
            return self.success(runners={k: dict(v) for k, v in self.remote_runners.items()})

    # --- Jobs -----------------------------------------------------------------------------------

    def execute_run(self, data):
        """ Job aus jobdef (JSON mit type) an die Ziel-Instanz senden """
        job, error = JobStoreMixin.parse_jobdef(self, data)
        if error is not None:
            return self.fail(error)
        with self.remote_lock:
            self.trim_jobs()
        try:
            guid = self.remote_send(job, kind='job')
        except Exception as e:
            self.set_remote_error(str(e))
            return self.fail(str(e))
        with self.remote_lock:
            self.jobs[guid] = dict(guid=guid, job=job, label=self.job_label(job), status='pending',
                                   sent=time.time(), done=None, result=None)
        return self.success(f'{self.job_label(job)} gesendet', guid=guid)

    def trim_jobs(self):
        """ unter remote_lock: aelteste abgeschlossene Jobs entfernen, bis Platz fuer einen neuen ist """
        for guid in list(self.jobs.keys()):
            if len(self.jobs) < self.max_results:
                return
            if self.jobs[guid]['status'] != 'pending':
                del self.jobs[guid]

    def on_job_result(self, guid: str, result: dict):
        job = self.jobs.get(guid)
        if job is None:
            return
        job['status'] = 'ok' if result.get('success', True) else 'error'
        job['done'] = time.time()
        job['result'] = result

    def execute_results(self, data):
        with self.remote_lock:
            return self.success(results=[dict(j) for j in self.jobs.values()])

    def execute_result(self, data):
        with self.remote_lock:
            job = self.jobs.get(str(data.get('guid', '')))
            if job is None:
                return self.fail('Unbekannte guid')
            return self.success(result=dict(job))

    def execute_clear_results(self, data):
        guid = str(data.get('guid') or '')
        with self.remote_lock:
            if guid:
                removed = 1 if self.jobs.pop(guid, None) is not None else 0
            else:
                done = [g for g, j in self.jobs.items() if j['status'] != 'pending']
                for g in done:
                    del self.jobs[g]
                removed = len(done)
        return self.success(f'{removed} Ergebnis(se) entfernt', ui_reload=True)

    # --- Seite: Runner --------------------------------------------------------------------------

    RUN_JS = """
    function nxrr_toggle(id) {
      var e = document.getElementById(id);
      e.style.display = e.style.display === 'none' ? 'block' : 'none';
    }
    function nxrr_submit(id, rtype) {
      var f = document.getElementById(id);
      var job = JSON.parse(f.dataset.job);
      f.querySelectorAll('[data-param]').forEach(function(i) {
        var v = i.value;
        if (v === '') { return; }
        if (i.type === 'number') { v = Number(v); }
        job[i.dataset.param] = v;
      });
      var msg = f.querySelector('.nxrr_msg');
      msg.textContent = 'sende...';
      exec_job({type: rtype, op: 'run', jobdef: JSON.stringify(job)}, function(r) {
        msg.textContent = (r && r.message) ? r.message : JSON.stringify(r);
      });
    }
    """

    def nav(self, p: b.Page):
        with self.remote_lock:
            pending = sum(1 for j in self.jobs.values() if j['status'] == 'pending')
            count = len(self.jobs)
        p.div(' | '.join([w.a("Runner", self.link(self.part_index), hint="Runner und Signaturen der Ziel-Instanz"),
                          w.a(f"Ergebnisse ({count}, {pending} offen)", self.link(self.part_results),
                              hint="Gesendete Jobs mit Status und Ergebnis")]))

    def part_header(self, p: b.Page):
        error = self.check_store() if self.target else 'target (Name der Ziel-Instanz) ist nicht gesetzt'
        if error:
            p(w.alert_error(esc(error)))
        with self.remote_lock:
            p.prop("Status", esc(self.remote_status()))
        p.prop("Job-Speicher", esc(self.store))

    def part_index(self, p: b.Page, params={}):
        self.nav(p)
        self.part_header(p)
        p.div(self.action_btn(dict(type=self.type, op='refresh', title="Runner abrufen"),
                              hint="Fragt die Runner der Ziel-Instanz und ihre Signaturen ueber den Job-Speicher ab"))
        p.script(self.RUN_JS)
        with self.remote_lock:
            runners = {k: dict(v) for k, v in self.remote_runners.items()}
        if not runners:
            p(w.alert_info("Noch keine Runner bekannt, mit \"Runner abrufen\" abfragen."))
        for i, (name, entry) in enumerate(sorted(runners.items())):
            self.part_remote_runner(p, f'nxrr_{i}', name, entry)

    def part_remote_runner(self, p: b.Page, prefix: str, name: str, entry: dict):
        sigs = entry.get('signatures')
        count = '?' if sigs is None else len(sigs)
        with p.expandable(f'{esc(name)} ({count})', expanded=False):
            if entry.get('error'):
                p(w.alert_error(esc(entry['error'])))
            if sigs is None:
                p.div("Signaturen noch nicht abgerufen.")
                return
            if not sigs:
                p.div("Keine Signaturen.")
            for j, sig in enumerate(sigs):
                self.part_signature(p, f'{prefix}_{j}', name, sig)

    def part_signature(self, p: b.Page, form_id: str, runner: str, sig: dict):
        label = sig.get('label') or sig.get('name') or '?'
        p('<div class="nxrr_sig" style="margin: 4px 0;">')
        p(w.button_js("Ausführen", f'nxrr_toggle({json.dumps(form_id)});',
                      hint=f"Formular fuer die Parameter von {label} anzeigen"))
        p(f' <b>{esc(label)}</b>')
        if sig.get('description'):
            p(f' <small>{esc(sig["description"])}</small>')
        self.part_signature_form(p, form_id, runner, sig)
        p('</div>')

    def part_signature_form(self, p: b.Page, form_id: str, runner: str, sig: dict):
        job = dict(sig.get('job') or {}, type=runner)
        p(f'<div id="{form_id}" data-job="{esc(json.dumps(job))}" '
          f'style="display: none; margin: 4px 0 8px 20px;">')
        for param in sig.get('params', []):
            if param.get('name') == 'op' and 'op' in job:
                continue  # fester Name der Signatur
            self.part_param_input(p, param)
        p(w.button_js("Senden", f'nxrr_submit({json.dumps(form_id)}, {json.dumps(self.type)});',
                      hint=f"Job an {self.target} ueber den Job-Speicher {self.store} senden"))
        p(' <span class="nxrr_msg"></span> ')
        p(w.a("Ergebnisse", self.link(self.part_results), hint="Ergebnisse der gesendeten Jobs"))
        p('</div>')

    @staticmethod
    def part_param_input(p: b.Page, param: dict):
        name = str(param.get('name', ''))
        ptype = str(param.get('type', 'str'))
        default = param.get('default')
        attrs = {'data-param': name, 'placeholder': ptype}
        if default is not None:
            attrs['value'] = esc(default if isinstance(default, str) else json.dumps(default))
        if ptype in ('int', 'integer', 'float', 'number', 'double', 'real'):
            attrs['type'] = 'number'
            attrs['step'] = 'any'
        hint = f"{name}: {ptype}" + ('' if default is not None else ' (Pflicht, leer = nicht senden)')
        p.form_input(name, esc(name), hint=hint, **attrs)

    # --- Seite: Ergebnisse ----------------------------------------------------------------------

    STATUS_BADGES = {'pending': ('wartet', 'tag'), 'ok': ('ok', 'ok'), 'error': ('Fehler', 'warn'),
                     'timeout': ('Timeout', 'warn')}

    def part_results(self, p: b.Page, params={}):
        self.nav(p)
        self.part_header(p)
        with self.remote_lock:
            rows = [dict(j) for j in reversed(list(self.jobs.values()))]
        with p.section(h="Ergebnisse"):
            p.grid(rows, self.result_cols(), params, search=True, empty="Noch keine Jobs gesendet.")
            p.div(self.action_btn(dict(type=self.type, op='clear_results', title="Abgeschlossene entfernen"),
                                  hint="Entfernt alle Ergebnisse, auf die nicht mehr gewartet wird"))
        if any(row['status'] == 'pending' for row in rows):
            p.js_ready(f'setTimeout(function() {{ window.location.reload(); }}, {int(max(self.poll_interval, 2) * 1000)});')

    def result_cols(self):
        col = w.Grid.col
        return [
            col('sent', "Gesendet", value=lambda e: fmt_ts(e['sent']), sort=lambda e: e['sent']),
            col('label', "Job", html=True, value=self.col_label, sort=lambda e: e['label']),
            col('status', "Status", html=True, value=self.col_status),
            col('duration', "Dauer", value=lambda e: f"{e['done'] - e['sent']:.1f} s" if e['done'] else ''),
            col('message', "Meldung", value=lambda e: str((e['result'] or {}).get('message', ''))[:120]),
            col('actions', '', html=True, sortable=False, value=self.col_actions),
        ]

    def col_label(self, e):
        return w.a(esc(e['label']), self.link(self.part_result, guid=e['guid']),
                   hint=json.dumps(e['job'], default=str))

    def col_status(self, e):
        text, css = self.STATUS_BADGES.get(e['status'], (e['status'], ''))
        return f'<span class="badge {css}">{esc(text)}</span>'

    def col_actions(self, e):
        rerun = dict(type=self.type, op='run', jobdef=json.dumps(e['job']), title="Erneut")
        clear = dict(type=self.type, op='clear_results', guid=e['guid'], title="Entfernen")
        return (self.action_btn(rerun, hint="Denselben Job noch einmal senden") +
                self.action_btn(clear, hint="Ergebnis aus der Liste entfernen"))

    def part_result(self, p: b.Page, params={}):
        self.nav(p)
        with self.remote_lock:
            job = self.jobs.get(str(params.get('guid', '')))
            job = dict(job) if job is not None else None
        if job is None:
            p(w.alert_error("Unbekannte guid"))
            return
        with p.section(h=f"Job {esc(job['label'])}"):
            p.prop("guid", esc(job['guid']))
            p.prop("Status", self.col_status(job))
            p.prop("Gesendet", fmt_ts(job['sent']))
            p.prop("Fertig", fmt_ts(job['done']))
            p.div(self.col_actions(job))
            p.h3("Job")
            p.pre(esc(json.dumps(job['job'], indent=2, default=str)))
            p.h3("Ergebnis")
            p.pre(esc(json.dumps(job['result'], indent=2, default=str)) if job['result'] is not None else 'wartet...')
        if job['status'] == 'pending':
            p.js_ready(f'setTimeout(function() {{ window.location.reload(); }}, {int(max(self.poll_interval, 2) * 1000)});')


def fmt_ts(ts) -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)) if ts else ''
