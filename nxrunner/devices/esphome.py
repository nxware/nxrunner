"""
ESPHome-Geraete (verschoben aus nxbot.esp)

    Config (runners):
        esp_home: nxrunner.devices.esphome:EspHome()
        esp_home: nxrunner.devices.esphome:EspHome(path="/config/esphome")   Verzeichnis mit ESPHome-ymls
        esp_home: nxrunner.devices.esphome:EspHome(network="192.168.178.0/24")   Netz fuer den Port-Scan
        mydevice: nxrunner.devices.esphome:EspHomeDevice("mydevice", yaml_file="mydevice.yaml")
"""
from __future__ import annotations
import time
import html
import json
import inspect
import re
import asyncio
import socket
import ipaddress
import concurrent.futures
import shutil
import subprocess
from typing import Optional
from threading import Thread
from collections import deque
from pathlib import Path
from urllib.parse import urlparse

import requests

from nwebclient import runner as r
from nwebclient import base
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


def load_esphome_yaml(yml_file_path: str | Path) -> dict:
    """
    Lädt eine ESPHome-Konfiguration und verarbeitet rekursiv:
    - packages:
    - !include

    !secret wird aus der secrets.yaml neben der Datei aufgeloest,
    unbekannte Tags (!lambda, ...) werden als Rohwert übernommen.
    Gibt die vollständig expandierte Konfiguration zurück.
    """
    import yaml
    class ESPHomeLoader(yaml.SafeLoader):
        pass
    def _include(loader: ESPHomeLoader, node):
        base_path = Path(loader.name).parent
        include_file = base_path / loader.construct_scalar(node)
        return _load_yaml(include_file)
    ESPHomeLoader.add_constructor("!include", _include)
    def _deep_merge(base, override):
        if isinstance(base, dict) and isinstance(override, dict):
            result = dict(base)
            for key, value in override.items():
                if key in result:
                    result[key] = _deep_merge(result[key], value)
                else:
                    result[key] = value
            return result
        return override
    def _load_yaml(path: str | Path):
        path = Path(path)

        with open(path, encoding="utf-8") as f:
            loader = ESPHomeLoader(f)
            loader.name = str(path)
            data = loader.get_single_data()

        if not isinstance(data, dict):
            return data

        # packages auflösen
        packages = data.pop("packages", {})

        merged = {}
        if isinstance(packages, dict):
            for _, package_ref in packages.items():
                if isinstance(package_ref, str):
                    package_data = _load_yaml(path.parent / package_ref)
                else:
                    package_data = package_ref
                merged = _deep_merge(merged, package_data)

        return _deep_merge(merged, data)

    def _unknown_tag(loader, tag_suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)

        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)

        if isinstance(node, yaml.MappingNode):
            return loader.construct_mapping(node)

        return None

    ESPHomeLoader.add_multi_constructor("!", _unknown_tag)

    secrets = {}
    secrets_file = Path(yml_file_path).parent / 'secrets.yaml'
    if secrets_file.exists():
        with open(secrets_file, encoding="utf-8") as f:
            secrets = yaml.safe_load(f) or {}

    def _secret(loader, node):
        key = loader.construct_scalar(node)
        return secrets.get(key, key)
    ESPHomeLoader.add_constructor("!secret", _secret)

    result = _load_yaml(yml_file_path)

    if not isinstance(result, dict):
        raise ValueError("ESPHome-Konfiguration muss ein Dictionary ergeben")

    return result


def esphome_device_name(yml: dict, default: str | None = None) -> str | None:
    """ esphome.name einer Konfiguration, ${var} bzw. $var aus substitutions werden ersetzt """
    name = (yml.get('esphome') or {}).get('name')
    if not isinstance(name, str):
        return default
    subs = yml.get('substitutions') or {}
    return re.sub(r'\$\{?(\w+)\}?', lambda m: str(subs.get(m.group(1), m.group(0))), name)


class EspHome(r.BaseJobExecutor):
    """
    Sucht ESPHome-Geraete per mDNS (wie avahi-browse -rt _esphomelib._tcp) und per Port-Scan (6053).

    Config (runners):
        esp_home: nxrunner.devices.esphome:EspHome()
        esp_home: nxrunner.devices.esphome:EspHome(path="/config/esphome")       Verzeichnis mit ESPHome-ymls
        esp_home: nxrunner.devices.esphome:EspHome(network="192.168.178.0/24")   Netz(e) fuer den Scan, mit Komma getrennt

    In WSL2 (NAT) und Docker (Bridge) kommt mDNS nicht an und das eigene Netz ist nicht das LAN.
    Unter WSL scannt der Port-Scan deshalb auch die Netze von Windows (ipconfig.exe), sonst network angeben.
    Alternativ WSL mit networkingMode=mirrored bzw. Docker mit network_mode: host betreiben.

    Jobs:
        {"type": "esphome", "op": "scan"}                                                 Port-Scan
        {"type": "esphome", "op": "add_device", "name": "kueche", "host": "192.168.178.20"}
    """

    MODULES = ['zeroconf']

    SERVICE_TYPE = "_esphomelib._tcp.local."

    def __init__(self, path=None, network=None):
        super().__init__('esphome')
        self.devices = []
        self.scan_results = []
        self.ymls = {}
        self.network = network
        if path is not None:
            self.load_esphome_ymls(path)

    def load_esphome_ymls(self, path):
        """
        Liest ein Verzeichnis mit ESPHome-Konfigurationen ein (wie das config-Verzeichnis des ESPHome-Dashboards).
        Die Dateien werden ueber esphome.name dem Geraet zugeordnet und beim Anlegen an EspHomeDevice uebergeben.
        """
        for file in sorted(Path(path).glob('*.y*ml')):
            if file.stem == 'secrets':
                continue
            try:
                yml = load_esphome_yaml(file)
            except Exception as e:
                self.error(f"{file}: {e}")
                continue
            if 'esphome' in yml:
                self.ymls[esphome_device_name(yml, file.stem)] = str(file)

    # --- mDNS -------------------------------------------------------------------------------

    def remove_service(self, zeroconf, type, name):
        pass

    def update_service(self, zeroconf, type, name):
        pass

    def add_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        if info and info.addresses:
            self.devices.append({
                'name': name,
                'device': name.removesuffix('.' + self.SERVICE_TYPE),
                'address': socket.inet_ntoa(info.addresses[0]),
                'port': info.port,
                'source': 'mDNS'
            })

    def run_api(self):
        from zeroconf import Zeroconf, ServiceBrowser
        self.devices = []
        zeroconf = Zeroconf()
        browser = ServiceBrowser(zeroconf, self.SERVICE_TYPE, self)
        time.sleep(2)
        zeroconf.close()

    # --- Port-Scan --------------------------------------------------------------------------

    @staticmethod
    def is_wsl() -> bool:
        try:
            return 'microsoft' in Path('/proc/version').read_text().lower()
        except OSError:
            return False

    @staticmethod
    def local_ip() -> str:
        """ Lokale Adresse ueber die Default-Route (gethostbyname liefert oft 127.0.1.1) """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(('10.255.255.255', 1))
            return s.getsockname()[0]

    @staticmethod
    def windows_ips() -> list[str]:
        """ IPv4-Adressen von Windows, unter WSL ueber ipconfig.exe """
        exe = shutil.which('ipconfig.exe') or '/mnt/c/Windows/System32/ipconfig.exe'
        try:
            out = subprocess.run([exe], capture_output=True, timeout=10).stdout.decode('utf-8', errors='replace')
        except (OSError, subprocess.SubprocessError):
            return []
        return re.findall(r'IPv4[^:\n]*:\s*(\d+\.\d+\.\d+\.\d+)', out)

    def scan_networks(self) -> list[ipaddress.IPv4Network]:
        """ network aus der Config, sonst das /24-Netz der lokalen Adresse und unter WSL die von Windows """
        if self.network:
            networks = self.network if isinstance(self.network, (list, tuple)) else str(self.network).split(',')
            return [ipaddress.IPv4Network(n.strip(), strict=False) for n in networks]
        ips = [self.local_ip()]
        if self.is_wsl():
            ips += self.windows_ips()
        result = []
        for ip in ips:
            if ip.startswith('127.') or ip.startswith('169.254.'):
                continue
            network = ipaddress.IPv4Network(f"{ip}/24", strict=False)
            if network not in result:
                result.append(network)
        return result

    def _check_esphome(self, ip: str, port: int = 6053, timeout: float = 0.5) -> str | None:
        """Check if an IP is running ESPHome (native API on port 6053)."""
        try:
            with socket.create_connection((ip, port), timeout=timeout):
                return ip
        except (ConnectionRefusedError, TimeoutError, OSError):
            return None

    def find_esphome_devices(self) -> list[str]:
        """
        Scan the local network(s) for ESPHome devices, see scan_networks.
        Returns a list of IP addresses with ESPHome's native API port open (6053).

        Example: ['192.168.4.4', '192.168.4.5']
        """
        hosts = [str(h) for network in self.scan_networks() for h in network.hosts()]

        found = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            futures = {executor.submit(self._check_esphome, ip): ip for ip in hosts}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    found.append(result)

        return sorted(found, key=ipaddress.IPv4Address)

    @staticmethod
    def device_name(ip: str) -> str:
        """ Name ueber Reverse-DNS (z.B. esp-anemometer.fritz.box -> esp-anemometer), sonst die IP """
        try:
            return socket.gethostbyaddr(ip)[0].split('.')[0]
        except OSError:
            return ip

    # --- Jobs und UI ------------------------------------------------------------------------

    @staticmethod
    def to_bool(value):
        return value is True or str(value).lower() in ('1', 'true', 'on', 'yes', 'ja')

    def execute_scan(self, data={}):
        """ Sucht per Port-Scan nach ESPHome-Geraeten, fuer Netze ohne mDNS """
        self.scan_results = []
        for ip in self.find_esphome_devices():
            name = self.device_name(ip)
            self.scan_results.append({'name': name, 'device': name, 'address': ip, 'port': 6053, 'source': 'Scan'})
        return self.success(devices=self.scan_results, ui_reload=True)

    def execute_add_device(self, data={}):
        """ name oder host angeben, ohne name wird er per Reverse-DNS ermittelt """
        host = (data.get('host') or '').strip() or None
        if host is not None:
            host = EspHomeDevice.default_host(host)
            if self._check_esphome(host) is None:
                return self.fail(f"{host}: ESPHome-API (Port 6053) nicht erreichbar")
        name = (data.get('name') or '').strip() or (self.device_name(host) if host else None)
        if not name:
            return self.fail("name oder host angeben")
        runner = EspHomeDevice(name, yaml_file=self.ymls.get(name), host=host,
                               live=self.to_bool(data.get('live', False)))
        with self.opt_dispatcher() as d:
            d.add_runner(runner)
        return self.success(ui_reload=True)

    def device_btn(self, p: base.Page, name, host=None):
        """ Link zum EspHomeDevice-Runner oder Button zum Anlegen """
        with self.opt_dispatcher() as d:
            if name in d:
                p.div(w.a("Gehe zu", f'?type={name}'))
            else:
                p.div(self.action_btn(dict(type=self.type, op='add_device', name=name, host=host,
                                           title="EspHomeDevice anlegen",
                                           hint="Legt einen Runner an, über den das Gerät gesteuert wird")))

    def part_index(self, p: base.Page, params={}):
        self.run_api()
        known = {dev['address'] for dev in self.devices}
        devices = self.devices + [dev for dev in self.scan_results if dev['address'] not in known]
        with p.section(h="Gefundene Geräte"):
            for dev in devices:
                p.div(f"Name: {dev['device']} ({dev['source']})")
                p.div(f"Host: {dev['address']}:{dev['port']}")
                if dev['device'] in self.ymls:
                    p.div(f"YML: {self.ymls[dev['device']]}")
                # Mit IP anlegen, .local-Namen loesen nicht ueberall auf (WSL, Docker)
                self.device_btn(p, dev['device'], dev['address'])
                p.div("-" * 30)
            if len(devices) == 0:
                p.div("Keine Geräte gefunden.")
        with p.section(h="Suche"):
            if len(self.devices) == 0 and self.is_wsl():
                p.div("WSL: mDNS erreicht die WSL nicht, Geräte über den Netzwerk-Scan suchen.")
            networks = ', '.join(map(str, self.scan_networks()))
            p(self.action_btn(dict(type=self.type, op='scan', title=f"Netzwerk-Scan (Port 6053, {networks})",
                                   hint="Sucht Geräte über den offenen API-Port, für Netze ohne mDNS (dauert einige Sekunden)")))
            with p.expandable("Gerät per IP anlegen", expanded=False):
                p.div('IP/Host: <input id="esph_add_host" type="text" placeholder="192.168.178.118">')
                p.div('Name: <input id="esph_add_name" type="text" placeholder="leer = per Reverse-DNS">')
                p.div('<label><input id="esph_add_live" type="checkbox"> Live-Verbindung</label>')
                p.div(self.action_btn_parametric(
                    "Anlegen", dict(type=self.type, op='add_device', host='#esph_add_host', name='#esph_add_name',
                                    live='#esph_add_live'),
                    hint="Prüft Port 6053 und legt den Runner für das Gerät an"))
        found = {dev['device'] for dev in devices}
        offline = [name for name in self.ymls if name not in found]
        if len(offline) > 0:
            with p.section(h="Konfigurationen ohne gefundenes Gerät"):
                for name in offline:
                    p.div(f"{name}: {self.ymls[name]}")
                    self.device_btn(p, name)


class EspHomeDevice(r.BaseJobExecutor):
    """
    Ein ESPHome-Geraet, angesprochen ueber die native API (Port 6053, aioesphomeapi)
    und die Web-API des web_server (Port 80).

    Config (runners):
        kueche: nxrunner.devices.esphome:EspHomeDevice("kueche")                              -> kueche.local
        kueche: nxrunner.devices.esphome:EspHomeDevice("kueche", host="192.168.1.42")       auch "http://192.168.1.42/"
        kueche: nxrunner.devices.esphome:EspHomeDevice("kueche", yaml_file="/config/esphome/kueche.yaml")
        kueche: nxrunner.devices.esphome:EspHomeDevice("kueche", encryption_key="base64...")
        kueche: nxrunner.devices.esphome:EspHomeDevice("kueche", live=True, log_level="debug")

    live=True haelt die native API offen: Zustaende kommen sofort als Variablen (object_id der Entity),
    Logs werden mitgeschrieben, Kommandos laufen ueber die bestehende Verbindung.
    Ohne live wird pro Kommando kurz verbunden.

    Aus der yml werden web_server.version, api.encryption.key bzw. api.password und als Host
    wifi.use_address bzw. wifi.manual_ip.static_ip uebernommen.

    Jobs:
        {"type": "kueche", "op": "refresh"}                                  Entities und Zustaende neu laden
        {"type": "kueche", "op": "states"}                                   Zustaende aller Entities
        {"type": "kueche", "op": "command", "entity": "licht", "state": "toggle", "brightness": 0.5}
                                                                             Kommando, Parameter siehe send_command
        {"type": "kueche", "op": "live_start"} / {"op": "live_stop"}         Live-Verbindung
        {"type": "kueche", "op": "logs"}                                     mitgeschriebene Logs (nur live)
        {"type": "kueche", "op": "switch", "key": "relais", "state": true}   key = Key, object_id oder Name
        {"type": "kueche", "op": "button", "name": "Neustart"}               Button ueber die native API
        {"type": "kueche", "op": "web_button", "name": "neustart"}           Button ueber die Web-API
        {"type": "kueche", "op": "number", "name": "helligkeit", "value": 5} Number ueber die Web-API
        {"type": "kueche", "op": "service", "name": "play", "params": {}}    User-Service (api: actions:)
        {"type": "kueche", "op": "read_event_stream"}                        Web-Events als Variablen lesen
    """
    MODULES = ['aioesphomeapi']

    def __init__(self, name, password=None, yaml_file=None, host=None, encryption_key=None, api_port=6053,
                 live=False, log_level='info'):
        super().__init__(name)
        self.define_vars('host', 'web_version')
        self.define_static_sigs()
        import aioesphomeapi as aio
        self.aio = aio
        self.host = host
        self.api_port = api_port
        self.password = password
        self.encryption_key = encryption_key
        self.states = {}
        self.entities: "list[aio.EntityInfo]" = u.List()
        self.services: "list[aio.UserService]" = []
        self.device_info = None
        self.values = {}
        self.value_times = {}
        self.entity_vars = {}
        self.logs = deque(maxlen=500)
        self.log_level = log_level
        self.live_api = None
        self.live_loop = None
        self.live_thread = None
        self.live_running = False
        self.event_stream = None
        self.web_version = None
        self.yml = {}
        self._init_yml(yaml_file)
        self.host = self.default_host(self.host or name)
        if self.web_version is None:
            self.web_version = self.get_esphome_web_generation(self.host)
        if live:
            self.start_live()

    @staticmethod
    def default_host(name: str) -> str:
        """ URLs werden auf den Host gekuerzt, IP-Adressen und Hostnamen mit Domain bleiben, sonst <name>.local """
        if '://' in name:
            name = urlparse(name).hostname or name
        name = name.strip('/')
        return name if '.' in name or ':' in name else name + '.local'

    def _init_yml(self, yaml_file):
        if yaml_file is not None:
            self.yml = load_esphome_yaml(yaml_file)
            if 'web_server' in self.yml:
                # ESPHome nutzt ohne Angabe Version 2
                self.web_version = (self.yml['web_server'] or {}).get('version', 2)
            wifi = self.yml.get('wifi') or {}
            if self.host is None:
                self.host = wifi.get('use_address') or (wifi.get('manual_ip') or {}).get('static_ip')
            api = self.yml.get('api') or {}
            if self.encryption_key is None:
                self.encryption_key = (api.get('encryption') or {}).get('key')
            if self.password is None:
                self.password = api.get('password')

    @staticmethod
    def get_esphome_web_generation(ip: str, port: int = 80, timeout: float = 3.0) -> Optional[int]:
        """
        Detect ESPHome web UI generation (1, 2, or 3).

        Gen 1 - Old static HTML, plain table layout, no JS framework
        Gen 2 - Lit-based web components (esphome-webserver v2), dark theme
        Gen 3 - Lit + ESPHome dashboard v3, richer UI, entity cards

        Returns 1, 2, 3, or None if not detectable.
        """
        try:
            r = requests.get(f"http://{ip}:{port}/", timeout=timeout)
            if r.status_code != 200:
                return None
            html = r.text
        except Exception:
            return None

        # --- Gen 3 markers ---
        if any([
            "oi.esphome.io/v3" in html,
            "esphome-web-app" in html,
            "esphome-dashboard" in html,
            'type="module"' in html and "lit" in html.lower(),
            re.search(r'esphome[_-]webserver[/@]3', html, re.IGNORECASE),
            "ha-card" in html,
        ]):
            return 3

        # --- Gen 2 markers ---
        if any([
            "oi.esphome.io/v2" in html,
            "<esp-app" in html,
            "esphome-webserver" in html,
            re.search(r'esphome[_-]webserver[/@]2', html, re.IGNORECASE),
            "customElements" in html,
            "LitElement" in html,
            re.search(r'<esphome-', html),  # custom elements
        ]):
            return 2

        # --- Gen 1 markers ---
        if any([
            "<table" in html and "ESPHome" in html,
            re.search(r'<form[^>]+action=["\']/(restart|update)', html),
            "esp8266" in html.lower() or "esp32" in html.lower(),
            re.search(r'<title>ESPHome</title>', html, re.IGNORECASE),
        ]):
            return 1

        return None

    # --- Web-API (web_server) ---------------------------------------------------------------

    @staticmethod
    def esphome_read_webeventstream_v3(ip):
        """
        Connect to an ESPHome device's web API v3 event stream and yield events.

        Args:
            ip: IP address or hostname of the ESPHome device

        Yields:
            dict: Parsed event data from the stream
        """
        url = f"http://{ip}/events"
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            event_type = None
            data_buffer = []
            for line in response.iter_lines(decode_unicode=True):
                if line is None:
                    continue
                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data_buffer.append(line[5:].strip())
                elif line == "":
                    # Empty line signals end of event
                    if data_buffer:
                        data_str = "\n".join(data_buffer)
                        try:
                            data = json.loads(data_str)
                        except json.JSONDecodeError:
                            data = data_str
                        yield {
                            "event": event_type,
                            "data": data
                        }
                    event_type = None
                    data_buffer = []

    def read_event_stream(self):
        """
        Speichert die Zustaende aus dem Event-Stream in self.states und als Variablen, z.B.
            "sensor-temperature": {"event": "state", "id": "sensor-temperature", "value": 23.5, "state": "23.5 °C"}
        """
        try:
            for event in self.esphome_read_webeventstream_v3(self.host):
                data = event.get("data")
                if not isinstance(data, dict) or not data.get("id"):
                    continue
                self.states[data["id"]] = {"event": event.get("event"), **data}
                if 'value' in data:
                    self.define_vars(data['id'])
                    self.set_var(dict(setvar=data['id'], value=data['value']))
        except Exception as e:
            self.error(e)

    def execute_read_event_stream(self, data={}):
        if self.event_stream is not None and self.event_stream.is_alive():
            return self.success('laeuft bereits')
        self.event_stream = Thread(target=self.read_event_stream, daemon=True)
        self.event_stream.start()
        return self.success()

    def hostname(self):
        return self.host

    def post(self, path, params=dict()):
        url = 'http://' + self.hostname() + path
        r = requests.post(url, params=params, timeout=10)
        r.raise_for_status()
        return r.content

    def get(self, path, params=dict()):
        url = 'http://' + self.hostname() + path
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    def rest_text(self, id):
        """ Text-Sensor ueber die Web-API, z.B. {"id": "text_sensor-version", "value": "...", "state": "..."} """
        return self.get(f'/text_sensor/{id}')

    def rest_light_turn_on(self, id, brightness=None):
        if brightness is None:
            return self.post(f'/light/{id}/turn_on')
        else:
            return self.post(f'/light/{id}/turn_on', dict(brightness=brightness))

    def rest_light_turn_off(self, id):
        return self.post(f'/light/{id}/turn_off')

    def web_v3_number_set(self, name, value):
        self.post('/number/' + name + '/set', dict(value=value))

    def web_v3_button_press(self, name):
        self.post('/button/' + name + '/press')

    # --- Native API (aioesphomeapi) ---------------------------------------------------------

    def _client(self):
        return self.aio.APIClient(self.host, self.api_port, self.password, noise_psk=self.encryption_key)

    async def _load(self, api):
        self.device_info = await api.device_info()
        entities, services = await api.list_entities_services()
        self.entities = u.List()
        self.entities.add_all(entities)
        self.services = services
        for e in entities:
            if e.object_id not in self.entity_vars:
                # object_ids, die mit Attributen des Runners kollidieren (z.B. host), bekommen das Praefix entity_
                var = e.object_id if not hasattr(self, e.object_id) else 'entity_' + e.object_id
                self.entity_vars[e.object_id] = var
                self.define_vars(var)
        self.update_sigs()

    @staticmethod
    async def _run_ops(api, ops):
        results = []
        for op in ops:
            result = op(api)
            # Kommandos wie switch_command sind synchron, execute_service ist async
            if inspect.isawaitable(result):
                result = await result
            results.append(result)
        return results

    async def _session(self, *ops):
        """ Verbindet sich, laedt Entities, Services und Zustaende, fuehrt ops(api) aus und trennt wieder """
        api = self._client()
        await api.connect(login=True)
        try:
            await self._load(api)
            api.subscribe_states(self._on_state)
            results = await self._run_ops(api, ops)
            # Das Geraet sendet nach dem Abonnieren alle Zustaende, kurz darauf warten
            await asyncio.sleep(0.5)
            return results
        finally:
            await api.disconnect()

    def run_api(self, *ops):
        """ Fuehrt ops(api) ueber die Live-Verbindung aus, falls vorhanden, sonst in einer eigenen Verbindung """
        try:
            if self.live_api is not None:
                future = asyncio.run_coroutine_threadsafe(self._run_ops(self.live_api, ops), self.live_loop)
                return future.result(timeout=30)
            return asyncio.run(self._session(*ops))
        except Exception as e:
            self.error(e)
            raise e

    # --- Live-Verbindung: Zustaende und Logs ------------------------------------------------

    def _on_state(self, state):
        e = self.find_entity(state.key, required=False)
        if e is None:
            return
        self.values[e.object_id] = state
        self.value_times[e.object_id] = time.time()
        value = self.state_value(state)
        var = self.entity_vars.get(e.object_id)
        if var is not None and getattr(self, var, None) != value:
            self.set_var(dict(setvar=var, value=value))

    def _on_log(self, msg):
        line = msg.message.decode('utf-8', errors='replace') if isinstance(msg.message, bytes) else str(msg.message)
        self.logs.append(re.sub(r'\x1b\[[0-9;]*m', '', line))

    async def _live(self):
        """ Haelt die Verbindung offen, abonniert Zustaende und Logs und verbindet sich bei Abbruch neu """
        while self.live_running:
            stopped = asyncio.Event()

            async def on_stop(expected_disconnect):
                stopped.set()

            api = self._client()
            try:
                await api.connect(on_stop=on_stop, login=True)
                await self._load(api)
                api.subscribe_states(self._on_state)
                api.subscribe_logs(self._on_log, log_level=self.aio.LogLevel[f'LOG_LEVEL_{self.log_level.upper()}'])
                self.live_api = api
                self.info(f"Live-Verbindung zu {self.host}")
                while self.live_running and not stopped.is_set():
                    await asyncio.sleep(0.5)
            except Exception as e:
                self.error(f"Live-Verbindung: {e}")
            finally:
                self.live_api = None
                try:
                    await api.disconnect()
                except Exception:
                    pass
            if self.live_running:
                await asyncio.sleep(10)

    def start_live(self):
        if self.live_thread is not None and self.live_thread.is_alive():
            return
        self.live_running = True
        self.live_loop = asyncio.new_event_loop()
        self.live_thread = Thread(target=self.live_loop.run_until_complete, args=(self._live(),), daemon=True)
        self.live_thread.start()

    def stop_live(self):
        self.live_running = False
        if self.live_thread is not None:
            self.live_thread.join(timeout=15)
            if not self.live_thread.is_alive():
                self.live_loop.close()
        self.live_thread = None

    # --- Entities und Kommandos -------------------------------------------------------------

    def find_entity(self, ident, cls=None, required=True):
        """ Sucht eine Entity per Key, object_id oder Name, optional eingeschraenkt auf eine Info-Klasse """
        for e in self.entities:
            if cls is not None and not isinstance(e, cls):
                continue
            if ident in (e.key, str(e.key), e.object_id, e.name):
                return e
        if required:
            raise KeyError(f"Entity '{ident}' nicht gefunden")
        return None

    @staticmethod
    def entity_type(e) -> str:
        return type(e).__name__.removesuffix('Info')

    @staticmethod
    def state_value(state):
        """ Einfacher Wert eines Zustands, z.B. fuer Variablen """
        if getattr(state, 'missing_state', False):
            return None
        for attr in ('state', 'position', 'current_temperature', 'epoch_seconds'):
            if hasattr(state, attr):
                value = getattr(state, attr)
                return value.name if hasattr(value, 'name') else value
        if hasattr(state, 'year'):
            return f'{state.year:04d}-{state.month:02d}-{state.day:02d}'
        if hasattr(state, 'hour'):
            return f'{state.hour:02d}:{state.minute:02d}:{state.second:02d}'
        if hasattr(state, 'latest_version'):
            return state.latest_version
        return None

    def state_text(self, e) -> str:
        state = self.values.get(e.object_id)
        if state is None:
            return ''
        value = self.state_value(state)
        if isinstance(state, self.aio.LightState) and state.state:
            return f'ON {round(state.brightness * 100)}%'
        if isinstance(state, self.aio.ClimateState):
            return f'{state.mode.name} {state.current_temperature} -> {state.target_temperature}'
        if isinstance(state, self.aio.UpdateState):
            return f'{state.current_version} -> {state.latest_version}'
        if isinstance(value, bool):
            return 'ON' if value else 'OFF'
        if isinstance(value, float):
            value = round(value, 2)
        unit = getattr(e, 'unit_of_measurement', '')
        return f'{value} {unit}'.strip() if value is not None else ''

    @staticmethod
    def _bool(value):
        if isinstance(value, str):
            return value.lower() in ('1', 'true', 'on', 'yes', 'ja')
        return bool(value)

    @staticmethod
    def _float(value):
        return None if value is None or value == '' else float(value)

    @staticmethod
    def _enum(cls, value):
        if value is None or value == '':
            return None
        if isinstance(value, int) or str(value).isdigit():
            return cls(int(value))
        return cls[str(value).upper()]

    def _state_param(self, e, value):
        """ state=toggle schaltet anhand des letzten bekannten Zustands um """
        if value == 'toggle':
            current = self.values.get(e.object_id)
            return not (current is not None and bool(getattr(current, 'state', False)))
        return None if value is None else self._bool(value)

    def send_command(self, api, e, p: dict):
        """
        Sendet ein Kommando passend zum Entity-Typ, Parameter (Strings werden umgewandelt):
            Switch:        state (true/false/toggle)
            Button:        -
            Light:         state, brightness (0-1), rgb ("r,g,b" 0-1), color_temperature (mireds), effect, transition_length
            Fan:           state, speed_level, oscillating, direction (forward/reverse), preset_mode
            Cover:         action (open/close/stop), position (0-1), tilt (0-1)
            Valve:         action (open/close/stop), position (0-1)
            Number:        value
            Select:        option
            Text:          value
            Lock:          command (lock/unlock/open), code
            Climate:       mode (off/heat/cool/auto/...), target_temperature, target_temperature_low/high,
                           fan_mode, preset, target_humidity
            Siren:         state, tone, volume, duration
            Date:          value (YYYY-MM-DD)
            Time:          value (HH:MM:SS)
            DateTime:      value (Epoch-Sekunden)
            MediaPlayer:   command (play/pause/stop/mute/unmute/toggle/...), volume (0-1), media_url
            Update:        command (install/check)
            AlarmControlPanel: command (arm_away/arm_home/disarm/...), code
        """
        aio, key = self.aio, e.key
        if isinstance(e, aio.SwitchInfo):
            return api.switch_command(key, self._state_param(e, p.get('state', True)))
        if isinstance(e, aio.ButtonInfo):
            return api.button_command(key)
        if isinstance(e, aio.LightInfo):
            rgb = p.get('rgb')
            if isinstance(rgb, str):
                rgb = tuple(float(c) for c in rgb.split(','))
            return api.light_command(key, state=self._state_param(e, p.get('state', True)),
                                     brightness=self._float(p.get('brightness')), rgb=rgb,
                                     color_temperature=self._float(p.get('color_temperature')),
                                     effect=p.get('effect') or None,
                                     transition_length=self._float(p.get('transition_length')))
        if isinstance(e, aio.FanInfo):
            return api.fan_command(key, state=self._state_param(e, p.get('state', True)),
                                   speed_level=int(p['speed_level']) if p.get('speed_level') not in (None, '') else None,
                                   oscillating=self._bool(p['oscillating']) if 'oscillating' in p else None,
                                   direction=self._enum(aio.FanDirection, p.get('direction')),
                                   preset_mode=p.get('preset_mode') or None)
        if isinstance(e, (aio.CoverInfo, aio.ValveInfo)):
            action = p.get('action')
            position = {'open': 1.0, 'close': 0.0}.get(action, self._float(p.get('position')))
            if isinstance(e, aio.CoverInfo):
                return api.cover_command(key, position=position, tilt=self._float(p.get('tilt')), stop=action == 'stop')
            return api.valve_command(key, position=position, stop=action == 'stop')
        if isinstance(e, aio.NumberInfo):
            return api.number_command(key, float(p['value']))
        if isinstance(e, aio.SelectInfo):
            return api.select_command(key, p.get('option', p.get('value')))
        if isinstance(e, aio.TextInfo):
            return api.text_command(key, str(p['value']))
        if isinstance(e, aio.LockInfo):
            return api.lock_command(key, self._enum(aio.LockCommand, p.get('command', 'lock')), code=p.get('code') or None)
        if isinstance(e, aio.ClimateInfo):
            return api.climate_command(key, mode=self._enum(aio.ClimateMode, p.get('mode')),
                                       target_temperature=self._float(p.get('target_temperature')),
                                       target_temperature_low=self._float(p.get('target_temperature_low')),
                                       target_temperature_high=self._float(p.get('target_temperature_high')),
                                       fan_mode=self._enum(aio.ClimateFanMode, p.get('fan_mode')),
                                       preset=self._enum(aio.ClimatePreset, p.get('preset')),
                                       target_humidity=self._float(p.get('target_humidity')))
        if isinstance(e, aio.SirenInfo):
            return api.siren_command(key, state=self._state_param(e, p.get('state', True)), tone=p.get('tone') or None,
                                     volume=self._float(p.get('volume')),
                                     duration=int(p['duration']) if p.get('duration') not in (None, '') else None)
        if isinstance(e, aio.DateInfo):
            year, month, day = (int(x) for x in str(p['value']).split('-'))
            return api.date_command(key, year, month, day)
        if isinstance(e, aio.TimeInfo):
            hour, minute, second = (int(x) for x in (str(p['value']) + ':0:0').split(':')[:3])
            return api.time_command(key, hour, minute, second)
        if isinstance(e, aio.DateTimeInfo):
            return api.datetime_command(key, int(p['value']))
        if isinstance(e, aio.MediaPlayerInfo):
            return api.media_player_command(key, command=self._enum(aio.MediaPlayerCommand, p.get('command')),
                                            volume=self._float(p.get('volume')), media_url=p.get('media_url') or None)
        if isinstance(e, aio.UpdateInfo):
            return api.update_command(key, self._enum(aio.UpdateCommand, p.get('command', 'install')))
        if isinstance(e, aio.AlarmControlPanelInfo):
            return api.alarm_control_panel_command(key, self._enum(aio.AlarmControlPanelCommand, p['command']),
                                                   code=p.get('code') or None)
        raise ValueError(f"{self.entity_type(e)} '{e.name}' unterstuetzt keine Kommandos")

    def command(self, ident, **params):
        """ Kommando an eine Entity (Key, object_id oder Name), Parameter siehe send_command """
        return self.run_api(lambda api: self.send_command(api, self.find_entity(ident), params))[0]

    def switch_command(self, key, state=True):
        return self.run_api(lambda api: self.send_command(api, self.find_entity(key, self.aio.SwitchInfo), dict(state=state)))[0]

    def button_press(self, name):
        return self.run_api(lambda api: self.send_command(api, self.find_entity(name, self.aio.ButtonInfo), {}))[0]

    def run_service(self, name, params={}):
        def _service(api):
            for service in self.services:
                if service.name == name:
                    return api.execute_service(service, params)
            raise KeyError(f"Service '{name}' nicht gefunden")
        return self.run_api(_service)[0]

    # --- Analoge Eingaenge (Schnittstelle wie nxbot.i2c.ADS) --------------------------------

    """ object_ids der analogen Eingaenge, Kanal = Index """
    ANALOG_IDS = ['analog_a0', 'analog_a1', 'analog_a2']

    def analog_channels(self) -> list[int]:
        """ :return: Kanaele, fuer die eine Entity analog_a<n> vorhanden ist """
        ids = {e.object_id for e in self.entities}
        return [i for i, object_id in enumerate(self.ANALOG_IDS) if object_id in ids]

    def read_a(self, channel=0):
        """ Wert von analog_a<channel>, ohne Live-Verbindung wird er neu vom Geraet gelesen """
        channel = int(channel)
        if channel >= len(self.ANALOG_IDS):
            raise KeyError(f"Kanal {channel} nicht vorhanden")
        if self.live_api is None:
            self.run_api()
        state = self.values.get(self.ANALOG_IDS[channel])
        if state is None:
            raise KeyError(f"{self.ANALOG_IDS[channel]} hat keinen Wert")
        return self.state_value(state)

    def update_sigs(self):
        """
        Signaturen passend zu den Entities: read_a nur, wenn alle analogen Eingaenge (analog_a0-2) vorhanden sind,
        dann kann das Geraet als ADC verwendet werden (z.B. von nxrunner.app.anemometer).
        """
        self.signatures.items = [s for s in self.signatures if 'read_a' not in s]
        if len(self.analog_channels()) == len(self.ANALOG_IDS):
            self.define_sig(d.PInt('read_a', 1), d.PInt('channel', 0)
                            .desc(f"Kanal 0-{len(self.ANALOG_IDS) - 1} = {', '.join(self.ANALOG_IDS)}")) \
                .set_description("Liest einen analogen Eingang").r(success='bool', message='str', value='float')

    def define_static_sigs(self):
        self.define_sig(d.PStr('op', 'refresh')).set_description("Entities, Services und Zustände neu laden") \
            .r(success='bool', entities='int')
        self.define_sig(d.PStr('op', 'states')).set_description("Zustände aller Entities") \
            .r(success='bool', states='dict')
        self.define_sig(d.PStr('op', 'command'), d.Param('entity', 'str'), d.PStr('state', 'toggle')) \
            .set_description("Kommando an eine Entity, weitere Parameter siehe send_command").r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'live_start')).set_description("Live-Verbindung starten").r(success='bool')
        self.define_sig(d.PStr('op', 'live_stop')).set_description("Live-Verbindung stoppen").r(success='bool')
        self.define_sig(d.PStr('op', 'logs')).set_description("Mitgeschriebene Logs (nur live)").r(success='bool', lines='list')

    def execute(self, data):
        if 'read_a' in data:
            try:
                return self.success('ok', value=self.read_a(data.get('channel', 0)))
            except Exception as e:
                return self.fail(str(e))
        return super().execute(data)

    # --- Jobs -------------------------------------------------------------------------------

    def execute_refresh(self, data={}):
        self.run_api()
        return self.success(entities=len(self.entities))

    def execute_command(self, data):
        params = {k: v for k, v in data.items() if k not in ('type', 'op', 'entity', 'title')}
        self.command(data['entity'], **params)
        return self.success('done')

    def execute_states(self, data={}):
        if self.live_api is None:
            self.run_api()
        return self.success(states={e.object_id: self.state_value(self.values[e.object_id])
                                    for e in self.entities if e.object_id in self.values})

    def execute_logs(self, data={}):
        return self.success(lines=list(self.logs))

    def execute_live_start(self, data={}):
        self.start_live()
        return self.success(ui_reload=True)

    def execute_live_stop(self, data={}):
        self.stop_live()
        return self.success(ui_reload=True)

    def execute_switch(self, data):
        self.switch_command(data['key'], data.get('state', True))
        return self.success('done')

    def execute_button(self, data):
        """ Button ueber die native API druecken, execute_web_button nutzt die Web-API """
        self.button_press(data['name'])
        return self.success('done')

    def execute_web_button(self, data):
        self.web_v3_button_press(data['name'])
        return self.success()

    def execute_number(self, data):
        self.web_v3_number_set(data['name'], data['value'])
        return self.success()

    def execute_service(self, data):
        self.run_service(data['name'], data.get('params', {}))
        return self.success('done')

    # --- UI ---------------------------------------------------------------------------------

    def cmd_btn(self, e, title, hint=None, **params):
        return self.action_btn(dict(type=self.type, op='command', entity=e.key, title=title, **params), hint=hint)

    def cmd_input(self, e, param, input_html, title="Setzen", hint="Sendet den eingestellten Wert an das Gerät",
                  **params):
        """ Eingabefeld und Button, der den Wert als param mitsendet """
        id = 'esph_' + str(e.key) + '_' + param
        data = json.dumps(dict(type=self.type, op='command', entity=e.key, **params))
        js = f'exec_job(Object.assign({data}, {{{json.dumps(param)}: document.getElementById("{id}").value}}));'
        return input_html.replace('<input', f'<input id="{id}"', 1).replace('<select', f'<select id="{id}"', 1) \
            + w.button_js(title, js, hint=hint)

    def entity_controls(self, e) -> str:
        aio = self.aio
        on_off = self.cmd_btn(e, "An", "Einschalten", state=True) + self.cmd_btn(e, "Aus", "Ausschalten", state=False) \
            + self.cmd_btn(e, "Umschalten", "Schaltet anhand des zuletzt bekannten Zustands um", state='toggle')
        if isinstance(e, (aio.SwitchInfo, aio.FanInfo, aio.SirenInfo)):
            return on_off
        if isinstance(e, aio.LightInfo):
            return on_off + self.cmd_input(e, 'brightness', '<input type="range" min="0" max="1" step="0.05">',
                                           hint="Schaltet mit der eingestellten Helligkeit ein", state=True)
        if isinstance(e, aio.ButtonInfo):
            return self.cmd_btn(e, "Drücken", "Löst den Button auf dem Gerät aus")
        if isinstance(e, (aio.CoverInfo, aio.ValveInfo)):
            res = self.cmd_btn(e, "Auf", "Ganz öffnen", action='open') + self.cmd_btn(e, "Zu", "Ganz schließen", action='close')
            if e.supports_stop:
                res += self.cmd_btn(e, "Stop", "Hält die Bewegung an", action='stop')
            return res
        if isinstance(e, aio.LockInfo):
            res = self.cmd_btn(e, "Sperren", "Verriegelt das Schloss", command='lock') \
                + self.cmd_btn(e, "Entsperren", "Entriegelt das Schloss", command='unlock')
            if e.supports_open:
                res += self.cmd_btn(e, "Öffnen", "Entriegelt und öffnet die Tür (Falle)", command='open')
            return res
        if isinstance(e, aio.NumberInfo):
            return self.cmd_input(e, 'value', f'<input type="number" min="{e.min_value}" max="{e.max_value}" step="{e.step}">')
        if isinstance(e, aio.SelectInfo):
            options = ''.join(f'<option>{html.escape(o)}</option>' for o in e.options)
            return self.cmd_input(e, 'option', f'<select>{options}</select>')
        if isinstance(e, aio.TextInfo):
            return self.cmd_input(e, 'value', '<input type="text">')
        if isinstance(e, aio.DateInfo):
            return self.cmd_input(e, 'value', '<input type="date">')
        if isinstance(e, aio.TimeInfo):
            return self.cmd_input(e, 'value', '<input type="time" step="1">')
        if isinstance(e, aio.ClimateInfo):
            modes = ''.join(f'<option>{m.name.lower()}</option>' for m in e.supported_modes)
            return self.cmd_input(e, 'mode', f'<select>{modes}</select>', title="Modus",
                                  hint="Setzt den Betriebsmodus") \
                + self.cmd_input(e, 'target_temperature',
                                 f'<input type="number" step="{e.visual_target_temperature_step or 0.5}">', title="Soll",
                                 hint="Setzt die Solltemperatur")
        if isinstance(e, aio.MediaPlayerInfo):
            return self.cmd_btn(e, "Play", "Wiedergabe starten", command='play') \
                + self.cmd_btn(e, "Pause", "Wiedergabe pausieren", command='pause') \
                + self.cmd_btn(e, "Stop", "Wiedergabe beenden", command='stop') \
                + self.cmd_input(e, 'volume', '<input type="range" min="0" max="1" step="0.05">', title="Lautstärke",
                                 hint="Setzt die eingestellte Lautstärke")
        if isinstance(e, aio.UpdateInfo):
            return self.cmd_btn(e, "Prüfen", "Sucht nach neuer Firmware", command='check') \
                + self.cmd_btn(e, "Installieren", "Installiert die verfügbare Firmware (Gerät startet neu)", command='install')
        if isinstance(e, aio.AlarmControlPanelInfo):
            return self.cmd_btn(e, "Scharf (abwesend)", "Alarmanlage scharf schalten, niemand zu Hause", command='arm_away') \
                + self.cmd_btn(e, "Scharf (zuhause)", "Alarmanlage scharf schalten, jemand zu Hause", command='arm_home') \
                + self.cmd_btn(e, "Unscharf", "Alarmanlage unscharf schalten", command='disarm')
        return ''

    def part_index(self, p: base.Page, params={}):
        p.right(w.img(w.Img.CHIP))
        if self.entities.is_empty() and self.live_api is None:
            self.delayed(0.1, self._load_background)
        with p.section(h="Gerät"):
            p.div(f"Host: {self.host}, Web-Version: {self.web_version}")
            if self.device_info is not None:
                p.div(f"{self.device_info.name}, ESPHome {self.device_info.esphome_version}, {self.device_info.model}")
            if self.live_api is not None:
                p.div("Live-Verbindung aktiv " + self.action_btn(dict(
                    title="Live stoppen", op='live_stop', type=self.type, hint="Trennt die dauerhafte Verbindung")))
            else:
                p.div(self.action_btn(dict(
                    title="Live starten (Zustände und Logs)", op='live_start', type=self.type,
                    hint="Hält die Verbindung offen: Zustände kommen sofort als Variablen, Logs werden mitgeschrieben")))
            p(self.action_btn(dict(title="Entities neu laden", op='refresh', type=self.type,
                                   hint="Lädt Entities, Services und Zustände neu vom Gerät")))
            p.div("Letzte Aktualisierung: " + self.age_html())

        with p.section(h="Entities"):
            rows = ''
            for e in self.entities:
                rows += '<tr>' + ''.join(f'<td>{c}</td>' for c in (
                    html.escape(self.entity_type(e)), html.escape(e.name), html.escape(e.object_id),
                    html.escape(self.state_text(e)), self.entity_controls(e))) + '</tr>'
            p('<table><tr><th>Typ</th><th>Name</th><th>ID</th><th>Zustand</th><th></th></tr>' + rows + '</table>')

        if len(self.services) > 0:
            with p.section(h="Services"):
                for s in self.services:
                    if len(s.args) == 0:
                        p.div(self.action_btn(dict(title=s.name, op='service', name=s.name, type=self.type,
                                                   hint=f"Führt den Service {s.name} auf dem Gerät aus")))
                    else:
                        p.div(f"{html.escape(s.name)}({', '.join(html.escape(a.name) for a in s.args)})")

        with p.section(h="Event Stream (Web-API)"):
            p(self.action_btn(dict(title="Start Event Reader", op='read_event_stream', type=self.type,
                                   hint="Liest Zustände über den Event-Stream der Web-API (Port 80)")))
            if len(self.states) > 0:
                p.grid([dict(id=k, state=v.get('state', v.get('value'))) for k, v in self.states.items()],
                       ['id', 'state'], params)

        with p.section(h="Links"):
            links = [
                w.a("Logs", self.link(self.part_logs)),
                w.a("YML", self.link(self.part_yml)),
                w.a("YML Buttons", self.link(self.part_yml_buttons)),
                w.a("Web UI", f'http://{self.hostname()}/')
            ]
            with self.opt_dispatcher() as d:
                for esp_home in d.get_runner_by_class('EspHome'):
                    links.append(w.a("ESPHome Geräte", f'?type={esp_home.type}'))
            p.ul(links)

    # Zaehlt das Alter im Browser hoch, ausgehend vom Alter beim Rendern (unabhaengig von der Uhr des Clients).
    # Jedes Element merkt sich, wann es zuerst gesehen wurde, so stimmt es auch nach erneutem Laden des Inhalts;
    # der Timer laeuft nur einmal je Seite.
    AGE_JS = '''<script type="text/javascript">(function() {
    if (window.esphAgeTimer) { return; }
    function fmt(s) { return s < 60 ? s + ' s' : (s < 3600 ? Math.floor(s / 60) + ' min ' + (s % 60) + ' s' : Math.floor(s / 3600) + ' h'); }
    window.esphAgeTimer = setInterval(function() {
        document.querySelectorAll('.esph_age').forEach(function(el) {
            if (!el.dataset.t0) { el.dataset.t0 = Date.now(); }
            el.textContent = fmt(Math.round(parseFloat(el.dataset.age) + (Date.now() - el.dataset.t0) / 1000));
        });
    }, 250);
})();</script>'''

    @staticmethod
    def format_age(seconds: float) -> str:
        s = round(seconds)
        if s < 60:
            return f'{s} s'
        return f'{s // 60} min {s % 60} s' if s < 3600 else f'{s // 3600} h'

    def age_html(self) -> str:
        """ Alter der letzten Aktualisierung (irgendein Wert vom Geraet), wird per AGE_JS sekuendlich hochgezaehlt """
        if len(self.value_times) == 0:
            return 'noch keine Werte'
        age = time.time() - max(self.value_times.values())
        return f'<span class="esph_age" data-age="{age:.1f}" title="Zeit seit dem letzten Wert vom Gerät">' \
               f'{self.format_age(age)}</span>' + self.AGE_JS

    def _load_background(self):
        """ Laedt Entities fuer die Seite nach, Fehler sind bereits von run_api geloggt """
        try:
            self.run_api()
        except Exception:
            pass

    def part_logs(self, p: base.Page, params={}):
        if self.live_api is None:
            p.div("Logs werden nur bei aktiver Live-Verbindung empfangen.")
        p.pre('\n'.join(self.logs))

    def part_yml(self, p: base.Page, params={}):
        p.pre(self.yml)

    def part_yml_buttons(self, p: base.Page, params={}):
        for btn in self.yml.get('button') or []:
            if 'name' in btn:
                p.div(self.action_btn(dict(
                    title=btn['name'],
                    type=self.type,
                    op='web_button',
                    name=btn['name'],
                    hint="Drückt den Button über die Web-API"
                )))
