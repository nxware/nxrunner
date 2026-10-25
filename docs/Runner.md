# Runner in nxrunner

Übersicht über die Runner, die nxrunner mitbringt. Wie man einen eigenen Runner schreibt, steht in der
[README](../README.md#eigene-runner-erstellen).

Alle Runner werden mit einem Job-`dict` aufgerufen, z.B. über REST:

```
curl "http://127.0.0.1:7070/?type=wsl&op=drives"
```

In diesem Dokument sind Jobs kurz als `op=name param=wert` geschrieben, das entspricht
`{"type": "<runner>", "op": "name", "param": "wert"}`. Parameter in `[...]` sind optional.

## Übersicht

Registrierte Runner (Entry-Points `nweb_runner` in `pyproject.toml`, ladbar über die Registry im Webinterface):

| Entry-Point | Klasse | Zweck | Zusätzliche Module |
|---|---|---|---|
| `wsl` | `nxrunner.system:Wsl` | Laufwerke in der WSL mounten, Windows-Befehle | |
| `nfile` | `nxrunner.system:NFile` | Dateien von Laufwerken indexieren und durchsuchen | |
| `backup` | `nxrunner.system:Backup` | Inkrementelle Backups aus Quellen in Ziele | (`paramiko`, `smbprotocol`) |
| `smb` | `nxrunner.fs:Smb` | Zugriff auf SMB-/Windows-Freigaben | `smbprotocol` |
| `mongo` | `nxrunner.db.mongo:MongoDB` | MongoDB: einfügen, suchen, Dateien sammeln | `pymongo` |
| `nwebdir` | `nxrunner.nweb:NWebDir` | Verzeichnis mit nweb-Gruppe abgleichen | |
| `dashboard_cyd` | `nxrunner.devices:DashboardCYD` | Dashboard, das zusätzlich an ein CYD-Display sendet | `requests` |
| `m5_multi` | `nxrunner.devices.esp_m5:EspM5` | M5StickS3 mit Firmware esp_m5_multi (Werte und Aktionen) | `requests` |
| `nimbot_b1` | `nxrunner.devices.nimbot:NimbotB1` | Etikettendrucker Niimbot B1 per Bluetooth | `pillow`, `bleak` |
| `joystick` | `nxrunner.hw.i2c:JoystickM5` | M5Stack Joystick 2 per I2C | `smbus2` |
| `elrs_display` | `nxrunner.hw.elrs:ElrsDisplay` | Kanalwerte eines ExpressLRS-Empfängers, Bindings auf Jobs | |
| `audio_convert` | `nxrunner.audio:Convert` | WAV nach MP3 konvertieren | `pydub` (+ ffmpeg) |
| `pihole_api` | `nxrunner.apis:PiHoleApi` | Statistik eines Pi-hole 6 | `pihole6api` |
| `trello` | `nxrunner.apis:Trello` | Boards eines Trello-Kontos | `py-trello` |

Fehlende Module lassen sich in der Registry mit `op=registry_pip name=<entry>` installieren, oder der Runner wird
mit `op=registry_docker` in einem Container gestartet (siehe [NxRunner](#nxrunner-dispatcher)).

Runner mit Pflichtparametern im Konstruktor (`nwebdir`: `path`, `trello`: `api_key`, `token`) brauchen beim Laden
`arg_<param>=...` bzw. einen Eintrag in der Konfiguration:

```
nwebdir: nxrunner.nweb:NWebDir(path="/data/bilder")
trello: nxrunner.apis:Trello(api_key="...", token="...")
```

Weitere Klassen, die nicht als Entry-Point registriert sind: [`NxRunner`](#nxrunner-dispatcher) (der Dispatcher
selbst), [`DockerProxy`](#dockerproxy), [`BluetoothDevice`](#bluetoothdevice-basisklasse) (Basisklasse) und
[`DbUi`](#dbui).

---

## System

### Wsl

`nxrunner.system:Wsl(type='wsl', timeout=60)`, Tags `system`, `wsl`

Für die Ausführung innerhalb der WSL: Windows-Laufwerke (z.B. `D:`) oder Netzwerkfreigaben (`\\server\share`) per
drvfs mounten und Befehle auf der Windows-cmd ausführen.

| Job | Beschreibung |
|---|---|
| `op=mount drive=D [mountpoint=/mnt/d]` | Laufwerk oder Freigabe mounten |
| `op=umount mountpoint=/mnt/d` | aushängen |
| `op=mounts` | aktuelle Mounts |
| `op=drives` | Windows-Laufwerksbuchstaben |
| `op=volumes` | Windows-Volumes |
| `op=cmd cmd="dir C:\"` | Befehl auf der Windows-cmd ausführen |

Variablen: `timeout` (Sekunden je Befehl)

### NFile

`nxrunner.system:NFile(type='nfile', search_limit=200)`, Tags `system`, `files`

Indexiert die Dateien von Laufwerken in der Tabelle `nfile` der nweb-Persistence. **Benötigt die nweb-Persistence
(Arg `DB`).**

- WSL: die Windows-Laufwerke unter `/mnt/` (mounten über den Wsl-Runner mit `op=update_drives`)
- Linux: die Dateisysteme aus `/proc/mounts` (ext4, btrfs, vfat, ntfs, nfs, cifs, ...), ohne `/boot`, `/snap` usw.

Der Name eines Laufwerks kommt aus `name=...` einer `nweb.ini` im Wurzelverzeichnis, sonst in der WSL aus dem
Windows-Laufwerksnamen und unter Linux aus dem Label bzw. dem Namen des Mountpoints. „Alle indexieren“
berücksichtigt nur Laufwerke mit `nweb.ini`. Andere Dateisysteme werden beim Indexieren nicht betreten
(wie `find -xdev`).

| Job | Beschreibung |
|---|---|
| `op=drives` | erkannte Laufwerke |
| `op=update_drives` | alle Windows-Laufwerke über den Wsl-Runner mounten (nur WSL) |
| `op=index [drive=name] [path=sub/dir] [exclude=a,b]` | ein oder alle Laufwerke indexieren (im Hintergrund); `path` nur mit `drive` |
| `op=status` | Fortschritt der Indexierung |
| `op=wipe drive=name` | Index eines Laufwerks löschen |
| `op=index_list` | Laufwerke im Index mit Anzahl Dateien |
| `op=search q=text [drive=name]` | Dateien suchen (höchstens `search_limit` Treffer) |
| `op=ls drive=name [path=sub/dir]` | Verzeichnisinhalt aus dem Index |

Variablen: `search_limit`

### Backup

`nxrunner.system:Backup(type='backup', check_interval=60)`, Tags `system`, `backup`

Backup-System aus Quellen und Zielen, gespeichert in der nweb-Persistence (Arg `DB`).

- **Quellen:** `path` (Verzeichnis mit Ausschlussmustern), `file` (einzelne Datei), `ssh` (Verzeichnis per
  SSH/SFTP, benötigt `paramiko`). Je Quelle optional `pre_script`/`post_script` (bei `ssh` auf dem entfernten
  Rechner).
- **Ziele:** `drive` (Laufwerksname wie in NFile + Pfad auf dem Medium), `drive_zip` (wie `drive`, aber als ZIP
  `<Quelle>.zip`, jeder Lauf schreibt sie neu, `keep` = Anzahl alter Versionen), `smb` (`\\host\share\pfad`,
  benötigt `smbprotocol`).

Eine Quelle kann mehrere Ziele haben und hat eine Häufigkeit (`interval_s`, `0` = nur manuell). Fällig ist jedes
Ziel einer Quelle für sich. Die Dateien landen unter `<Ziel>/<Name der Quelle>/<relativer Pfad>`. Gesichert wird
inkrementell: Größe und Änderungszeit jeder gesicherten Datei werden je Ziel gespeichert, unveränderte Dateien
übersprungen. Alle `check_interval` Sekunden wird geprüft, ob Quellen fällig sind.

| Job | Beschreibung |
|---|---|
| `op=sources` | Quellen |
| `op=source_save kind name path [exclude] [interval_s] [destination_ids=1,2] [source_id]` | Quelle anlegen bzw. mit `source_id` ändern |
| `op=source_delete source_id` | Quelle löschen |
| `op=destinations` | Ziele |
| `op=destination_save kind name [drive path host share username password] [destination_id]` | Ziel anlegen bzw. ändern |
| `op=destination_delete destination_id` | Ziel löschen |
| `op=run source_id [destination_id]` | Quelle jetzt sichern (alle bzw. ein Ziel) |
| `op=run_due` | alle fälligen Quellen/Ziele sichern (läuft auch automatisch) |
| `op=cancel` | laufendes Backup abbrechen, ausstehende Ziele werden nicht mehr gesichert |
| `op=status` | Status der Quellen und des laufenden Backups |
| `op=runs [source_id]` | Historie der Backups |
| `op=errors source_id` | Fehlerdetails der letzten Läufe (nur im Speicher, seit dem Start) |

### Smb

`nxrunner.fs:Smb()`, benötigt `smbprotocol`

Zugriff auf SMB-/Windows-Freigaben, z.B. als Backup-Ziel. Der Runner hat keine Jobs, sondern wird aus Python
benutzt. Pfade sind `/share/verzeichnis/datei`, vorher `connect(host, username, password)` aufrufen.

Methoden: `listdir(path)`, `get_contents(path)`, `put_contents(path, contents)`, `makedirs(path)`,
`copy_to_remote(src_path, dst_path, mtime=None)`

---

## Datenbanken

### MongoDB

`nxrunner.db.mongo:MongoDB(connection_url=None, args=None)`, benötigt `pymongo`

Ohne `connection_url` wird das Arg `mongo_url` benutzt (Standard `mongodb://localhost:27017/`). Ein Ticker führt
periodisch die Jobs aus der Collection `nweb.jobs` aus (Intervall Arg `mongo_job_cron`, Standard 623 s).

| Job | Beschreibung |
|---|---|
| `op=insert_one db=main collection=log row=...` | Dokument einfügen; statt `row` auch `row_<feld>=...` oder die übrigen Werte des Jobs |
| `op=find db collection [q={...}]` | Dokumente suchen |
| `op=jobs` | Jobs aus `nweb.jobs` jetzt ausführen |
| `op=collect_files path=/glob/* [db=nweb] [collection=files]` | Dateien zu `path` (Glob) als Dokumente speichern |
| `op=set_ticker value=600` | Intervall des Tickers in Sekunden |
| `op=create_ticker` | Ticker neu starten |

Variablen: `tick_count`

### DbUi

`nxrunner.db.nweb:DbUi(db, type='db')`, nicht registriert

Oberfläche für SQL-Abfragen auf einer nweb-Datenbank. `db` ist ein `DB`-Objekt, der Runner wird daher im Code
erzeugt.

| Job | Beschreibung |
|---|---|
| `op=sql sql="select ..."` | liefert die Zeilen als `rows` |

### NWebDir

`nxrunner.nweb:NWebDir(path, group=None, instance=None)`

Verzeichnis auf dem Server mit Auswahl einer nweb-Gruppe und eines ImageExecutors dieser Instanz. Ist eine Gruppe
festgelegt, zeigt die Dateiliste, ob es dort ein Dokument mit dem Dateinamen (ohne Endung) gibt. Dateien mit
`_id_<id>` im Namen werden über die id zugeordnet.

| Job | Beschreibung |
|---|---|
| `op=files` | Dateien im Verzeichnis (`name`, `size`, `mtime`) |
| `op=image_executors` | geladene ImageExecutoren des Dispatchers (`type`, `cls`) |
| `op=set_group group_id=...` | Gruppe festlegen, leer = keine Gruppe |
| `op=name_ids` | an Dateien mit passendem Dokument `_id_<id>` anhängen (`a.jpg` → `a_id_42.jpg`) |

Variablen: `file_count`

---

## Geräte und Hardware

### DashboardCYD

`nxrunner.devices:DashboardCYD(type='dashboard_cyd', host=None, items=None, file=None, interval=5000, db=False,
push_interval=10, keepalive=60, timeout=3, title=None, metrics=None)`, benötigt `requests`

Ein Dashboard (`nwebclient.runner:Dashboard`), das seine Werte zusätzlich an ein CYD (Cheap Yellow Display,
ESP32 + LVGL) sendet. Alle Einträge werden per `PUT /api/metrics` als Kacheln gesendet, in derselben Reihenfolge.
Gesendet wird alle `push_interval` Sekunden, wenn sich etwas geändert hat, sonst spätestens alle `keepalive`
Sekunden.

```
dashboard_cyd: nxrunner.devices:DashboardCYD(items=["pin1.state"])         -> sendet an dashboard_cyd.local
dashboard_cyd: nxrunner.devices:DashboardCYD(host="192.168.1.42", items=["pin1.state"])
dashboard_cyd: nxrunner.devices:DashboardCYD(db=True, title="Labor",
                   metrics={"pin1.state": {"unit": "°C", "min": 0, "max": 40, "color": "#4CAF50"}})
```

- `host`: IP oder Name des CYD, ohne Angabe `<type>.local`
- `metrics`: Optionen je Eintrag (Schlüssel ist das Label, z.B. `pin1.state`): `label`, `unit`, `color`, `min`,
  `max`, `decimals`, `wide`

Das CYD meldet sich nach dem WLAN-Connect per UDP-Broadcast `{"type": "dashboard_cyd", "op": "connect", "ip": ...}`.
Heißt der Runner wie dieser type und leitet ein UDP-Empfänger die Broadcasts an den Dispatcher weiter, wird die
IP automatisch übernommen.

| Job | Beschreibung |
|---|---|
| `op=push` | jetzt senden |
| `op=connect ip=...` (oder `host=...`) | Adresse setzen und senden |
| `op=info` | `/api/info` des Geräts |
| `op=add_var var_type var_name` | Variable hinzufügen (vom Dashboard geerbt) |
| `op=add_event event_type event_name [event_key]` | Event-Wert hinzufügen |
| `op=add_job job_type job_op [job_key]` | Job-Ergebnis hinzufügen |
| `op=run_item index` / `op=remove_item index` | Job-Eintrag ausführen / Eintrag entfernen |
| `op=get_values` | aktuelle Werte aller Einträge |

Variablen: `host`, `last_push`, `last_error`

### EspM5

`nxrunner.devices.esp_m5:EspM5(type='m5_multi', host=None, timeout=3)`, benötigt `requests`

M5StickS3 mit der Firmware [esp_m5_multi](../../esp_m5_multi/README.md). Es ist immer genau ein Modul aktiv
(Wage, IR Sniffer, Tail RS485, Joystick2, Button, AS5600). Die Seite zeigt die Werte des aktiven Moduls mit seinen
Aktionen, die Modulwahl und die Systemwerte (Akku, WLAN, Laufzeit). Ohne `host` wird `<type>.local` verwendet,
der Standard-Name der Firmware ist `m5_multi`.

```
m5_multi: nxrunner.devices.esp_m5:EspM5()
m5_multi: nxrunner.devices.esp_m5:EspM5(host="192.168.1.42")
```

| Job | Beschreibung |
|---|---|
| `op=get_status` | `/api/status`: System und unter `data` die Werte des aktiven Moduls |
| `op=get_rotation` | AS5600: `rpm`, `angle`, `revolutions`, `magnet`, ... |
| `op=get_weight` | Wage: `weight` in g |
| `op=get_joystick` | Joystick2: `x`, `y`, `button` |
| `op=get_log` | IR Sniffer / Tail RS485: empfangene Zeilen |
| `op=run_action a=... [text=...]` | Aktion des aktiven Moduls, z.B. `reset`, `tare`, `send` |
| `op=set_module id=...` | Modul wechseln (`scale`, `ir`, `rs485`, `joy`, `btn`, `as5600`), das Gerät startet neu |
| `op=restart_device` | Neustart |
| `op=i2c_scan` | I2C-Scan am Port A |
| `op=connect ip=...` | Adresse setzen |

Die `get_*`-Jobs für ein Modul schlagen fehl, wenn gerade ein anderes Modul aktiv ist.

Variablen: `host`

### NimbotB1

`nxrunner.devices.nimbot:NimbotB1(address=None, channel=1, width_mm=50, height_mm=30, density=3,
transport='auto', args={})`, benötigt `pillow`, `bleak`

Etikettendrucker Niimbot B1 per Bluetooth. Der Druckkopf ist 384 px (48 mm bei 203 dpi) breit, Bilder werden bei
Bedarf skaliert. Ein Etikett mit 50x30 mm entspricht etwa 384x240 px. `transport`: `auto` (RFCOMM, bei Fehler
BLE), `rfcomm` oder `ble`. Die Parameter lassen sich auch über die Args `nimbot_address`, `nimbot_channel`,
`nimbot_transport`, `nimbot_width_mm`, `nimbot_height_mm` und `nimbot_density` setzen.

| Job | Beschreibung |
|---|---|
| `op=print_text text=... [font_size] [label]` | Text drucken |
| `op=print_image file=/pfad.png [label]` | Bild aus einer Datei drucken |
| `op=print_image image=... [label]` | Bild als URL, Data-URL oder Base64 drucken |
| `op=info` | Akku, Seriennummer, Software- und Hardware-Version |

Druckoptionen für beide `print_*`: `[quantity=1] [density] [rotate=0]`. Dazu kommen die Bluetooth-Jobs der
[Basisklasse](#bluetoothdevice-basisklasse).

```python
from nxrunner.devices.nimbot import NimbotB1
p = NimbotB1(address='AA:BB:CC:DD:EE:FF')
p.print_text("Hallo Welt")
```

### BluetoothDevice (Basisklasse)

`nxrunner.devices.bluetooth:BluetoothDevice(type, address=None, channel=1, transport='auto')`, benötigt `bleak`

Basis für Runner, die ein Gerät per Bluetooth ansprechen: Verbindung per RFCOMM oder BLE, Suchen und Koppeln per
`bluetoothctl`, inklusive Oberfläche (`part_install`).

```python
class MyDevice(BluetoothDevice):
    BLE_CHARACTERISTIC = '...'

    def __init__(self, address=None):
        super().__init__('my_device', address)
```

| Job | Beschreibung |
|---|---|
| `op=set_address address=AA:BB:...` | Adresse des Geräts setzen |
| `op=bt_power` | Bluetooth einschalten |
| `op=bt_scan [duration=10]` | Geräte suchen |
| `op=bt_pair address=...` | koppeln |
| `op=bt_device_info address=...` | Infos zum Gerät |
| `op=bt_disconnect address=...` | Verbindung trennen |
| `op=bt_remove address=...` | Kopplung entfernen |

### JoystickM5

`nxrunner.hw.i2c:JoystickM5()`, type `joystick`, benötigt `smbus2`

M5Stack Joystick 2 (Unit, I2C-Adresse `0x63` an Bus 1): Achsen, Taster und RGB-LED.

| Job | Beschreibung |
|---|---|
| `op=read` | X/Y (8 und 16 Bit), Taster (0 = gedrückt), Firmware, Adresse |
| `op=rgb r g b` | Farbe der LED setzen |
| `op=setaction jobdef x_var` | alle 0,5 s einen Job mit dem X-Wert in `x_var` ausführen |

### ElrsDisplay

`nxrunner.hw.elrs:ElrsDisplay(args={})`

Zeigt die Kanalwerte (1-11) eines ExpressLRS-Empfängers an und löst über Bindings Jobs aus. Ein Binding verbindet
Kanal und Modus (`PWM_H`, `PWM_L`, `PWM_F`, `DIR_H`, `DIR_L`) mit dem Wert `value_name` in `job_data`. Bindings
lassen sich vorab über das Arg `elrs_bindings` festlegen.

| Job | Beschreibung |
|---|---|
| `op=fetch ip=...` | Kanalwerte einmal vom ESP lesen (`http://ip/val`) |
| `op=auto_fetch ip=...` | Kanalwerte periodisch lesen |
| `op=bind_esp_motor ip=... [channel=2]` | Kanal periodisch auf einen Motor des ESP übertragen |
| `op=binding channel mode value_name job_data` | Binding anlegen |
| `op=process_line value=channel1=...` | Kanalwerte aus einem Query-String setzen |
| `op=stop_thread thread_name=...` | Hintergrund-Thread beenden |
| `channel1=... channel2=...` (ohne `op`) | Kanalwerte direkt setzen |
| `page=stream` | Kanalwerte als Server-Sent-Events |

---

## Sonstige

### Convert (audio_convert)

`nxrunner.audio:Convert()`, benötigt `pydub` und ffmpeg

| Job | Beschreibung |
|---|---|
| `op=tomp3 infile=/pfad/in.wav outfile=/pfad/out.mp3` | WAV nach MP3 konvertieren |

### PiHoleApi

`nxrunner.apis:PiHoleApi(url='http://127.0.0.1:7101/', password=None)`, benötigt `pihole6api`

Zeigt die Statistik eines Pi-hole 6 auf der Seite des Runners an
([pihole6api](https://github.com/sbarbett/pihole6api)). Keine Jobs.

### Trello

`nxrunner.apis:Trello(api_key, token, args={})`, benötigt `py-trello`

Zeigt die Boards eines Trello-Kontos auf der Seite des Runners an. Keine Jobs.

---

## Infrastruktur

### NxRunner (Dispatcher)

`nxrunner.ui:NxRunner(**kwargs)`, gestartet mit `python -m nxrunner`

Der Dispatcher von nxrunner (ein `AutoDispatcher`) mit der Oberfläche `/pysys/ui` für Runner-Seiten und die
Registry.

| Job | Beschreibung |
|---|---|
| `op=registry [refresh]` | installierte Runner (Entry-Points `nweb_runner`) mit Status |
| `op=registry_load name [runner_type] [arg_<param>=...]` | Runner aus der Registry laden |
| `op=registry_install name [runner_type] [directory] [overwrite] [preview] [arg_<param>=...]` | Config `<directory>/<runner_type>.yml` schreiben und Runner laden |
| `op=registry_pip name` | fehlende `MODULES` per pip installieren |
| `op=registry_docker name [runner_type] [packages] [mounts] [ports] [arg_<param>=...]` | Runner in einem Docker-Container laden |

### DockerProxy

`nxrunner.dockerproxy:DockerProxy(type, entry, spec, packages, mounts=(), ports=(), ctor_args=None, start=True)`,
benötigt `docker`

Führt einen Runner in einem Docker-Container aus, z.B. wenn ihm lokal Module fehlen. Wird von
`op=registry_docker` bzw. „In Docker laden“ in der Registry erzeugt.

1. Image `nxrunner-<entry>` bauen, falls es fehlt: `python:3.12-slim` plus `pip install` der angegebenen Pakete
2. Container starten: Port 7070 des Containers auf einen freien Port von `127.0.0.1`, zusätzliche Ports und
   Mounts wie angegeben, Konstruktor-Parameter per Umgebungsvariable `NX_RUNNER_ARGS`
3. Alle Jobs, auch die Seiten der Oberfläche, per HTTP an den Container weiterleiten

Die Container laufen nur, solange nxrunner läuft.

| Job | Beschreibung |
|---|---|
| `op=docker_status` | Status des Containers |
| `op=docker_start` / `docker_stop` / `docker_restart` | Container steuern |
| `op=docker_rebuild` | Image neu bauen und starten |
| `op=docker_logs` | Log des Containers |
| alle anderen | gehen an den Runner im Container |
