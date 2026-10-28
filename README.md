
# NxRunner

System Automation made simple.

## Running

Der Runner wird über die `nweb.json` oder das `nx.d`-Verzeichnis konfiguriert. Die `nweb.json` wird in `.` oder `/etc` gesucht.

```
python -m nwebclient.runner --rest
```

Default-Port für das Webinterface ist `7070` (`--port 7070`)

## Runners/Features

 - Backup
 - nfile: Index Filesystems

[See more](docs/Runner.md)

## Eigene Runner erstellen

Ein Runner ist eine Klasse, die von `nwebclient.runner.BaseJobExecutor` erbt. Er bekommt Jobs als `dict`
(z.B. `{"type": "greeter", "op": "greet", "name": "Welt"}`) und liefert ein `dict` zurück.

### Registrieren

Der Runner wird als Entry-Point in der Gruppe `nweb_runner` eingetragen (`pyproject.toml`):

```toml
[project.entry-points.nweb_runner]
greeter = "myns.greeter:Greeter"
```

Über `/pysys/registry` kann er dann im Webinterface geladen werden. Dabei wird die Klasse **ohne Argumente**
erzeugt, alle Parameter des Konstruktors brauchen also Standardwerte. Alternativ in der Konfiguration (`runners`):

```
greeter: myns.greeter:Greeter(prefix="Hallo")
```

### Operationen mit `@op`

Methoden, die mit `@r.op(...)` markiert sind, werden automatisch als Operation angeboten und mit
`{"op": "<methodenname>", ...}` aufgerufen:

```python
from nwebclient import runner as r
from nwebclient import base as b


class Greeter(r.BaseJobExecutor):

    MODULES = []  # optionale pip-Abhaengigkeiten

    def __init__(self, type='greeter', prefix='Hallo'):
        super().__init__(type)   # registriert die @op-Methoden, daher immer aufrufen
        self.prefix = prefix
        self.count = 0
        self.define_vars('prefix', 'count')                  # lesbar/setzbar per getvar/setvar
        self.define_event('on_greet', {'name': 'Gegruesster Name'})

    @r.op("Begruesst name, optional mehrfach", result={'success': 'bool', 'message': 'str'})
    def greet(self, name: str = 'Welt', times: int = 1, shout: bool = False):
        text = ' '.join([f'{self.prefix} {name}'] * times)
        self.count += 1
        self.emit_event('on_greet', name=name)
        return self.success(text.upper() if shout else text)

    @r.op("Setzt den Zaehler zurueck", result={'success': 'bool', 'message': 'str'})
    def reset(self):
        self.count = 0
        return self.success('zurueckgesetzt')
```

Aufrufe:

```python
g = Greeter()
g({'op': 'greet', 'name': 'Anna', 'times': '2'})   # {'success': True, 'message': 'Hallo Anna Hallo Anna'}
g(op='greet', shout='true')                         # {'success': True, 'message': 'HALLO WELT'}
g(getvar='count')                                   # {'success': True, 'value': 2}
```

Über REST (Port 7070, GET oder POST):

```
curl "http://127.0.0.1:7070/?type=greeter&op=greet&name=Anna&times=2"
```

Was `@op` dabei übernimmt:

- **Parameter** werden aus dem Job gelesen, alles andere (z.B. `type`) wird ignoriert. Hat die Methode `**kwargs`,
  bekommt sie die übrigen Werte dort.
- **Typumwandlung** anhand der Annotation, denn über REST kommt alles als Text an:

  | Annotation | Eingabe | Ergebnis |
  |---|---|---|
  | `int`, `float` | `'5'`, `'2.5'` | `5`, `2.5` (ein `int` bleibt bei `float` ein `int`) |
  | `bool` | `'1'`, `'true'`, `'yes'`, `'on'` | `True`, alles andere `False` |
  | `list`, `dict` | `'[1, 2]'` | per JSON gelesen |
  | `str` / keine | | unverändert |

  Ist ein Wert nicht umwandelbar (`times='abc'`) oder fehlt ein Pflichtparameter, wird die Methode nicht aufgerufen
  und der Job liefert `{'success': False, 'message': ...}`.
- **Signatur**: Beschreibung, Parameter mit Typ und Standardwert sowie `result` erscheinen im Webinterface und in
  der Funktionsliste. Operationen ohne Pflichtparameter können z.B. im Dashboard als Job-Eintrag ausgewählt werden,
  `result` liefert dort die Auswahl der Schlüssel.

Hinweise:

- `name=` setzt einen anderen Namen der Operation, wenn der Methodenname schon belegt ist.
  `info`, `error`, `prn`, `success`, `fail`, `execute` usw. gehören zu `BaseJobExecutor`:
  ```python
  @r.op("Geraeteinfo", name='info')
  def device_info(self): ...
  ```
- Ein Standardwert `None` zählt für die Oberfläche als Pflichtparameter. Optionale Parameter, die die Operation
  im Dashboard auswählbar lassen sollen, bekommen daher `''` oder einen anderen Wert als Standard.
  Umgekehrt hält `None` Operationen mit Nebenwirkungen aus der Auswahl heraus.
- Operationen ohne Pflichtparameter, die im Dashboard ausgewählt werden, laufen bei jeder Aktualisierung.
  Dort nur Aufrufe ohne Nebenwirkungen eintragen.

### Ohne `@op`

Wird das komplette `data` gebraucht, kann weiterhin eine Methode `execute_<name>(self, data)` geschrieben werden.
Sie hat Vorrang vor einer gleichnamigen `@op`-Operation. Für Jobs ganz ohne `op` lässt sich `execute(self, data)`
überschreiben; unbekannte Jobs dann mit `return super().execute(data)` weitergeben, damit `op`, `getvar`, `setvar`
usw. funktionieren.

### Webinterface

Mit `part_index(self, p: b.Page, params={})` bekommt der Runner eine eigene Seite. `Page` ist eine Hilfsklasse zum
Erzeugen von HTML mit Methoden wie `h(str)`, `div(str)`, `a(content, href)` oder `ul(list)`. Buttons, die eine
Operation aufrufen, z.B. mit
`p(self.action_btn({'title': 'Zuruecksetzen', 'type': self.type, 'op': 'reset'}))`.
Mehr unter [https://gitlab.com/bsalgert/nwebclient/-/blob/master/docs/Page.md](https://gitlab.com/bsalgert/nwebclient/-/blob/master/docs/Page.md?ref_type=heads)

## See also
 - nwebclient
   - nwebclient docs/Typing.md (für define_sig(...)...)
 - nxml
