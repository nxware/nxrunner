# Basisoperationen der Runner

Operationen, die jeder Runner über die Basisklasse `nwebclient.runner:BaseJobExecutor` hat, und die
Operationen des Dispatchers `nwebclient.runner:LazyDispatcher`. Die Runner-spezifischen Jobs stehen in [Runner.md](Runner.md).

Schreibweise wie in [Runner.md](Runner.md): `op=name param=wert` entspricht
`{"type": "<runner>", "op": "name", "param": "wert"}`, Parameter in `[...]` sind optional.

```
curl "http://127.0.0.1:7070/?type=wsl&op=get_signatures"
curl "http://127.0.0.1:7070/?type=wsl&getvar=timeout"
```

---

## BaseJobExecutor

Quelle: `nwebclient/runner/__init__.py`, Signaturen in `nwebclient/runner/signatures.py` (`SignatureMixin`).

### Jobs

`BaseJobExecutor.execute(data)` prüft die Schlüssel in dieser Reihenfolge, der erste Treffer gewinnt:

| Job | Beschreibung | Ergebnis |
|---|---|---|
| `setvar=name value=...` | Variable setzen, nur wenn `name` in `var_names` steht (siehe [Variablen](#variablen)) | `success`, `ui_reload=true` |
| `getvar=name` | Variable lesen (auch Attribute, die nicht in `var_names` stehen) | `value` |
| `pipe_event event_type new_type [extra={...}]` | Event des Runners als Job mit `type=new_type` an den Dispatcher weiterleiten | `success` |
| `__str__` | `str(runner)` | `value` |
| `__repr__` | `repr(runner)` | `value` |
| `op=name ...` | Operation ausführen (siehe [Operationen](#operationen-op)) | je Operation |
| `stdout` | letzte Ausgaben des Runners (`prn`, `fail`; höchstens 15) | `value` |
| `page=name ...` | Teilseite `part_<name>` als HTML (für die UI v2) | `content`, `intern_links` |
| sonst | Fehler `Unknown Operation (BaseJobExecutor)` | `request_keys` |

### Operationen (`op`)

Operationen, die jeder Runner hat:

| Job | Beschreibung | Ergebnis |
|---|---|---|
| `op=get_signatures` | Signaturen des Runners als JSON (ohne den vom Dispatcher angehängten Parameter `type`) | `signatures` |
| `op=get_vars` | alle Variablen (`var_names`) mit aktuellem Wert, nicht JSON-fähige Werte als Text | `vars` |

### Variablen

- **Lesen** (`getvar=name`): `get_<name>()`, wenn vorhanden, sonst das Attribut `name` (`''`, wenn es fehlt).
  `getvar` funktioniert auch für Attribute, die nicht in `var_names` stehen.
- **Setzen** (`setvar=name value=...`): `set_<name>(value)`, wenn vorhanden. Gibt es nur einen `@r.var`-Getter,
  ist die Variable nur lesbar (Fehler). Sonst wird das Event `change_<name>` mit `old` und `new` ausgelöst und
  das Attribut gesetzt. `setvar` für einen Namen außerhalb von `var_names` wird nicht als Variable behandelt.


### Ergebnisse

| Methode | Ergebnis |
|---|---|
| `success(msg='ok', **kwargs)` | `{'success': True, 'message': msg, ...}` |
| `fail(msg='', **kwargs)` | `{'success': False, 'message': msg, ...}`, zusätzlich in `stdout` |

Bekannte Zusatzfelder für die Oberfläche: `ui_reload=True` (Seite neu laden), `ui={'toast': ..., 'prepend': ...,
'clear_content': True}`, `message` (wird als Hinweis angezeigt).

### Jobs

Jobs werden anhand von `data['type']` an spezifische Runner verteilt.

`LazyDispatcher.execute(data)`:

| Job | Beschreibung |
|---|---|
| `"?type=x&op=y"` (String) | wird als Query-String gelesen |
| ohne `type` | geht an `BaseJobExecutor.execute` des Dispatchers selbst (z.B. `op=get_signatures`) |
| `type=<runner> op=get_signatures` / `op=get_vars` | wird immer direkt an `execute_<op>` des Runners gegeben |
| `type=list_runners` | per Spec registrierte Runner, `names` |
| `type=load_runners runners={type: spec}` | Runner nachladen, wie [`load_dict`](#runner-registrieren) |
| unbekannter `type` | Job zurück mit `success=false`, `error_code=599` (`ERROR_UNKNOWN_JOB_TYPE`), `message` |

Schlägt das Erzeugen fehl, steht der Fehler in `entry.error` (Seite „Dispatcher“ und `runner_info`), die Exception
geht an den Aufrufer und der nächste Zugriff versucht es erneut.


# See also

 - nx docs/Runner.md