# NxRunner Design Guide

Wie Runner-Seiten in NxRunner aussehen und wie man sie baut. Die Vorlage ist die NxRunner-Oberfläche
([ui.html](../../nxrunner/resources/ui.html)) und das nweb-Layout von [bsnx.net/4.0](https://bsnx.net/4.0/w/)
(SB-Admin, dunkle Leisten).

> Diese Datei wird erzeugt. Text bearbeiten in `DesignGuide.template.md`, danach
> `python docs/design/build.py` ausführen. Das erzeugt auch alle Screenshots in `img/` neu.

## Grundsätze

NxRunner sieht aus wie ein Schaltschrank, nicht wie eine App: dunkel, kantig, beschriftet. Weiche Karten,
runde Buttons und bunte Kacheln, wie sie die meisten Admin-Oberflächen haben, gibt es hier nicht.

- **Anthrazit, nicht Schwarz.** Die Seite steht auf Tiefblau-Schwarz <img src="img/colors/12151b.png" width="14" height="14" alt="#12151b"> `#12151b`, Karten heben sich mit <img src="img/colors/1b2029.png" width="14" height="14" alt="#1b2029"> `#1b2029`
  nur leicht ab, Abschnitte liegen wieder eine Stufe tiefer. Die Kopfleiste ist das Schiefergrau <img src="img/colors/343a40.png" width="14" height="14" alt="#343a40"> `#343a40`
  von bsnx.net und damit bewusst heller als die Seite: Sie ist der Deckel des Schranks. Links darin sind
  halbtransparentes Weiß und werden beim Darüberfahren heller.
- **Rechte Winkel, überall.** Keine abgerundete Ecke, auch nicht bei Buttons, Eingabefeldern oder Badges.
  Kein Schatten, kein Verlauf, keine Unschärfe. Tiefe entsteht nur durch Helligkeitsstufen und 1px-Linien
  in <img src="img/colors/2a303c.png" width="14" height="14" alt="#2a303c"> `#2a303c`.
- **Ein Blau, sonst Grau.** <img src="img/colors/5b86ff.png" width="14" height="14" alt="#5b86ff"> `#5b86ff` ist die einzige Farbe der Bedienung: Links, Hover-Kanten, die aktive Auswahl.
  Alles andere ist Grau zwischen <img src="img/colors/8b93a5.png" width="14" height="14" alt="#8b93a5"> `#8b93a5` (Nebeninfo) und <img src="img/colors/e4e7ec.png" width="14" height="14" alt="#e4e7ec"> `#e4e7ec` (Text). Wo Blau ist, kann man klicken.
- **Auswahl als voller Block.** Was gewählt ist, wird nicht unterstrichen oder mit einem Strich markiert, sondern
  komplett blau gefüllt, Schrift weiß. Man sieht aus dem Augenwinkel, wo man ist.
- **Status als Farbkante.** Meldungen tragen links eine kräftige 5px-Kante auf einer gedämpften Fläche:
  Blau <img src="img/colors/009.png" width="14" height="14" alt="#009"> `#009` Info, Grün <img src="img/colors/090.png" width="14" height="14" alt="#090"> `#090` Erfolg, Ocker <img src="img/colors/a70.png" width="14" height="14" alt="#a70"> `#a70` Warnung, Rot <img src="img/colors/900.png" width="14" height="14" alt="#900"> `#900` Fehler. Sie sind die einzigen hellen
  Flächen auf der Seite und deshalb nicht zu übersehen. Grün <img src="img/colors/3ecf7a.png" width="14" height="14" alt="#3ecf7a"> `#3ecf7a` und Rot <img src="img/colors/ff6b6b.png" width="14" height="14" alt="#ff6b6b"> `#ff6b6b` als Textfarbe gibt es
  nur für Zustände, nie zur Dekoration.
- **Umrandet statt gefüllt.** Badges und Buttons sind Rahmen mit Text, keine farbigen Pillen. Die Farbe steckt im
  Rahmen und in der Schrift, die Fläche bleibt dunkel. Gefüllt wird nur, was aktiv ist.
- **Beschriftet wie ein Gerät.** Kurze Wörter statt Sätze, Klassennamen und Pfade klein und grau unter dem Namen,
  Zwischentitel in Versalien mit Sperrung. Jeder Button sagt per Tooltip, was er auslöst.
- **Dicht gepackt.** Kleine Abstände, Tabellen statt Kacheln, kein Leerraum als Stilmittel. Eine Seite zeigt
  so viel wie möglich, ohne zu scrollen.
- **Feste Ordnung.** Oben die Leiste, links die Liste, rechts der Inhalt mit einer großen Überschrift und darunter
  Abschnitten. Das Menü des Runners steht immer am Ende als Reihe umrandeter Felder.

## Layout

### NxRunner

Oben eine feste Kopfzeile mit Titel, Statistik und Navigation. Darunter ein Raster aus der Runner-Liste
(280px) und dem Inhalt (`#content.card`). Unter 760px Breite stehen beide Spalten untereinander.

![NxRunner](img/layout_nxrunner.png)

### bsnx.net 4.0

Kopfzeile <img src="img/colors/343a40.png" width="14" height="14" alt="#343a40"> `#343a40`, Seitenleiste <img src="img/colors/212529.png" width="14" height="14" alt="#212529"> `#212529`, Inhalt auf <img src="img/colors/333.png" width="14" height="14" alt="#333"> `#333`, Karten <img src="img/colors/555.png" width="14" height="14" alt="#555"> `#555`, Fußzeile <img src="img/colors/e9ecef.png" width="14" height="14" alt="#e9ecef"> `#e9ecef`.
Runner-Inhalte nutzen dieselben Elemente, nur die Farbvariablen sind andere.

![bsnx.net 4.0](img/layout_bsnx.png)

## Farben

| Variable | NxRunner | bsnx.net 4.0 | Verwendung |
|---|---|---|---|
| `--header-bg` | <img src="img/colors/343a40.png" width="14" height="14" alt="#343a40"> `#343a40` | <img src="img/colors/343a40.png" width="14" height="14" alt="#343a40"> `#343a40` | Kopfzeile |
| `--header-muted` | Weiß, 50 % | Weiß, 50 % | Links in der Kopfzeile |
| `--bg` | <img src="img/colors/12151b.png" width="14" height="14" alt="#12151b"> `#12151b` | <img src="img/colors/333333.png" width="14" height="14" alt="#333333"> `#333333` | Seite, Sections, Hover |
| `--panel` | <img src="img/colors/1b2029.png" width="14" height="14" alt="#1b2029"> `#1b2029` | <img src="img/colors/555555.png" width="14" height="14" alt="#555555"> `#555555` | Karten, Eingabefelder |
| `--text` | <img src="img/colors/e4e7ec.png" width="14" height="14" alt="#e4e7ec"> `#e4e7ec` | <img src="img/colors/dfdfdf.png" width="14" height="14" alt="#dfdfdf"> `#dfdfdf` | Text |
| `--muted` | <img src="img/colors/8b93a5.png" width="14" height="14" alt="#8b93a5"> `#8b93a5` | <img src="img/colors/adb5bd.png" width="14" height="14" alt="#adb5bd"> `#adb5bd` | Hinweise, Klassennamen |
| `--border` | <img src="img/colors/2a303c.png" width="14" height="14" alt="#2a303c"> `#2a303c` | <img src="img/colors/6c757d.png" width="14" height="14" alt="#6c757d"> `#6c757d` | Rahmen, Trenner |
| `--accent` | <img src="img/colors/5b86ff.png" width="14" height="14" alt="#5b86ff"> `#5b86ff` | <img src="img/colors/007bff.png" width="14" height="14" alt="#007bff"> `#007bff` | Links, aktive Auswahl |
| `--ok` | <img src="img/colors/3ecf7a.png" width="14" height="14" alt="#3ecf7a"> `#3ecf7a` | <img src="img/colors/28a745.png" width="14" height="14" alt="#28a745"> `#28a745` | Erfolg |
| `--err` | <img src="img/colors/ff6b6b.png" width="14" height="14" alt="#ff6b6b"> `#ff6b6b` | <img src="img/colors/dc3545.png" width="14" height="14" alt="#dc3545"> `#dc3545` | Fehler |
| `--code` | <img src="img/colors/151920.png" width="14" height="14" alt="#151920"> `#151920` | <img src="img/colors/2b2b2b.png" width="14" height="14" alt="#2b2b2b"> `#2b2b2b` | `pre`, JSON |

Meldungen (`alert_*`) haben feste Farben, weil sie mit Inline-Styles aus `nwebclient.web` kommen:
Info <img src="img/colors/009.png" width="14" height="14" alt="#009"> `#009`/<img src="img/colors/77b.png" width="14" height="14" alt="#77b"> `#77b`, Erfolg <img src="img/colors/090.png" width="14" height="14" alt="#090"> `#090`/<img src="img/colors/7b7.png" width="14" height="14" alt="#7b7"> `#7b7`, Warnung <img src="img/colors/a70.png" width="14" height="14" alt="#a70"> `#a70`/<img src="img/colors/dc7.png" width="14" height="14" alt="#dc7"> `#dc7`, Fehler <img src="img/colors/900.png" width="14" height="14" alt="#900"> `#900`/<img src="img/colors/b77.png" width="14" height="14" alt="#b77"> `#b77` (Rand/Fläche).

## Abstände

| Wert | Verwendung |
|---|---|
| 0 | Sections seitlich: bündig mit Überschrift und Meldungen |
| 4px | Formularzeilen, Meldungen innen |
| 8px | Sections innen und untereinander, Abstand zwischen Badges/Buttons |
| 12px | Abstand unter Überschriften, Navigation |
| 16px | Karten innen, Kopfzeile seitlich |
| 20px | Raster der Hauptseite |

## Elemente

Jedes Beispiel unten ist genau der Code, mit dem der Screenshot darunter gerendert wurde
(Methode `part_*(self, p: b.Page, params={})` im Runner, `from nwebclient import base as b, web as w`).

### Typografie

```python
p.h1("Backup")
p.h2("Quellen")
p.p("Fließtext in 14px system-ui, Zeilenhöhe 1.5. Hinweise werden mit "
    "<span class='muted'>.muted</span> abgeschwächt.")
p.h3("Unterüberschrift")
```

![Typografie](img/typography.png)

### p.section

```python
with p.section(h="Fortschritt"):
    p.div("Inhalt eines Abschnitts")
    p.prop("Status", "läuft")
with p.section(h="Ziele"):
    p.div("Abschnitte gliedern jede Runner-Seite.")
```

![p.section](img/section.png)

### p.expandable

```python
with p.expandable("Erweitert", expanded=True):
    p.div("Aufgeklappter Inhalt, ohne JavaScript (&lt;details&gt;)")
with p.expandable("Protokoll", expanded=False):
    p.div("verborgen")
```

![p.expandable](img/expandable.png)

### p.prop

```python
p.prop("Version", "1.4.2")
p.prop("Laufzeit", "3 h 12 min")
p.prop("Ziele", ["nas", "s3"])
```

![p.prop](img/prop.png)

### Formulare

```python
p('<form method="post">')
p.form_input('name', "Name", hint="Anzeigename der Quelle", value="Fotos")
p.form_select('kind', "Art", {'dir': "Verzeichnis", 'mysql': "MySQL"}, value='dir',
              hint="Art der Quelle")
p.form_input('interval_s', "Intervall (s)", hint="0 = nur manuell", type='number', value=3600)
p.form_elem(w.tag("input", "", type="submit", value="Speichern", title="Quelle speichern"))
p('</form>')
```

![Formulare](img/form.png)

### Buttons & Links

```python
p.div(w.button_js("Jetzt sichern", "", hint="Startet die Sicherung sofort") + ' '
      + w.button_js("Prüfen", "", hint="Ziel auf Erreichbarkeit prüfen") + ' '
      + w.a("Fällige Sicherungen", "#", hint="Liste der fälligen Sicherungen"))
```

![Buttons & Links](img/buttons.png)

### Meldungen

```python
p.alert_info("Info: Scheduler prüft alle 60 s")
p.alert_success("Sicherung erfolgreich abgeschlossen")
p.alert_warn("Ziel fast voll (92 %)")
p.alert_error("Ziel nicht erreichbar")
```

![Meldungen](img/alerts.png)

### p.grid

```python
rows = [{'name': "Fotos", 'kind': "Verzeichnis", 'size': "12,4 GB"},
        {'name': "Wiki", 'kind': "MySQL", 'size': "380 MB"},
        {'name': "Mail", 'kind': "IMAP", 'size': "2,1 GB"}]
p.grid(rows, [w.Grid.col('name', "Name"), w.Grid.col('kind', "Art"), w.Grid.col('size', "Größe")],
       {}, search=True)
```

![p.grid](img/grid.png)

### p.pre

```python
p.pre({'success': True, 'value': ["Fotos", "Wiki"], 'duration': 1.42})
```

![p.pre](img/pre.png)

### Listen & Trenner

```python
p.ul([w.a("Quellen", "#", hint="Alle Quellen"), w.a("Ziele", "#", hint="Alle Ziele"), "Protokoll"])
p.hr()
p.right(w.a("Weiter &raquo;", "#", hint="Nächste Seite"))
```

![Listen & Trenner](img/list.png)

### Badges & Chips

```python
p.div('<span class="badge">laden</span> <span class="badge tag">tag</span> '
      '<span class="badge ok">ok</span> <span class="badge warn">fehler</span>')
p.div('<span class="chip active">alle</span> <span class="chip">system</span> '
      '<span class="chip">devices</span>', style='margin-top: 8px')
```

![Badges & Chips](img/badges.png)

### Runner-Menü (nx_menu)

```python
p.div("Inhalt des Runners")
p('<ul class="nx_menu">' + ''.join(f'<li><a href="#" title="{t}">{t}</a></li>'
                                   for t in ["StdOut", "Events", "Vars", "Help", "Run", "Sigs"]) + '</ul>')
```

![Runner-Menü (nx_menu)](img/menu.png)


## Seitenaufbau eines Runners

```python
def part_index(self, p: b.Page, params={}):
    p.h1("Backup")
    p.div(w.a("Fällige Sicherungen", self.link_due(), hint="Liste der fälligen Sicherungen"))
    with p.section(h="Quellen"):
        p.grid(sources, cols, params, empty="Noch keine Quellen.", prefix='s_')
    with p.section(h="Neue Quelle"):
        p.form_input('name', "Name", hint="Anzeigename")
        p.form_elem(w.tag("input", "", type="submit", value="Anlegen", title="Quelle anlegen"))
```

- Genau ein `p.h1` pro Seite, Gliederung mit `p.section(h=...)`. Selten Gebrauchtes in `p.expandable(..., expanded=False)`.
- Tabellen immer mit `p.grid`, damit Sortierung, Suche und Seiten überall gleich funktionieren.
- Einzelwerte mit `p.prop`, Rohdaten mit `p.pre` (dicts werden als JSON formatiert).
- Rückmeldungen mit `p.alert_*`, nicht mit eigenen farbigen `div`s.
- Nutzereingaben in HTML mit `w.s(...)` maskieren.
- Das Runner-Menü (`nx_menu`) hängt NxRunner selbst an. Der Runner rendert es nicht.
- Methoden kurz halten (max. 50 Zeilen). Große Seiten in mehrere `part_*`-Hilfsmethoden aufteilen.
