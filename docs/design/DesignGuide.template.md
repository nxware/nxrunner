# NxRunner Design Guide

Wie Runner-Seiten in NxRunner aussehen und wie man sie baut. Die Vorlage ist die NxRunner-Oberfläche
([ui.html](../../nxrunner/resources/ui.html)) und das nweb-Layout von [bsnx.net/4.0](https://bsnx.net/4.0/w/)
(SB-Admin, dunkle Leisten).

> Diese Datei wird erzeugt. Text bearbeiten in `DesignGuide.template.md`, danach
> `python docs/design/build.py` ausführen. Das erzeugt auch alle Screenshots in `img/` neu.

## Grundsätze

NxRunner sieht aus wie ein Schaltschrank, nicht wie eine App: dunkel, kantig, beschriftet. Weiche Karten,
runde Buttons und bunte Kacheln, wie sie die meisten Admin-Oberflächen haben, gibt es hier nicht.

- **Anthrazit, nicht Schwarz.** Die Seite steht auf Tiefblau-Schwarz `#12151b`, Karten heben sich mit `#1b2029`
  nur leicht ab, Abschnitte liegen wieder eine Stufe tiefer. Die Kopfleiste ist das Schiefergrau `#343a40`
  von bsnx.net und damit bewusst heller als die Seite: Sie ist der Deckel des Schranks. Links darin sind
  halbtransparentes Weiß und werden beim Darüberfahren heller.
- **Rechte Winkel, überall.** Keine abgerundete Ecke, auch nicht bei Buttons, Eingabefeldern oder Badges.
  Kein Schatten, kein Verlauf, keine Unschärfe. Tiefe entsteht nur durch Helligkeitsstufen und 1px-Linien
  in `#2a303c`.
- **Ein Blau, sonst Grau.** `#5b86ff` ist die einzige Farbe der Bedienung: Links, Hover-Kanten, die aktive Auswahl.
  Alles andere ist Grau zwischen `#8b93a5` (Nebeninfo) und `#e4e7ec` (Text). Wo Blau ist, kann man klicken.
- **Auswahl als voller Block.** Was gewählt ist, wird nicht unterstrichen oder mit einem Strich markiert, sondern
  komplett blau gefüllt, Schrift weiß. Man sieht aus dem Augenwinkel, wo man ist.
- **Status als Farbkante.** Meldungen tragen links eine kräftige 5px-Kante auf einer gedämpften Fläche:
  Blau `#009` Info, Grün `#090` Erfolg, Ocker `#a70` Warnung, Rot `#900` Fehler. Sie sind die einzigen hellen
  Flächen auf der Seite und deshalb nicht zu übersehen. Grün `#3ecf7a` und Rot `#ff6b6b` als Textfarbe gibt es
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

Kopfzeile `#343a40`, Seitenleiste `#212529`, Inhalt auf `#333`, Karten `#555`, Fußzeile `#e9ecef`.
Runner-Inhalte nutzen dieselben Elemente, nur die Farbvariablen sind andere.

![bsnx.net 4.0](img/layout_bsnx.png)

## Farben

| Variable | NxRunner | bsnx.net 4.0 | Verwendung |
|---|---|---|---|
| `--header-bg` | `#343a40` | `#343a40` | Kopfzeile |
| `--header-muted` | Weiß, 50 % | Weiß, 50 % | Links in der Kopfzeile |
| `--bg` | `#12151b` | `#333333` | Seite, Sections, Hover |
| `--panel` | `#1b2029` | `#555555` | Karten, Eingabefelder |
| `--text` | `#e4e7ec` | `#dfdfdf` | Text |
| `--muted` | `#8b93a5` | `#adb5bd` | Hinweise, Klassennamen |
| `--border` | `#2a303c` | `#6c757d` | Rahmen, Trenner |
| `--accent` | `#5b86ff` | `#007bff` | Links, aktive Auswahl |
| `--ok` | `#3ecf7a` | `#28a745` | Erfolg |
| `--err` | `#ff6b6b` | `#dc3545` | Fehler |
| `--code` | `#151920` | `#2b2b2b` | `pre`, JSON |

Meldungen (`alert_*`) haben feste Farben, weil sie mit Inline-Styles aus `nwebclient.web` kommen:
Info `#009`/`#77b`, Erfolg `#090`/`#7b7`, Warnung `#a70`/`#dc7`, Fehler `#900`/`#b77` (Rand/Fläche).

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

{{ELEMENTS}}
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
