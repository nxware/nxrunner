# Meshtastic

`nxrunner.devices.meshtastic:MeshtasticBot` (type `mb`) verbindet einen nxrunner mit einem Meshtastic-Knoten
(LoRa-Mesh) und kann darüber verschlüsselte Jobs an andere nxrunner schicken.

## Einrichtung

Entry-Point `mb` (Registry im Webinterface) oder in der Runner-Konfiguration:

```
runners:
  crypt: nxrunner.system.crypt:CryptRunner(key_file="rr.txt")
  mb: nxrunner.devices.meshtastic:MeshtasticBot(port="/dev/ttyACM0")
  # oder: mb: nxrunner.devices.meshtastic:MeshtasticBot(host="192.168.178.50")
```

Abhängigkeiten: `meshtastic`, `cryptography`, für Bilder `Pillow`; für die TCP-Suche zusätzlich `zeroconf`.

Alle Knoten, die sich gegenseitig Jobs schicken sollen, brauchen **dieselbe Schlüsseldatei** des CryptRunners.

| Parameter | Standard | Bedeutung |
|---|---|---|
| `port` | – | serieller Port, z. B. `/dev/ttyACM0` oder `COM5` |
| `host` | – | IP/Hostname eines Knotens mit WLAN (TCP-Port 4403) |
| `max_fragments` | 16 | maximale Pakete je Nachricht (Job oder Ergebnis) |
| `max_age` | 600 | Sekunden, ältere Nachrichten werden verworfen, `None` = keine Prüfung |
| `fragment_delay` | 0.5 | Sekunden Pause zwischen den Paketen einer Nachricht (auch bei Bildern) |
| `image_quality` | 30 | JPEG-Qualität für Bilder (1–95) |
| `max_image_fragments` | 200 | maximale Pakete je Bild, sonst wird die Qualität in 5er-Schritten bis 10 gesenkt |
| `image_resend_timeout` | 20 | Sekunden ohne neues Bildpaket, danach prüft der Empfänger, ob er nachfordert |
| `image_min_complete` | 0.9 | Anteil des lückenlosen Anfangs, ab dem nichts nachgefordert wird |
| `image_max_rounds` | 3 | maximale Nachforderungen je Bild (Empfänger) bzw. je Bild und Anfrager (Sender) |

> Der Typ hieß früher `meshtastic`. Bestehende Konfigurationen und Jobs mit `type: meshtastic` auf `mb` umstellen.

## Weboberfläche

| Seite | Inhalt |
|---|---|
| Übersicht | Verbindungsstatus, Textnachricht senden |
| Messages | die letzten 25 empfangenen Textnachrichten (Port 1) |
| Jobs | Job an einen Knoten senden, gesendete Jobs mit Ergebnissen, empfangene Jobs |
| Bilder | Bild hochladen und senden, empfangene Bilder mit Vorschau während der Übertragung |
| Devices | Meshtastic-Geräte an seriellen Ports, per mDNS im Netz und per Bluetooth LE |
| GPIO | Remote Hardware (in Arbeit) |

## Jobs

| Job | Wirkung |
|---|---|
| `{type: mb, message: "Hallo"}` | Textnachricht senden |
| `{type: mb, op: send, dest: "!a1b2c3d4", job: {...}}` | Job verschlüsselt an einen Knoten senden, `dest: "^all"` an alle |
| `{type: mb, op: d, ref: <id>, r: {...}}` | Ergebnis eines entfernt ausgeführten Jobs (kommt über das Mesh zurück) |
| `{type: mb, op: send_image, data: "<Base64>", dest: "!a1b2c3d4", quality: 30}` | Bild senden (Port 256, Kennung `nI`), `data` auch als Data-URL |
| `{type: mb, connect_serial: "/dev/ttyACM0"}` | verbinden, ebenso `connect_tcp`, `connect_ble` |

`op: send` liefert `id` (Nachrichten-ID), `fragments` (Anzahl Pakete) und `size` (Byte verschlüsselt). Das
Senden läuft im Hintergrund, das Ergebnis erscheint auf der Seite *Jobs* und als Event `on_result`
(`ref`, `src`, `r`).

## Port 256 und Kennungen

Jobs und Bilder laufen auf Port **256** (`PRIVATE_APP`), den Meshtastic für private Anwendungen vorsieht.
Eigene Portnummern wie 370 wären zwar im privaten Bereich 256–511 erlaubt, aber die Meshtastic-Android-App
kennt nur die Ports aus ihrem Protobuf-Paket: sie würde eine unbekannte Nummer als Port 0 senden und lesen.
Damit Jobs, Bilder und fremde private Apps auf Port 256 auseinanderzuhalten sind, beginnt jedes Paket mit
einer **2-Byte-Kennung**:

| Kennung | Bytes | Inhalt |
|---|---|---|
| `nJ` | `6e 4a` | verschlüsselter Job oder Job-Ergebnis |
| `nI` | `6e 49` | Bildpaket oder Nachforderung |

Pakete ohne eine dieser Kennungen werden ignoriert. Nutzbar sind je Paket **227 Byte**: Meshtastic erlaubt
233 Byte für das ganze Data-Protobuf, davon gehen Portnummer und Längenfeld ab.

## Jobs (Kennung `nJ`)

### Senden

```
Job-JSON (kompakt)
  -> raw deflate (zlib ohne Header/Prüfsumme)
  -> CryptRunner op=encrypt_file (Fernet: AES-128-CBC + HMAC-SHA256, mit Zeitstempel)
  -> Fernet-Token als Binärdaten (Base64 wird für die Funkstrecke entfernt)
  -> in Pakete zu je max. 220 Byte Nutzdaten zerlegt, jedes mit Kennung und 5 Byte Header
```

Paket (max. 227 Byte):

| Byte | Feld | |
|---|---|---|
| 0–1 | Kennung | `nJ` |
| 2 | Version | `1` = raw deflate + Fernet |
| 3–4 | Nachrichten-ID | zufällig, little endian |
| 5 | Index | 0 … Anzahl−1 |
| 6 | Anzahl | Pakete dieser Nachricht |
| 7… | Daten | Ausschnitt des Fernet-Tokens |

### Empfangen

1. Pakete werden je (Absender, Nachrichten-ID) gesammelt; unvollständige Nachrichten verfallen nach 300 s.
2. Ist die Nachricht vollständig, wird sie über den CryptRunner (`op=decrypt_file`) entschlüsselt.
   Falscher Schlüssel oder veränderte Daten: die Nachricht wird verworfen und **nicht beantwortet**.
3. Ist der Fernet-Zeitstempel älter als `max_age`, oder kam dieselbe Nachricht schon einmal an
   (Wiederholungsangriff), wird sie verworfen.
4. Entpacken; ist das Ergebnis kein gültiges JSON-Objekt, wird die Nachricht verworfen.
5. Der Job wird auf dem Root-Dispatcher ausgeführt (wie beim CryptRunner), in einem eigenen Thread.
6. Das Ergebnis geht auf demselben Weg als Job `{type: mb, op: d, ref: <Nachrichten-ID>, r: <Ergebnis>}`
   an den Absender zurück. Passt es nicht in `max_fragments` Pakete, kommt stattdessen eine Fehlermeldung.
7. Jobs `type: mb, op: d` werden beim Empfänger in die Liste der gesendeten Jobs eingetragen und nicht
   beantwortet (keine Endlosschleife). Bei `dest: "^all"` sammelt ein Job die Antworten mehrerer Knoten.

### Größen

Fernet fügt etwa 57 Byte hinzu (Version, Zeitstempel, IV, HMAC) und füllt auf 16-Byte-Blöcke auf.

| Job | JSON | deflate | verschlüsselt | Pakete |
|---|---|---|---|---|
| `{"type":"ping"}` | 15 | 17 | 89 | 1 |
| `{"type":"gpio","op":"set","pin":17,"value":1}` | 45 | 43 | 105 | 1 |
| typisches Ergebnis `op: d` mit kleinem `r` | 83 | 75 | 137 | 1 |

Bis 159 Byte komprimiertes JSON passt eine Nachricht in ein Paket. Längere Jobs und Ergebnisse werden
zerlegt; mit `max_fragments=16` sind bis knapp 3,5 KB verschlüsselt möglich.

## Bilder (Kennung `nI`)

Dasselbe Format beherrscht die Meshtastic-Android-App im Branch `feat/image-transfer` (Chat → Ordner-Symbol
*Images* in der Kopfleiste); Bilder lassen sich also zwischen nxrunner und der App in beide Richtungen schicken. Die
Android-App erzeugt allerdings eine *normale* (baseline) JPEG, weil Android keine progressive JPEG schreiben
kann: deren Vorschau zeigt während der Übertragung die oberen Zeilen statt eines unscharfen Gesamtbilds.

### Senden

1. Bild hochladen: auf der Seite *Bilder* in die Dropzone ziehen (Formular wie `ImageExecutor.ui_imageform`)
   oder über die Dateiauswahl (auf dem Handy: Galerie oder Kamera). Beides füllt das versteckte Feld
   `image_data` mit Base64, *Senden* ruft `op: send_image` auf.
2. Das Bild wird nach EXIF-Ausrichtung gedreht, auf **600 px längste Kante** verkleinert und als
   **progressive JPEG** mit Qualität `image_quality` (Standard 30) und optimierten Huffman-Tabellen gespeichert.
   EXIF und andere Metadaten (z. B. GPS-Position) werden nicht übernommen.
3. Würde das Bild mehr als `max_image_fragments` Pakete brauchen, wird die Qualität schrittweise gesenkt.
4. Die JPEG wird **der Reihe nach** in Pakete zerlegt und im Hintergrund gesendet (Pause `fragment_delay`).

Paket (max. 227 Byte):

| Byte | Feld | |
|---|---|---|
| 0–1 | Kennung | `nI` |
| 2 | Version | `1` = JPEG, unverschlüsselt |
| 3–4 | Bild-ID | zufällig, little endian |
| 5–6 | Index | 0 … Anzahl−1 |
| 7–8 | Anzahl | Pakete dieses Bildes |
| 9… | Daten | bis 218 Byte der JPEG |

### Empfangen

Pakete werden je (Absender, Bild-ID) gesammelt. Weil eine progressive JPEG zuerst eine grobe Fassung des
ganzen Bildes enthält und mit jedem Scan schärfer wird, zeigt die Seite *Bilder* schon nach wenigen Paketen
eine unscharfe Vorschau; sie wird alle 10 s neu geladen, solange eine Übertragung läuft. Nutzbar ist nur der
**lückenlose Anfang**: fehlt ein Paket, bleibt die Vorschau auf dem Stand davor. Ist das Bild vollständig,
kommt das Event `on_image` (`src`, `id`, `bytes`).

### Nachforderung verlorener Pakete

Kommt `image_resend_timeout` Sekunden lang kein Paket eines unvollständigen Bildes, bewertet der Empfänger
den **lückenlosen Anfang** (nur der ist für die progressive JPEG nutzbar):

- mindestens `image_min_complete` (Standard 90 %): das Bild gilt als *ausreichend*, es wird nichts
  nachgefordert (z. B. wenn nur die letzten, feinsten Details fehlen);
- weniger: der Empfänger schickt dem Sender eine Nachforderung mit allen fehlenden Paketen, der Sender
  schickt genau diese erneut, nur an den Anfrager. Danach beginnt die Wartezeit von vorn;
- nach `image_max_rounds` Nachforderungen ohne Erfolg gilt das Bild als *unvollständig*.

Nachforderung (max. 227 Byte):

| Byte | Feld | |
|---|---|---|
| 0–1 | Kennung | `nI` |
| 2 | Typ | `2` = Nachforderung |
| 3–4 | Bild-ID | |
| 5… | Bereiche | je 4 Byte: erstes fehlendes Paket, Anzahl (little endian), bis zu 55 Bereiche |

Der Sender beantwortet je Bild und Anfrager höchstens `image_max_rounds` Nachforderungen und nur, solange
er das Bild noch im Speicher hat (die letzten 10). Bei `dest: "^all"` fordert jeder Empfänger für sich nach.
Gehen **alle** Pakete eines Bildes verloren, weiß der Empfänger nichts davon und kann nichts nachfordern.

### Größen und Dauer

Gemessen bei 600 × 450 px und Qualität 30:

| Bild | Byte | Pakete |
|---|---|---|
| glattes Hintergrundbild (Windows-Wallpaper) | 3 306 | 16 |
| Verlauf mit starkem Rauschen (Testbild) | 4 398 | 21 |

Detailreiche Fotos dürften deutlich größer werden (nicht gemessen). Bei Qualität 50 verdoppelt sich die
Größe ungefähr (Testbild: 8 917 Byte, 41 Pakete). Mit `fragment_delay=0.5` dauern 21 Pakete mindestens 10 s,
dazu kommt die Sendezeit des LoRa-Presets.

Bilder werden **nicht verschlüsselt** (nur die Kanalverschlüsselung von Meshtastic greift), damit jedes
Paket für sich verwertbar bleibt. Gesendete und empfangene Bilder liegen nur im Speicher (die letzten 10).

## Sicherheit

- Wer die Schlüsseldatei besitzt, kann auf jedem Knoten **beliebige Jobs** ausführen. Die Datei wie ein
  Passwort behandeln und nur auf vertrauenswürdige Knoten verteilen.
- Die Kanalverschlüsselung von Meshtastic kommt zusätzlich hinzu; der Schutz der Jobs hängt aber nur am
  Fernet-Schlüssel.
- Die Uhren der Knoten müssen ungefähr stimmen (`max_age`), sonst werden Jobs als zu alt verworfen.
- Paket-Header (ID, Index, Anzahl) sind unverschlüsselt; Manipulationen fallen spätestens beim HMAC auf.

## Grenzen

- LoRa ist langsam: je nach Preset dauert ein Paket mit 233 Byte etwa 0,1 bis über 1 s Sendezeit, dazu
  kommen Weiterleitungen im Mesh. Große Jobs und Ergebnisse vermeiden; Duty-Cycle-Grenzen (EU 868 MHz)
  beachten.
- Job-Pakete werden ohne Bestätigung und ohne Nachforderung gesendet (anders als Bilder). Fehlt
  ein Paket, verfällt die Nachricht beim Empfänger, und
  der Sender bekommt keine Antwort (auf der Seite *Jobs* bleibt der Job auf „wartet“).
- Gesendete und empfangene Jobs liegen nur im Speicher (die letzten 25).

## Tests

```bash
python -m unittest tests.test_meshtastic
```

Die Tests verbinden zwei Bots über eine simulierte Funkstrecke (ohne Hardware): Hin- und Rückweg, Zerlegung
langer Jobs, zu große Nachrichten, falscher Schlüssel, Wiederholung, zu alte Nachrichten und ungültiges JSON;
für Bilder Verkleinerung, progressive JPEG ohne EXIF, Senken der Qualität, vollständige Übertragung und
Vorschau aus einem Teil der Pakete, Nachforderung verlorener Pakete, kleiner Verlust ohne Nachforderung
und Abbruch nach der letzten Runde.
