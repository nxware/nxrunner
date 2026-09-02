"""
Berechnung eines ImageTransform aus georeferenzierten Bildpunkten
(z. B. um ein Bild anschliessend in Web-Mercator-Kacheln (256x256px,
Standard-Slippy-Map-Schema wie OSM/Google Maps) zu zerlegen und als
TileLayer in einer Map-Anwendung zu nutzen).

Wichtig: Um eine Skalierung / ein Zoom-Level zu bestimmen, werden
mindestens 2 Referenzpunkte benoetigt. Mit nur einem Punkt kann nur
die Verschiebung (Translation) berechnet werden - dafuer muss dann
das gewuenschte zoom_level explizit uebergeben werden.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple, Union
import math
import os

from PIL import Image as PILImage

PILImage.MAX_IMAGE_PIXELS = None

TILE_SIZE = 256  # Standardgroesse einer Kachel in Pixeln (Slippy-Map-Schema)


# ---------------------------------------------------------------------------
# Datenmodelle
# ---------------------------------------------------------------------------

@dataclass
class LatLng:
    lat: float
    lng: float


@dataclass
class ImageRef:
    x: int
    y: int
    lat: float
    lng: float


@dataclass
class ImageRefs:
    width: int
    height: int
    refs: List[ImageRef]


@dataclass
class ImageTransform:
    zoom_level: int
    offset_x: int   # Verschiebung der Bild-Ecke (0,0) innerhalb der globalen
                     # Kachel bei zoom_level, Wertebereich 0..TILE_SIZE-1
    offset_y: int
    topleft: LatLng  # Geokoordinate von Bildpixel (0,0)
    scale: float     # Faktor, um den das Originalbild reskaliert werden muss,
                      # damit 1 Bildpixel exakt 1 Weltpixel bei zoom_level
                      # entspricht (siehe apply_offset). 1.0 = keine Aenderung.

    def apply_offset(self, img: Union[PILImage.Image, str], mode="RGB") -> PILImage.Image:
        """
        Bereitet das Bild fuer die Kachelung bei zoom_level vor:

        1. Reskaliert das Bild um 'scale', damit ein Bildpixel exakt einem
           Weltpixel des gewaehlten Zoom-Levels entspricht (ohne diesen
           Schritt waeren die spaeter geschnittenen Kacheln leicht falsch
           positioniert bzw. gestaucht/gestreckt).
        2. Richtet das reskalierte Bild am globalen Kachelraster aus, indem
           es links/oben um (offset_x, offset_y) transparente Pixel
           erweitert wird - danach beginnt die obere linke Ecke exakt auf
           einer Kachelgrenze (Vielfaches von TILE_SIZE).
        3. Fuellt rechts/unten so weit auf, dass Breite und Hoehe ein
           Vielfaches von TILE_SIZE sind, damit sich das Ergebnis
           anschliessend einfach in 256x256px-Kacheln zerschneiden laesst.

        img: entweder ein bereits geladenes PIL Image oder ein Dateipfad.
        return: neues PIL Image (RGBA) mit angewendeter Skalierung/Offset.
        """
        if isinstance(img, str):
            img = PILImage.open(img)

        img = img.convert(mode)

        if self.scale != 1.0:
            new_width = max(1, round(img.width * self.scale))
            new_height = max(1, round(img.height * self.scale))
            img = img.resize((new_width, new_height), PILImage.LANCZOS)

        width, height = img.size
        padded_width = width + self.offset_x
        padded_height = height + self.offset_y

        # Rechts/unten auf volle Kachelgroesse auffuellen
        padded_width = math.ceil(padded_width / TILE_SIZE) * TILE_SIZE
        padded_height = math.ceil(padded_height / TILE_SIZE) * TILE_SIZE

        canvas = PILImage.new(mode, (padded_width, padded_height), (0, 0, 0, 0))
        canvas.paste(img, (self.offset_x, self.offset_y))
        return canvas

    def to_tiles(
            self,
            img: Union[PILImage.Image, str],
            target_dir: str,
            ext: str = "jpg",
            quality: int = 85,
    ) -> None:
        """
        Zerschneidet das Bild (nach Skalierung/Ausrichtung via apply_offset)
        in einzelne 256x256px-Kacheln im Standard-Slippy-Map-Schema und
        speichert sie unter:

            target_dir/{zoom_level}/{tile_x}/{tile_y}.{ext}

        img: bereits geladenes PIL Image oder Dateipfad.
        target_dir: Zielverzeichnis, wird bei Bedarf angelegt.
        ext: Dateiendung/-format, z. B. "jpg" oder "png". JPEG unterstuetzt
             keine Transparenz - leere/transparente Bildbereiche werden
             dabei auf weissem Hintergrund geflatteted.
        quality: JPEG-Qualitaet (0-100), nur relevant fuer ext="jpg"/"jpeg".
        """
        aligned = self.apply_offset(img)
        width, height = aligned.size

        # Weltpixel-Position der linken oberen Ecke des ausgerichteten
        # (gepaddeten) Bildes bestimmen - das ist per Konstruktion
        # (siehe apply_offset/offset_x/offset_y) ein Vielfaches von TILE_SIZE.
        world_topleft_x, world_topleft_y = _lat_lng_to_world_px(
            self.topleft.lat, self.topleft.lng, self.zoom_level
        )
        canvas_world_x = round(world_topleft_x) - self.offset_x
        canvas_world_y = round(world_topleft_y) - self.offset_y
        tile_x0 = canvas_world_x // TILE_SIZE
        tile_y0 = canvas_world_y // TILE_SIZE

        cols = width // TILE_SIZE
        rows = height // TILE_SIZE
        ext_lower = ext.lower().lstrip(".")
        is_jpeg = ext_lower in ("jpg", "jpeg")

        for row in range(rows):
            print(f"Process Row {row} / {rows}")
            for col in range(cols):
                box = (
                    col * TILE_SIZE,
                    row * TILE_SIZE,
                    (col + 1) * TILE_SIZE,
                    (row + 1) * TILE_SIZE,
                )
                tile = aligned.crop(box)

                # komplett leere (transparente) Kacheln ueberspringen
                if tile.getbbox() is None:
                    continue

                tile_x = tile_x0 + col
                tile_y = tile_y0 + row

                tile_path = os.path.join(target_dir, f"z{self.zoom_level}_x{tile_x}_y{tile_y}.{ext_lower}")

                if is_jpeg:
                    # JPEG kennt keine Transparenz -> auf weissem
                    # Hintergrund flatten, bevor gespeichert wird
                    background = PILImage.new("RGB", tile.size, (255, 255, 255))
                    background.paste(tile)
                    background.save(tile_path, "JPEG", quality=quality)
                else:
                    tile.save(tile_path)


# ---------------------------------------------------------------------------
# Web-Mercator Projektion (identisch zum Google-/OSM-Kachelschema)
# ---------------------------------------------------------------------------

def _lat_lng_to_world_px(lat: float, lng: float, zoom: int) -> Tuple[float, float]:
    """Projiziert lat/lng in Web-Mercator-Weltpixelkoordinaten bei gegebenem Zoom."""
    siny = math.sin(lat * math.pi / 180)
    siny = min(max(siny, -0.9999), 0.9999)  # Pole abschneiden (Mercator-Grenze)
    scale = TILE_SIZE * (2 ** zoom)
    x = (0.5 + lng / 360) * scale
    y = (0.5 - math.log((1 + siny) / (1 - siny)) / (4 * math.pi)) * scale
    return x, y


def _world_px_to_lat_lng(x: float, y: float, zoom: int) -> LatLng:
    """Rueckprojektion von Weltpixelkoordinaten nach lat/lng."""
    scale = TILE_SIZE * (2 ** zoom)
    lng = (x / scale - 0.5) * 360
    n = math.pi - 2 * math.pi * y / scale
    lat = 180 / math.pi * math.atan(0.5 * (math.exp(n) - math.exp(-n)))
    return LatLng(lat=lat, lng=lng)


# ---------------------------------------------------------------------------
# Least-Squares Fit: world = s * pixel + t   (gleichfoermige Skalierung,
# keine Rotation - passend fuer nordausgerichtete, unverzerrte Bilder)
# ---------------------------------------------------------------------------

def _solve_3x3(m: List[List[float]], rhs: List[float]) -> List[float]:
    """Loest ein 3x3-Gleichungssystem per Gauss-Elimination (ohne numpy)."""
    m = [row[:] for row in m]
    rhs = rhs[:]
    n = 3
    for i in range(n):
        pivot = max(range(i, n), key=lambda r: abs(m[r][i]))
        m[i], m[pivot] = m[pivot], m[i]
        rhs[i], rhs[pivot] = rhs[pivot], rhs[i]
        piv_val = m[i][i]
        if abs(piv_val) < 1e-12:
            raise ValueError("Referenzpunkte sind linear abhaengig / degenerativ.")
        for k in range(i + 1, n):
            factor = m[k][i] / piv_val
            for j in range(i, n):
                m[k][j] -= factor * m[i][j]
            rhs[k] -= factor * rhs[i]

    x = [0.0] * n
    for i in reversed(range(n)):
        total = rhs[i] - sum(m[i][j] * x[j] for j in range(i + 1, n))
        x[i] = total / m[i][i]
    return x


def _fit_scale_translation(points: List[Tuple[float, float, float, float]]) -> Tuple[float, float, float]:
    """
    Fit fuer: world_x = s*px + tx ; world_y = s*py + ty
    points: Liste von (px, py, world_x, world_y). Benoetigt >= 2 Punkte.
    """
    if len(points) < 2:
        raise ValueError(
            "Fuer die automatische Bestimmung von Skalierung/Zoom werden "
            "mindestens 2 Referenzpunkte benoetigt. Mit nur einem Punkt "
            "bitte 'zoom_level' explizit an calc_transform uebergeben."
        )

    A, b = [], []
    for px, py, wx, wy in points:
        A.append([px, 1, 0]); b.append(wx)
        A.append([py, 0, 1]); b.append(wy)

    ata = [[0.0] * 3 for _ in range(3)]
    atb = [0.0, 0.0, 0.0]
    for row, val in zip(A, b):
        for i in range(3):
            atb[i] += row[i] * val
            for j in range(3):
                ata[i][j] += row[i] * row[j]

    s, tx, ty = _solve_3x3(ata, atb)
    return s, tx, ty


def _fit_translation_only(points: List[Tuple[float, float, float, float]]) -> Tuple[float, float]:
    """Fit fuer: world_x = px + tx ; world_y = py + ty (Skalierung fest = 1)."""
    tx = sum(wx - px for px, py, wx, wy in points) / len(points)
    ty = sum(wy - py for px, py, wx, wy in points) / len(points)
    return tx, ty


# ---------------------------------------------------------------------------
# Hauptfunktion
# ---------------------------------------------------------------------------

def calc_transform(ref: ImageRefs, zoom_level: Optional[int] = None) -> ImageTransform:
    """
    Berechnet aus georeferenzierten Bildpunkten ein ImageTransform, mit dem
    das Bild spaeter in Standard-Web-Mercator-Kacheln (256x256px) zerlegt
    werden kann.

    Faelle:
      - len(ref.refs) >= 2 und zoom_level=None:
          Zoom-Level UND Position werden automatisch bestimmt.
      - len(ref.refs) >= 1 und zoom_level angegeben:
          Nur die Position (Translation) wird bestimmt, die Skalierung
          entspricht dann exakt der des angegebenen Zoom-Levels.
      - len(ref.refs) == 1 und zoom_level=None:
          Fehler, da die Skalierung nicht bestimmbar ist.
    """
    if not ref.refs:
        raise ValueError("ImageRefs.refs ist leer - mindestens 1 Referenzpunkt noetig.")

    if zoom_level is None:
        # --- automatische Zoom-Erkennung (braucht >= 2 Punkte) ---
        points_zoom0 = [
            (r.x, r.y, *_lat_lng_to_world_px(r.lat, r.lng, zoom=0))
            for r in ref.refs
        ]
        s0, _, _ = _fit_scale_translation(points_zoom0)
        if s0 <= 0:
            raise ValueError("Ungueltige/degenerierte Referenzpunkte (Skalierung <= 0).")

        # world_px(zoom) = world_px(zoom0) * 2^zoom
        # gesucht: s0 * 2^zoom ≈ 1  =>  zoom = -log2(s0)
        ideal_zoom = -math.log2(s0)
        zoom_level = max(0, min(22, round(ideal_zoom)))

        # Fit direkt bei gewaehltem Zoom neu berechnen (praeziser wegen
        # nichtlinearer Mercator-Projektion)
        points_zoom = [
            (r.x, r.y, *_lat_lng_to_world_px(r.lat, r.lng, zoom=zoom_level))
            for r in ref.refs
        ]
        s, tx, ty = _fit_scale_translation(points_zoom)
        world_x_topleft = tx  # s*0 + tx
        world_y_topleft = ty
        scale = s

    else:
        # --- Zoom vorgegeben, nur Translation bestimmen ---
        points_zoom = [
            (r.x, r.y, *_lat_lng_to_world_px(r.lat, r.lng, zoom=zoom_level))
            for r in ref.refs
        ]
        tx, ty = _fit_translation_only(points_zoom)
        world_x_topleft = tx
        world_y_topleft = ty
        scale = 1.0  # ohne 2. Referenzpunkt nicht bestimmbar, wird als 1:1 angenommen

    topleft = _world_px_to_lat_lng(world_x_topleft, world_y_topleft, zoom_level)

    offset_x = int(round(world_x_topleft)) % TILE_SIZE
    offset_y = int(round(world_y_topleft)) % TILE_SIZE

    return ImageTransform(
        zoom_level=zoom_level,
        offset_x=offset_x,
        offset_y=offset_y,
        topleft=topleft,
        scale=scale,
    )


# ---------------------------------------------------------------------------
# Beispiel / manueller Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    # Mit 2 Punkten kann der Zoom automatisch bestimmt werden
    #two_refs = ImageRefs(width=14881, height=18037, refs=[
    #    ImageRef(x=10159, y=3986, lat=49.45102834650, lng=5.98198056221),
    #    ImageRef(x=7833, y=1323, lat=49.54821970976442, lng=5.850943922996522)])
    #transform2 = calc_transform(two_refs)
    #print("\nMit 2 Referenzpunkten (Zoom automatisch bestimmt):")
    #print(transform2)
    #ImageTransform(zoom_level=15, offset_x=119, offset_y=254, topleft=LatLng(lat=49.59653702187687, lng=5.4103771533733225))
    t = ImageTransform(zoom_level=15, offset_x=119, offset_y=254,
                   topleft=LatLng(lat=49.59653702187687, lng=5.4103771533733225), scale=1.3108756447911694)
    #t.apply_offset('/mnt/d/a.jpg').save("/mnt/d/b.jpg")
    t.to_tiles("/mnt/d/a.jpg", "/mnt/d/tiles")

