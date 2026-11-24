from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import dev as d
from nwebclient import web as w
from nwebclient.runner import op


class JoystickM5(r.BaseJobExecutor):
    """
      M5Stack Joystick 2 (Unit, I2C-Adresse 0x63 an Bus 1): Achsen, Taster und RGB-LED.
      op=read                        X/Y (8 und 16 Bit), Taster (0 = gedrueckt), Firmware, Adresse
      op=rgb r g b                   Farbe der LED setzen
      op=setaction jobdef x_var      alle 0,5 s einen Job mit dem X-Wert in x_var ausfuehren
    """

    MODULES = ["smbus2"]

    JOYSTICK_ADDR = 0x63

    JOYSTICK2_ADC_VALUE_12BITS_REG = 0x00
    JOYSTICK2_ADC_VALUE_8BITS_REG = 0x10
    JOYSTICK2_BUTTON_REG = 0x20
    JOYSTICK2_RGB_REG = 0x30
    JOYSTICK2_ADC_VALUE_CAL_REG = 0x40
    JOYSTICK2_OFFSET_ADC_VALUE_12BITS_REG = 0x50
    JOYSTICK2_OFFSET_ADC_VALUE_8BITS_REG = 0x60
    JOYSTICK2_FIRMWARE_VERSION_REG = 0xFE
    JOYSTICK2_BOOTLOADER_VERSION_REG = 0xFC
    JOYSTICK2_I2C_ADDRESS_REG = 0xFF

    def __init__(self):
        super().__init__("joystick")
        from smbus2 import SMBus

        self.bus = SMBus(1)
        self.ticker = None
        self.jobdef = None
        self.jobdef_x_name = None

        self.define_sig(d.PStr("op", "read")).set_description(
            "Achsen (8 und 16 Bit), Taster (0 = gedrueckt), Firmware, Bootloader und I2C-Adresse"
        ).r(success='bool', message='str', x16='int', y16='int', x='int', y='int', button='int',
            firmware='int', bootloader='int', address='str')
        self.define_sig(
            d.PStr("op", "rgb"),
            d.Param("r", "int"),
            d.Param("g", "int"),
            d.Param("b", "int")
        ).set_description("Farbe der LED setzen (0-255 je Kanal)").r(success='bool', message='str')
        self.define_sig(
            d.PStr("op", "setaction"),
            d.PStr("jobdef"),
            d.PStr("x_var")
        ).set_description("Alle 0,5 s den Job jobdef mit dem X-Wert (8 Bit) in x_var ausfuehren").r(
            success='bool', message='str')

    # ------------------------------------------------------------------
    # low level
    # ------------------------------------------------------------------

    def read_bytes(self, reg, length):
        return self.bus.read_i2c_block_data(
            self.JOYSTICK_ADDR,
            reg,
            length
        )

    def write_bytes(self, reg, data):
        self.bus.write_i2c_block_data(
            self.JOYSTICK_ADDR,
            reg,
            data
        )

    # ------------------------------------------------------------------
    # joystick values
    # ------------------------------------------------------------------

    def get_adc_16bit_xy(self):
        import struct
        data = self.read_bytes(
            self.JOYSTICK2_ADC_VALUE_12BITS_REG,
            4
        )

        x = struct.unpack("<H", bytes(data[0:2]))[0]
        y = struct.unpack("<H", bytes(data[2:4]))[0]

        return x, y

    def get_adc_8bit_xy(self):
        data = self.read_bytes(
            self.JOYSTICK2_ADC_VALUE_8BITS_REG,
            2
        )
        return data[0], data[1]

    def get_button(self):
        return self.read_bytes(
            self.JOYSTICK2_BUTTON_REG,
            1
        )[0]

    # ------------------------------------------------------------------
    # info
    # ------------------------------------------------------------------

    def get_firmware_version(self):
        return self.read_bytes(
            self.JOYSTICK2_FIRMWARE_VERSION_REG,
            1
        )[0]

    def get_bootloader_version(self):
        return self.read_bytes(
            self.JOYSTICK2_BOOTLOADER_VERSION_REG,
            1
        )[0]

    def get_i2c_address(self):
        return self.read_bytes(
            self.JOYSTICK2_I2C_ADDRESS_REG,
            1
        )[0]

    # ------------------------------------------------------------------
    # RGB
    # ------------------------------------------------------------------

    def set_rgb(self, r, g, b):
        import struct
        color = (
            (0xFF << 24) |
            (r << 16) |
            (g << 8) |
            b
        )

        data = list(struct.pack("<I", color))

        self.write_bytes(
            self.JOYSTICK2_RGB_REG,
            data
        )

    # ------------------------------------------------------------------
    # operations
    # ------------------------------------------------------------------

    def execute_read(self, data):

        x16, y16 = self.get_adc_16bit_xy()
        x8, y8 = self.get_adc_8bit_xy()

        return self.success(
            x16=x16,
            y16=y16,
            x=x8,
            y=y8,
            button=self.get_button(),  # 0 für pressed
            firmware=self.get_firmware_version(),
            bootloader=self.get_bootloader_version(),
            address=hex(self.get_i2c_address())
        )

    def execute_rgb(self, data):

        self.set_rgb(
            int(data["r"]),
            int(data["g"]),
            int(data["b"])
        )

        return self.success()

    # ------------------------------------------------------------------
    # html ui
    # ------------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):

        x16, y16 = self.get_adc_16bit_xy()
        x8, y8 = self.get_adc_8bit_xy()

        p.ul([
            f"X (16 Bit): {x16}",
            f"Y (16 Bit): {y16}",
            f"X (8 Bit): {x8}",
            f"Y (8 Bit): {y8}",
            f"Button: {self.get_button()}",
            f"Firmware: {self.get_firmware_version()}",
            f"Bootloader: {self.get_bootloader_version()}",
            f"I2C-Adresse: 0x{self.get_i2c_address():02X}"
        ])

        p.ul([
            w.a("Aktualisieren", self.link(self.part_index)),
            w.a("Bind", self.link(self.part_bind))
        ])

    def tick(self):
        x, y = self.get_adc_8bit_xy()
        if self.jobdef is not None:
            data = {self.jobdef_x_name: x, **self.jobdef}
            self.owner().execute(data)

    def execute_setaction(self, data):
        jobdef = data['jobdef']
        x_var = data['x_var']
        self.jobdef = u.load_to_dict(jobdef)
        self.jobdef_x_name = x_var
        if self.ticker is None:
            self.ticker = self.periodic(0.5, self.tick)
        return self.success()

    def part_bind(self, p: b.Page, params={}):
        p("Set Actions")
        p("X-Achse")
        p.form_input("Jobdef", "x_def")
        p.form_input("X-Varname", "x_var")
        p(self.action_btn_parametric("Bind", dict(
            type=self.type,
            op='setaction',
            jobdef='#x_def',
            x_var='#x_var'
        )))


class AS5600(r.BaseJobExecutor):
    """
      AS5600 Magnet-Drehwinkelsensor (12 Bit, I2C-Adresse 0x36 an Bus 1).
      op=read                        Winkel (roh, skaliert, Grad), Magnetstatus, AGC, Magnitude
      op=setzero                     aktuelle Position als Nullpunkt setzen (fluechtig, kein Burn)
      op=setaction jobdef var        alle 0,5 s einen Job mit dem Winkel (Grad) in var ausfuehren
    """

    MODULES = ["smbus2"]

    AS5600_ADDR = 0x36

    AS5600_ZMCO_REG = 0x00
    AS5600_ZPOS_REG = 0x01
    AS5600_MPOS_REG = 0x03
    AS5600_MANG_REG = 0x05
    AS5600_CONF_REG = 0x07
    AS5600_RAW_ANGLE_REG = 0x0C
    AS5600_ANGLE_REG = 0x0E
    AS5600_STATUS_REG = 0x0B
    AS5600_AGC_REG = 0x1A
    AS5600_MAGNITUDE_REG = 0x1B

    STATUS_MH = 0x08  # Magnet zu stark
    STATUS_ML = 0x10  # Magnet zu schwach
    STATUS_MD = 0x20  # Magnet erkannt

    def __init__(self, bus=1):
        super().__init__("as5600")
        from smbus2 import SMBus

        self.bus = SMBus(int(bus))
        self.ticker = None
        self.jobdef = None
        self.jobdef_var_name = None

        self.define_sig(d.PStr("op", "read"))
        self.define_sig(d.PStr("op", "setzero"))

    # ------------------------------------------------------------------
    # low level
    # ------------------------------------------------------------------

    def read_bytes(self, reg, length):
        return self.bus.read_i2c_block_data(
            self.AS5600_ADDR,
            reg,
            length
        )

    def write_bytes(self, reg, data):
        self.bus.write_i2c_block_data(
            self.AS5600_ADDR,
            reg,
            data
        )

    def read_12bit(self, reg):
        data = self.read_bytes(reg, 2)
        return ((data[0] << 8) | data[1]) & 0x0FFF

    def write_12bit(self, reg, value):
        value = int(value) & 0x0FFF
        self.write_bytes(reg, [value >> 8, value & 0xFF])

    # ------------------------------------------------------------------
    # sensor values
    # ------------------------------------------------------------------

    def get_raw_angle(self):
        return self.read_12bit(self.AS5600_RAW_ANGLE_REG)

    def get_angle(self):
        """ skalierter Winkel (beruecksichtigt ZPOS/MPOS/MANG) """
        return self.read_12bit(self.AS5600_ANGLE_REG)

    def get_degrees(self):
        return self.get_angle() * 360.0 / 4096.0

    def get_status(self):
        return self.read_bytes(self.AS5600_STATUS_REG, 1)[0]

    def get_magnet(self):
        status = self.get_status()
        return {
            "detected": bool(status & self.STATUS_MD),
            "too_weak": bool(status & self.STATUS_ML),
            "too_strong": bool(status & self.STATUS_MH)
        }

    def get_agc(self):
        return self.read_bytes(self.AS5600_AGC_REG, 1)[0]

    def get_magnitude(self):
        return self.read_12bit(self.AS5600_MAGNITUDE_REG)

    def get_burn_count(self):
        return self.read_bytes(self.AS5600_ZMCO_REG, 1)[0] & 0x03

    def set_zero(self, raw=None):
        if raw is None:
            raw = self.get_raw_angle()
        self.write_12bit(self.AS5600_ZPOS_REG, raw)
        return raw

    # ------------------------------------------------------------------
    # operations
    # ------------------------------------------------------------------

    def execute_read(self, data):
        angle = self.get_angle()
        return self.success({
            "raw_angle": self.get_raw_angle(),
            "angle": angle,
            "degrees": round(angle * 360.0 / 4096.0, 2),
            "magnet": self.get_magnet(),
            "agc": self.get_agc(),
            "magnitude": self.get_magnitude(),
            "zpos": self.read_12bit(self.AS5600_ZPOS_REG),
            "burn_count": self.get_burn_count()
        })

    def execute_setzero(self, data):
        return self.success({"zpos": self.set_zero()})

    # ------------------------------------------------------------------
    # html ui
    # ------------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):
        magnet = self.get_magnet()
        p.ul([
            f"Winkel: {self.get_degrees():.2f} Grad",
            f"Winkel (skaliert): {self.get_angle()}",
            f"Winkel (roh): {self.get_raw_angle()}",
            f"Magnet erkannt: {magnet['detected']}",
            f"Magnet zu schwach: {magnet['too_weak']}",
            f"Magnet zu stark: {magnet['too_strong']}",
            f"AGC: {self.get_agc()}",
            f"Magnitude: {self.get_magnitude()}",
            f"ZPOS: {self.read_12bit(self.AS5600_ZPOS_REG)}"
        ])

        p(self.action_btn(dict(title="Nullpunkt setzen", type=self.type, op='setzero')))

        p.ul([
            w.a("Aktualisieren", self.link(self.part_index)),
            w.a("Bind", self.link(self.part_bind))
        ])

    def tick(self):
        deg = self.get_degrees()
        if self.jobdef is not None:
            data = {self.jobdef_var_name: deg, **self.jobdef}
            self.owner().execute(data)

    def execute_setaction(self, data):
        self.jobdef = u.load_to_dict(data['jobdef'])
        self.jobdef_var_name = data['var']
        if self.ticker is None:
            self.ticker = self.periodic(0.5, self.tick)
        return self.success()

    def part_bind(self, p: b.Page, params={}):
        p("Set Actions")
        p("Winkel (Grad)")
        p.form_input("Jobdef", "a_def")
        p.form_input("Varname", "a_var")
        p(self.action_btn_parametric("Bind", dict(
            type=self.type,
            op='setaction',
            jobdef='#a_def',
            var='#a_var'
        )))


class FS3000(r.BaseJobExecutor):
    """
      Renesas FS3000 Luftstromsensor (12 Bit, I2C-Adresse 0x28 an Bus 1).
      Varianten: 1005 (0 - 7,23 m/s) und 1015 (0 - 15 m/s), Rohwerte 409 (0 m/s) bis 3686 (Endwert).
      op=read [samples]              Luftgeschwindigkeit (m/s, km/h, mph) und Rohwert, gemittelt
      op=set_range range             Variante 1005 oder 1015 setzen
      op=setaction jobdef var        alle 0,5 s einen Job mit der Geschwindigkeit (m/s) in var ausfuehren
    """

    MODULES = ["smbus2"]

    FS3000_ADDR = 0x28

    RAW_MIN = 409
    RAW_MAX = 3686

    # Kennlinien laut Datenblatt (Rohwert -> m/s)
    CURVES = {
        "1005": ([409, 915, 1522, 2066, 2523, 2908, 3256, 3572, 3686],
                 [0.0, 1.07, 2.01, 3.0, 3.97, 4.96, 5.98, 6.99, 7.23]),
        "1015": ([409, 1203, 1597, 1908, 2187, 2400, 2629, 2801, 3006, 3178, 3309, 3563, 3686],
                 [0.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 13.0, 15.0])
    }

    def __init__(self, bus=1, range="1005", address=FS3000_ADDR):
        super().__init__("fs3000")
        from smbus2 import SMBus

        self.bus = SMBus(int(bus))
        self.address = int(address)
        self.range = str(range) if str(range) in self.CURVES else "1005"
        self.ticker = None
        self.jobdef = None
        self.jobdef_var_name = None
        self.last = None

    # ------------------------------------------------------------------
    # low level
    # ------------------------------------------------------------------

    def read_frame(self):
        """ 5 Byte: Checksumme, Daten High, Daten Low, generische Checksumme, generische Daten """
        from smbus2 import i2c_msg
        msg = i2c_msg.read(self.address, 5)
        self.bus.i2c_rdwr(msg)
        return list(msg)

    @staticmethod
    def check_frame(frame):
        """ Summe aller 5 Byte muss (mod 256) 0 ergeben """
        return (sum(frame) & 0xFF) == 0

    def read_raw(self):
        frame = self.read_frame()
        if not self.check_frame(frame):
            raise ValueError(f"FS3000 Checksumme falsch: {frame}")
        return ((frame[1] << 8) | frame[2]) & 0x0FFF

    def read_raw_avg(self, samples=1):
        import time
        values = []
        for i in range(max(1, int(samples))):
            values.append(self.read_raw())
            if i + 1 < samples:
                time.sleep(0.01)
        return sum(values) / len(values)

    # ------------------------------------------------------------------
    # umrechnung
    # ------------------------------------------------------------------

    def raw_to_mps(self, raw):
        xs, ys = self.CURVES[self.range]
        if raw <= xs[0]:
            return 0.0
        if raw >= xs[-1]:
            return ys[-1]
        for i in range(1, len(xs)):
            if raw <= xs[i]:
                t = (raw - xs[i - 1]) / (xs[i] - xs[i - 1])
                return ys[i - 1] + t * (ys[i] - ys[i - 1])
        return ys[-1]

    def get_mps(self, samples=1):
        return self.raw_to_mps(self.read_raw_avg(samples))

    def measure(self, samples=1):
        raw = self.read_raw_avg(samples)
        mps = self.raw_to_mps(raw)
        self.last = {
            "raw": round(raw, 1),
            "mps": round(mps, 3),
            "kmh": round(mps * 3.6, 2),
            "mph": round(mps * 2.23694, 2),
            "range": self.range,
            "max_mps": self.CURVES[self.range][1][-1]
        }
        return self.last

    # ------------------------------------------------------------------
    # operations
    # ------------------------------------------------------------------

    @op("Luftgeschwindigkeit (mps, kmh, mph) und Rohwert, gemittelt ueber samples Messungen",
        result={'success': 'bool', 'message': 'str', 'raw': 'float', 'mps': 'float', 'kmh': 'float',
                'mph': 'float', 'range': 'str', 'max_mps': 'float'})
    def read(self, samples: int = 5):
        try:
            return self.success(**self.measure(int(samples)))
        except Exception as e:
            return self.fail(str(e))

    @op("Variante setzen: 1005 (0 - 7,23 m/s) oder 1015 (0 - 15 m/s)",
        result={'success': 'bool', 'message': 'str'})
    def set_range(self, range: str = "1005"):
        range = str(range)
        if range not in self.CURVES:
            return self.fail(f"Unbekannte Variante {range}, erlaubt: {', '.join(self.CURVES)}")
        self.range = range
        return self.success(f"Variante {range}", ui_reload=True)

    @op("Alle 0,5 s den Job jobdef mit der Geschwindigkeit (m/s) in var ausfuehren",
        result={'success': 'bool', 'message': 'str'})
    def setaction(self, jobdef: str, var: str):
        self.jobdef = u.load_to_dict(jobdef)
        self.jobdef_var_name = var
        if self.ticker is None:
            self.ticker = self.periodic(0.5, self.tick)
        return self.success()

    def tick(self):
        if self.jobdef is not None:
            data = {self.jobdef_var_name: round(self.get_mps(3), 3), **self.jobdef}
            self.owner().execute(data)

    # ------------------------------------------------------------------
    # html ui
    # ------------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):
        try:
            m = self.measure(5)
            p.ul([
                f"Geschwindigkeit: {m['mps']:.2f} m/s ({m['kmh']:.1f} km/h)",
                f"Rohwert: {m['raw']} ({self.RAW_MIN} - {self.RAW_MAX})",
                f"Variante: FS3000-{m['range']} (max. {m['max_mps']} m/s)",
                f"I2C-Adresse: 0x{self.address:02X}"
            ])
        except Exception as e:
            p(f"Fehler beim Lesen: {e}")
        for rng in self.CURVES:
            p(self.action_btn(dict(type=self.type, op='set_range', range=rng),
                              label=f"FS3000-{rng}", hint=f"Kennlinie der Variante FS3000-{rng} verwenden"))
        p.ul([
            w.a("Aktualisieren", self.link(self.part_index)),
            w.a("Bind", self.link(self.part_bind))
        ])

    def part_bind(self, p: b.Page, params={}):
        p("Set Actions")
        p("Luftgeschwindigkeit (m/s)")
        p.form_input("Jobdef", "f_def")
        p.form_input("Varname", "f_var")
        p(self.action_btn_parametric("Bind", dict(
            type=self.type,
            op='setaction',
            jobdef='#f_def',
            var='#f_var'
        ), hint="Job alle 0,5 s mit der aktuellen Geschwindigkeit ausfuehren"))
