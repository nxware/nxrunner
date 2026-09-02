from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import dev as d


class JoystickM5(r.BaseJobExecutor):

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

        self.define_sig(
            d.PStr("op", "read"),
        )

        self.define_sig(
            d.PStr("op", "rgb"),
            d.Param("r", "int"),
            d.Param("g", "int"),
            d.Param("b", "int")
        )

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

        return self.success({
            "x16": x16,
            "y16": y16,
            "x8": x8,
            "y8": y8,
            "button": self.get_button(),
            "firmware": self.get_firmware_version(),
            "bootloader": self.get_bootloader_version(),
            "address": hex(self.get_i2c_address())
        })

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

        p.h("Joystick 2")

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

        p.div(
            p.a(
                "Aktualisieren",
                "?op=index"
            )
        )

        return p