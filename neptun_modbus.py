"""
Локальный доступ к Neptun Smart по Modbus (порт 503, адрес 240 по умолчанию).

Карта регистров: «Карта памяти регистров модуля управления Neptun Smart для протокола MODBUS-RTU».
Модуль Wi-Fi отдаёт эти же регистры по TCP. Клиент ниже написан без внешних зависимостей и умеет два
способа упаковки: обычный Modbus TCP и Modbus RTU поверх TCP (определяется автоматически).

Модуль принимает только одно TCP-соединение на порту 503, поэтому на каждый опрос мы открываем
соединение, читаем всё нужное и закрываем.
"""
import socket
import struct

REG_STATUS = 0       # конфигурация и статус модуля
REG_WIRED = 3        # статус проводных линий (биты 0..3)
REG_RF_COUNT = 6     # количество беспроводных датчиков
REG_RF_STATUS = 57   # статус беспроводного датчика 1 (дальше по одному регистру до 106)
REG_COUNTERS = 107   # показания: 8 счётчиков по 2 регистра (старшие, младшие)


class ModbusError(Exception):
    pass


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ModbusError("соединение закрыто устройством")
        buf += chunk
    return buf


class ModbusConnection:
    """Одно соединение; read() можно вызывать несколько раз подряд."""

    def __init__(self, host, port=503, unit=240, framing="auto", timeout=5.0):
        self.host, self.port, self.unit, self.timeout = host, port, unit, timeout
        self.framing = framing  # auto | tcp | rtu
        self._tid = 0
        self._sock = None

    def __enter__(self):
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._sock.settimeout(self.timeout)
        return self

    def __exit__(self, *exc):
        try:
            self._sock.close()
        except Exception:
            pass

    def _read_tcp(self, start, count):
        self._tid = (self._tid + 1) & 0xFFFF
        pdu = struct.pack(">BHH", 3, start, count)
        self._sock.sendall(struct.pack(">HHHB", self._tid, 0, len(pdu) + 1, self.unit) + pdu)
        _tid, proto, length, unit = struct.unpack(">HHHB", _recv_exact(self._sock, 7))
        if proto != 0 or not 2 <= length <= 260:
            raise ModbusError("ответ не похож на Modbus TCP")
        data = _recv_exact(self._sock, length - 1)
        return self._parse_pdu(data, count)

    def _read_rtu(self, start, count):
        body = struct.pack(">BBHH", self.unit, 3, start, count)
        self._sock.sendall(body + struct.pack("<H", crc16(body)))
        head = _recv_exact(self._sock, 3)
        if head[1] & 0x80:
            rest = _recv_exact(self._sock, 2)
            raise ModbusError(f"устройство вернуло исключение Modbus, код {head[2]}")
        rest = _recv_exact(self._sock, head[2] + 2)
        frame = head + rest
        if struct.unpack("<H", frame[-2:])[0] != crc16(frame[:-2]):
            raise ModbusError("неверная контрольная сумма RTU")
        if head[0] != self.unit:
            raise ModbusError("ответ от другого адреса")
        return self._parse_pdu(frame[1:-2], count)

    @staticmethod
    def _parse_pdu(pdu, count):
        if pdu[0] & 0x80:
            raise ModbusError(f"устройство вернуло исключение Modbus, код {pdu[1] if len(pdu) > 1 else '?'}")
        if pdu[0] != 3 or len(pdu) < 2 or pdu[1] != count * 2 or len(pdu) < 2 + count * 2:
            raise ModbusError("неожиданная длина ответа")
        return list(struct.unpack(f">{count}H", pdu[2 : 2 + count * 2]))

    def read(self, start, count):
        if self.framing == "tcp":
            return self._read_tcp(start, count)
        if self.framing == "rtu":
            return self._read_rtu(start, count)
        # автоопределение: сначала Modbus TCP, затем RTU поверх TCP
        errors = []
        for name, fn in (("tcp", self._read_tcp), ("rtu", self._read_rtu)):
            try:
                result = fn(start, count)
                self.framing = name
                return result
            except (ModbusError, OSError, struct.error) as e:
                errors.append(f"{name}: {e}")
                # после неудачи соединение в неопределённом состоянии: открываем заново
                self._sock.close()
                self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
                self._sock.settimeout(self.timeout)
        raise ModbusError("; ".join(errors))


def read_snapshot(host, port=503, unit=240, framing="auto", timeout=5.0):
    """Читает всё нужное для контроля одним соединением. Возвращает (данные, определённый framing)."""
    with ModbusConnection(host, port, unit, framing, timeout) as c:
        head = c.read(REG_STATUS, 7)  # регистры 0..6
        rf_count = min(head[REG_RF_COUNT], 50)
        tail = c.read(REG_RF_STATUS, 66)  # 57..122: статусы датчиков и показания счётчиков
        framing = c.framing
    return decode_snapshot(head, tail, rf_count), framing


def decode_snapshot(head, tail, rf_count):
    r0, r3 = head[0], head[REG_WIRED]
    bit = lambda value, n: bool(value >> n & 1)
    rf = []
    for i in range(rf_count):
        v = tail[i]
        rf.append(
            {
                "n": i + 1,
                "battery": v >> 8,
                "signal": v >> 3 & 7,
                "lost": bit(v, 2),
                "discharged": bit(v, 1),
                "alarm": bit(v, 0),
            }
        )
    counters = []
    base = REG_COUNTERS - REG_RF_STATUS  # 50
    for i in range(8):
        hi, lo = tail[base + i * 2], tail[base + i * 2 + 1]
        counters.append((hi << 16) | lo)
    return {
        "floor_cleaning": bit(r0, 0),
        "alarm_g1": bit(r0, 1),
        "alarm_g2": bit(r0, 2),
        "battery_low": bit(r0, 3),
        "sensor_lost": bit(r0, 4),
        "closed_by_loss_g1": bit(r0, 5),
        "closed_by_loss_g2": bit(r0, 6),
        "valves_g1_open": bit(r0, 8),
        "valves_g2_open": bit(r0, 9),
        "two_groups": bit(r0, 10),
        "close_on_sensor_loss": bit(r0, 11),
        "keyboard_lock": bit(r0, 12),
        "wired_alarm": [bit(r3, i) for i in range(4)],
        "rf": rf,
        "counters": counters,  # литры или импульсы, как отдаёт модуль: сверяйте с приложением
        "raw_status": r0,
    }
