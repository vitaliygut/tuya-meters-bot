"""
tuya-meters-bot

Показания счётчиков воды Neptun (кнопка, ежедневный автосъём, ежемесячный отчёт) и передача на mos.ru.
Источник показаний на выбор (READ_SOURCE): cloud (Tuya API), local (локальная сеть, tinytuya) или auto.
"""
import asyncio
import base64
import calendar
import io
import json
import logging
import os
import re
import socket
import sqlite3
import threading
import time
from datetime import datetime

import qrcode
import tinytuya
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

import mosru
from mosru_api import MosRuApiError, MosRuAuthError, MosRuTemporaryError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tuya-bot")

# --- настройки ---------------------------------------------------------
REGION = os.environ.get("TUYA_REGION", "eu")
API_ID = os.environ.get("TUYA_ACCESS_ID", "").strip()
API_SECRET = os.environ.get("TUYA_ACCESS_SECRET", "").strip()
DEVICE_ID = os.environ["TUYA_DEVICE_ID"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])  # чат, где работают кнопки
NOTIFY_CHAT_ID = int(os.environ.get("NOTIFY_CHAT_ID") or CHAT_ID)  # куда слать алерты и отчёты

DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")
API_MONTHLY_LIMIT = int(os.environ.get("API_MONTHLY_LIMIT", "26000"))  # только для отображения

ALERT_REPEAT_HOURS = float(os.environ.get("ALERT_REPEAT_HOURS", "24"))  # напоминание о нерешённой проблеме

# Откуда брать показания: cloud (Tuya API) | local (локальная сеть) | auto (локально, при сбое облако)
READ_SOURCE = os.environ.get("READ_SOURCE", "cloud").strip().lower()
LOCAL_IP = os.environ.get("TUYA_LOCAL_IP", "").strip()
LOCAL_KEY_ENV = os.environ.get("TUYA_LOCAL_KEY", "").strip()  # пусто = взять из облака один раз
LOCAL_VERSION = os.environ.get("TUYA_LOCAL_VERSION", "auto").strip()  # auto | 3.3 | 3.4 | 3.5
LOCAL_RETRIES = int(os.environ.get("LOCAL_RETRIES", "3"))  # попыток на один запрос
if READ_SOURCE not in ("cloud", "local", "auto"):
    raise SystemExit("READ_SOURCE должен быть cloud, local или auto")
if READ_SOURCE in ("local", "auto") and not LOCAL_IP:
    raise SystemExit(f"READ_SOURCE={READ_SOURCE} требует TUYA_LOCAL_IP")
if READ_SOURCE in ("cloud", "auto") and not (API_ID and API_SECRET):
    raise SystemExit(f"READ_SOURCE={READ_SOURCE} требует TUYA_ACCESS_ID и TUYA_ACCESS_SECRET")
if READ_SOURCE == "local" and not (LOCAL_KEY_ENV or (API_ID and API_SECRET)):
    raise SystemExit("READ_SOURCE=local требует TUYA_LOCAL_KEY (либо ключи облака, чтобы получить его один раз)")

# автоматическое снятие показаний раз в сутки (облако, 1 вызов API в день)
DAILY_READ_HOUR = int(os.environ.get("DAILY_READ_HOUR", "8"))  # час по TZ, -1 = выключено
DAILY_RETRY_MINUTES = int(os.environ.get("DAILY_RETRY_MINUTES", "60"))  # повтор после неудачи
DAILY_REPORT = os.environ.get("DAILY_REPORT", "false").lower() == "true"  # слать показания в чат каждый день
NO_DATA_ALERT_DAYS = float(os.environ.get("NO_DATA_ALERT_DAYS", "3"))  # нет показаний N дней -> тревога

# ежемесячный отчёт: число месяца (0 = выключен) и час по времени TZ
MONTHLY_REPORT_DAY = int(os.environ.get("MONTHLY_REPORT_DAY", "0"))
MONTHLY_REPORT_HOUR = int(os.environ.get("MONTHLY_REPORT_HOUR", "9"))

# передача показаний на mos.ru (Электронный дом): off | confirm (с кнопкой подтверждения) | auto
MOSRU_MODE = os.environ.get("MOSRU_MODE", "off").strip().lower()
MOSRU_SUBMIT_DAY = int(os.environ.get("MOSRU_SUBMIT_DAY") or MONTHLY_REPORT_DAY or 20)
MOSRU_SUBMIT_HOUR = int(os.environ.get("MOSRU_SUBMIT_HOUR") or MONTHLY_REPORT_HOUR)
MOSRU_COLD_CODE = os.environ.get("MOSRU_COLD_CODE", "counter8").strip()  # DP холодной воды Neptun
MOSRU_HOT_CODE = os.environ.get("MOSRU_HOT_CODE", "counter7").strip()    # DP горячей воды Neptun
MOSRU_MAX_DELTA = int(os.environ.get("MOSRU_MAX_DELTA", "100"))  # макс. рост относительно портала, м³


def parse_sensors(raw):
    """code:Название:масштаб:единица,code2:..."""
    out = []
    for part in filter(None, (p.strip() for p in raw.split(","))):
        it = part.split(":")
        out.append(
            (
                it[0],
                it[1] if len(it) > 1 else it[0],
                float(it[2]) if len(it) > 2 else 1.0,
                it[3] if len(it) > 3 else "",
            )
        )
    return out


SENSORS = parse_sensors(os.environ.get("SENSORS", ""))

# --- кнопки ------------------------------------------------------------
BTN_NOW = "📊 Показания"
BTN_HEALTH = "🩺 Состояние"
BTN_RAW = "🔧 Все DP"
BTN_MOSRU = "📤 mos.ru"

_rows = [[KeyboardButton(text=BTN_NOW)], [KeyboardButton(text=BTN_HEALTH), KeyboardButton(text=BTN_RAW)]]
if MOSRU_MODE != "off":
    _rows.insert(1, [KeyboardButton(text=BTN_MOSRU)])
KEYBOARD = ReplyKeyboardMarkup(keyboard=_rows, resize_keyboard=True)

# --- хранилище (SQLite) -------------------------------------------------
def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS readings (ts INTEGER, code TEXT, value REAL)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings ON readings (code, ts)")
    return conn


def meta_get(key):
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def meta_set(key, value):
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))


def record_readings(status):
    """Сохраняет показания счётчиков из SENSORS. True, если получено хоть одно значение."""
    now_ts = int(time.time())
    rows = []
    for code, *_ in SENSORS:
        try:
            rows.append((now_ts, code, float(status[code])))
        except (KeyError, TypeError, ValueError):
            pass
    if not rows:
        return False
    with db() as conn:
        conn.executemany("INSERT INTO readings VALUES (?,?,?)", rows)
        conn.execute("DELETE FROM readings WHERE ts < ?", (now_ts - 400 * 86400,))
    meta_set("last_ok_ts", str(now_ts))
    return True


def reading_before(code, ts):
    """Последнее сохранённое значение не позже ts: (время, значение) или None."""
    with db() as conn:
        return conn.execute(
            "SELECT ts, value FROM readings WHERE code=? AND ts<=? ORDER BY ts DESC LIMIT 1", (code, int(ts))
        ).fetchone()


def count_api_call(n=1):
    try:
        key = "api_calls:" + datetime.now().strftime("%Y-%m")
        meta_set(key, str(int(meta_get(key) or 0) + n))
    except Exception:
        log.exception("count_api_call")


def api_usage():
    try:
        return int(meta_get("api_calls:" + datetime.now().strftime("%Y-%m")) or 0)
    except Exception:
        return 0


mosru_svc = mosru.MosRuService(
    meta_get,
    meta_set,
    place_id=os.environ.get("MOSRU_USER_PLACE_ID", "").strip(),
    cold_id=os.environ.get("MOSRU_COLD_ID", "").strip(),
    hot_id=os.environ.get("MOSRU_HOT_ID", "").strip(),
    max_delta=MOSRU_MAX_DELTA,
)

# --- облако Tuya: показания ----------------------------------------------
cloud = tinytuya.Cloud(apiRegion=REGION, apiKey=API_ID, apiSecret=API_SECRET, apiDeviceID=DEVICE_ID) if API_ID and API_SECRET else None
_cloud_lock = threading.Lock()


def cloud_get(url):
    """Любой запрос к облаку: под замком и со счётчиком вызовов."""
    if cloud is None:
        raise RuntimeError("Облако Tuya не настроено (TUYA_ACCESS_ID / TUYA_ACCESS_SECRET)")
    with _cloud_lock:
        count_api_call()
        return cloud.cloudrequest(url)


SOURCES = ("status", "device", "shadow")
_working_source = None


def _try_source(name):
    urls = {
        "status": f"/v1.0/devices/{DEVICE_ID}/status",
        "device": f"/v1.0/devices/{DEVICE_ID}",
        "shadow": f"/v2.0/cloud/thing/{DEVICE_ID}/shadow/properties",
    }
    res = cloud_get(urls[name])
    if not (res and res.get("success")):
        return None, res
    result = res.get("result")
    if name == "status":
        return {i["code"]: i["value"] for i in (result or [])}, None
    if name == "device":
        return {i["code"]: i["value"] for i in ((result or {}).get("status") or [])}, None
    props = (result or {}).get("properties") or []
    return {p.get("code") or str(p.get("dp_id")): p.get("value") for p in props}, None


def _get_status_sync():
    """Показания из облака: первый рабочий источник запоминается, чтобы не тратить лимит."""
    global _working_source
    order = [_working_source] if _working_source else list(SOURCES)
    errors = []
    for name in order:
        status, err = _try_source(name)
        if status:
            if _working_source != name:
                log.info("Источник данных Tuya: %s", name)
            _working_source = name
            return status
        if err is not None:
            errors.append(f"{name}: {err}")
    _working_source = None
    if errors and len(errors) == len(order):
        raise RuntimeError("Tuya API error: " + " | ".join(errors))
    return {}


# --- локальная сеть (tinytuya): показания без облака ---------------------------------------
_local = {"version": None, "key_check": 0.0}
_local_lock = threading.Lock()
read_state = {"source": None, "ts": None, "error": None, "version": None}


def decode_counters(blob):
    """counters_data (base64): записи по 8 байт, значение счётчика в литрах = байты 2..4 (big-endian).
    Сверено с облачными counter7/counter8 Neptun Smart. Возвращает {} если это не такой блок."""
    try:
        raw = base64.b64decode(blob, validate=True)
    except Exception:
        return {}
    if len(raw) < 16 or len(raw) % 8:
        return {}
    return {f"counter{i + 1}": int.from_bytes(raw[i * 8 + 2 : i * 8 + 5], "big") for i in range(len(raw) // 8)}


def _extract_counters(dps):
    for value in dps.values():
        if isinstance(value, str) and len(value) >= 20:
            counters = decode_counters(value)
            if counters:
                return counters
    return {}


# Локальные номера DP Neptun Smart -> коды (порядок как в облачном списке, сверен с локальным дампом:
# значения 101, 108-113, 115-119, 123-144 совпали; 114, 120-122, 145 приходят только при изменении).
NEPTUN_LOCAL_DPS = {
    "101": "floor_cleaning_flag", "108": "add_new_sensors", "109": "status_crane_group_1",
    "110": "status_crane_group_2", "111": "group_eneble", "112": "close_if_sensor_loss",
    "113": "child_protection", "114": "line_settings", "115": "status_wire_line",
    "116": "relay_settings", "117": "address_modbus", "118": "modbus_speed", "119": "count_rf_sensors",
    "120": "settings_rf_sensors", "121": "status_rf_sensor", "122": "counters_data", "123": "notifications",
    "124": "name_rf_1_5", "125": "name_rf_6_10", "126": "name_rf_11_15", "127": "name_rf_16_20",
    "128": "name_rf_20_25", "129": "name_rf_26_30", "130": "name_rf_31_35", "131": "name_rf_36_40",
    "132": "name_rf_40_45", "133": "name_rf_46_50", "134": "name_lines", "135": "name_zones",
    "136": "name_of_counters", "145": "slots_map",
}
NEPTUN_LOCAL_DPS.update({str(136 + n): f"counter{n}" for n in range(1, 9)})  # 137..144 = counter1..counter8


def _to_codes(dps):
    return {NEPTUN_LOCAL_DPS.get(str(k), f"dp_{k}"): v for k, v in dps.items()}


def _counters_from(dps):
    """Счётчики из отдельных DP (137-144); если их нет, пробуем расшифровать блок counters_data."""
    counters = {c: v for c, v in _to_codes(dps).items() if re.fullmatch(r"counter\d+", c) and isinstance(v, (int, float))}
    return counters or _extract_counters(dps)


def _cloud_local_key_sync():
    """Актуальный local key из облака (1 вызов API)."""
    res = cloud_get(f"/v1.0/devices/{DEVICE_ID}")
    key = ((res or {}).get("result") or {}).get("local_key") if res and res.get("success") else None
    if not key:
        raise RuntimeError(f"не удалось получить local_key из облака: {res}")
    return key


def _local_key_sync():
    """Приоритет: ключ, подтверждённый облаком (после смены) > TUYA_LOCAL_KEY из .env > сохранённый > облако."""
    key = meta_get("local_key_override") or LOCAL_KEY_ENV or meta_get("local_key")
    if key:
        return key
    key = _cloud_local_key_sync()
    meta_set("local_key", key)
    return key


def _make_device(version, key):
    """Как в проверенном скрипте: disabledetect=True (иначе для 22-символьного ID tinytuya
    переключается на device22 и получает ошибки) и версия протокола задаётся явно."""
    d = tinytuya.Device(DEVICE_ID, LOCAL_IP, key)
    for name, arg in (("set_socketTimeout", 5), ("set_socketRetryLimit", 1)):
        try:
            getattr(d, name)(arg)
        except Exception:
            pass
    d.disabledetect = True
    d.set_version(float(version))
    return d


def _local_variants():
    if _local["version"]:
        return [_local["version"]]
    return ["3.4", "3.3", "3.5", "3.2"] if LOCAL_VERSION == "auto" else [LOCAL_VERSION]


def _local_attempt(key):
    """Несколько попыток чтения. Возвращает ({код: значение}, список ошибок) или (None, ошибки)."""
    errors = []
    for attempt in range(max(LOCAL_RETRIES, 1)):
        for version in _local_variants():
            device = _make_device(version, key)
            try:
                res = device.status()
            finally:
                try:
                    device.close()
                except Exception:
                    pass
            dps = (res or {}).get("dps")
            if dps:
                counters = _counters_from(dps)
                if counters:
                    _local["version"] = version
                    out = {c: v for c, v in _to_codes(dps).items() if not c.startswith("name_")}
                    out.update(counters)
                    return out, errors
                errors.append(f"{version}: в ответе нет счётчиков (DP: {', '.join(map(str, dps))})")
            else:
                errors.append(f"{version}: {(res or {}).get('Error')} (Err {(res or {}).get('Err')})")
        if attempt + 1 < LOCAL_RETRIES:
            time.sleep(1)
    return None, errors


def _local_status_sync():
    """Читает счётчики по локальной сети. Возвращает {counter1..counterN, dp_<id>: значение}."""
    with _local_lock:  # устройство обычно принимает одно соединение за раз
        key = _local_key_sync()
        status, errors = _local_attempt(key)
        if status:
            return status
        # Err 914 = не подошёл ключ или версия. Если устройство переподключали, ключ сменился: сверяем с облаком
        if any("Err 914" in e for e in errors) and cloud is not None and time.time() - _local["key_check"] > 3600:
            _local["key_check"] = time.time()
            try:
                fresh = _cloud_local_key_sync()
            except Exception as e:
                errors.append(f"сверка ключа с облаком: {e}")
            else:
                if fresh != key:
                    meta_set("local_key_override", fresh)
                    meta_set("local_key", fresh)
                    log.info("Local key обновлён из облака (прежний не подошёл)")
                    status, more = _local_attempt(fresh)
                    if status:
                        return status
                    errors += more
        _local["version"] = None  # в следующий раз версию определим заново
        raise RuntimeError("локальный опрос не удался: " + "; ".join(errors[-4:]))


def _lan_try_versions(key, out):
    """Пробует версии протокола с данным ключом. Возвращает (версия, счётчики) или None."""
    found = None
    for version in ("3.4", "3.3", "3.5", "3.2", "3.1"):
        try:
            res = _make_device(version, key).status()
        except Exception as e:
            res = {"Error": str(e), "Err": "exc"}
        dps = (res or {}).get("dps")
        if dps:
            counters = _counters_from(dps)
            out.append(f"   версия {version}: ✅ DP: {len(dps)}, счётчики: {'есть' if counters else 'нет'}")
            if counters and not found:
                found = (version, counters)
        else:
            out.append(f"   версия {version}: ❌ {(res or {}).get('Error')} (Err {(res or {}).get('Err')})")
    return found


def _lan_diag_sync():
    out = []
    if not LOCAL_IP:
        return ["TUYA_LOCAL_IP не задан в .env"]
    with _local_lock:
        try:
            socket.create_connection((LOCAL_IP, 6668), timeout=5).close()
            out.append(f"1) TCP {LOCAL_IP}:6668: ✅ порт открыт")
        except OSError as e:
            return out + [f"1) TCP {LOCAL_IP}:6668: ❌ {e}",
                          "Проверьте IP в роутере, что бот и устройство в одной сети, что модуль включён "
                          "и что приложение Smart Life не держит локальное соединение."]
        try:
            key = _local_key_sync()
            src = "подтверждённый облаком" if meta_get("local_key_override") else ("из .env" if LOCAL_KEY_ENV else "из базы/облака")
            out.append(f"2) local key: {src}, длина {len(key)} (должно быть 16)")
            if len(key) != 16:
                out.append("⚠️ Если в ключе есть $, в .env берите значение в одинарные кавычки: TUYA_LOCAL_KEY='abc$def'")
        except Exception as e:
            return out + [f"2) local key: ❌ {e}"]
        out.append("3) проверка версий протокола с этим ключом:")
        found = _lan_try_versions(key, out)
        if not found and cloud is not None:
            # возможно, устройство переподключали и ключ сменился: берём актуальный из облака (1 вызов API)
            try:
                fresh = _cloud_local_key_sync()
            except Exception as e:
                out.append(f"4) ключ из облака: ❌ {e}")
            else:
                if fresh == key:
                    out.append("4) ключ из облака совпадает с используемым: дело не в ключе")
                else:
                    out.append("4) ключ из облака ОТЛИЧАЕТСЯ от используемого, проверяю его:")
                    found = _lan_try_versions(fresh, out)
                    if found:
                        meta_set("local_key_override", fresh)
                        meta_set("local_key", fresh)
                        out.append("✅ Новый ключ подошёл и сохранён. Из .env можно удалить TUYA_LOCAL_KEY.")
        if found:
            _local["version"] = found[0]
            c = found[1]
            out.append("Счётчики: " + ", ".join(f"counter{i}={c[f'counter{i}']}" for i in range(1, len(c) + 1) if c.get(f"counter{i}")))
            out.append(f"Итог: работает версия {found[0]}. Закрепите: TUYA_LOCAL_VERSION={found[0]}")
        else:
            out.append("Итог: данные не получены. Если ключ из облака совпал с вашим, закройте приложение Smart Life, "
                       "выключите и включите питание модуля Neptun на минуту и повторите /lan.")
    return out


async def get_status():
    """Показания из выбранного источника (READ_SOURCE). В режиме auto: локально, при сбое облако."""
    if READ_SOURCE in ("local", "auto"):
        try:
            status = await asyncio.to_thread(_local_status_sync)
            read_state.update(source="local", ts=time.time(), error=None, version=_local["version"])
            return status
        except Exception as e:
            read_state["error"] = str(e)
            if READ_SOURCE == "local":
                raise
            log.warning("Локальный опрос не удался, берём из облака: %s", e)
    status = await asyncio.to_thread(_get_status_sync)
    read_state.update(source="cloud", ts=time.time(), version=None)
    if READ_SOURCE == "cloud":
        read_state["error"] = None
    return status


SECRET_KEYS = {"local_key", "uid", "ip", "lat", "lon", "owner_id", "sn", "uuid", "mac", "time_zone"}


def _scrub(obj):
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items() if k not in SECRET_KEYS}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    return obj


def _diag_sync():
    out = {}
    for name, url in [
        ("device", f"/v1.0/devices/{DEVICE_ID}"),
        ("status", f"/v1.0/devices/{DEVICE_ID}/status"),
        ("specifications", f"/v1.0/devices/{DEVICE_ID}/specifications"),
        ("shadow", f"/v2.0/cloud/thing/{DEVICE_ID}/shadow/properties"),
    ]:
        try:
            out[name] = _scrub(cloud_get(url))
        except Exception as e:
            out[name] = f"ошибка: {e}"
    return out


# --- форматирование -----------------------------------------------------
def fmt_val(v):
    return json.dumps(v, ensure_ascii=False)


def fmt_dur(seconds):
    seconds = int(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d} д {h} ч"
    if h:
        return f"{h} ч {m} мин"
    return f"{m} мин"


def fmt_ts(ts):
    return time.strftime("%d.%m.%Y %H:%M", time.localtime(ts)) if ts else "нет"


def fmt_now(status):
    if not SENSORS:
        return "SENSORS не задан в .env. Нажмите «🔧 Все DP», чтобы увидеть коды."
    lines = ["📊 Показания счётчиков:"]
    for code, name, scale, unit in SENSORS:
        if code not in status:
            lines.append(f"{name}: нет данных ({code})")
            continue
        try:
            lines.append(f"{name}: {float(status[code]) * scale:.3f} {unit}".strip())
        except (TypeError, ValueError):
            lines.append(f"{name}: {status[code]}")
    return "\n".join(lines)


def fmt_daily(status):
    now_ts = time.time()
    lines = [f"📅 Показания на {datetime.now():%d.%m.%Y}:"]
    for code, name, scale, unit in SENSORS:
        try:
            value = float(status[code])
        except (KeyError, TypeError, ValueError):
            lines.append(f"{name}: нет данных ({code})")
            continue
        line = f"{name}: {value * scale:.3f} {unit}".strip()
        prev = reading_before(code, now_ts - 12 * 3600)
        if prev:
            line += f" (+{(value - prev[1]) * scale:.3f} {unit} с {time.strftime('%d.%m', time.localtime(prev[0]))})"
        lines.append(line)
    return "\n".join(lines)


def fmt_monthly(status, prev, prev_date, now):
    lines = [f"🗓 Показания на {now:%d.%m.%Y}:"]
    for code, name, scale, unit in SENSORS:
        if code not in status:
            lines.append(f"{name}: нет данных ({code})")
            continue
        try:
            value = float(status[code])
        except (TypeError, ValueError):
            lines.append(f"{name}: {status[code]}")
            continue
        line = f"{name}: {value * scale:.3f} {unit}".strip()
        if prev and code in prev:
            line += f" (+{(value - prev[code]) * scale:.3f} {unit} с {prev_date})".replace("  ", " ")
        lines.append(line)
    if not prev:
        lines.append("\nЭто первый ежемесячный отчёт, расход появится со следующего.")
    return "\n".join(lines)


def fmt_health():
    lines = ["🩺 Состояние"]
    names = {"cloud": "облако Tuya (API)", "local": "локальная сеть", "auto": "локальная сеть, при сбое облако"}
    src = f"Источник показаний: {names[READ_SOURCE]}"
    if READ_SOURCE != "cloud":
        src += f" ({LOCAL_IP}" + (f", протокол {_local['version']}" if _local["version"] else "") + ")"
    lines.append(src)
    if read_state["source"] and read_state["ts"]:
        lines.append(f"Последнее чтение: {'локально' if read_state['source'] == 'local' else 'из облака'}, {fmt_ts(read_state['ts'])}")
    if READ_SOURCE != "cloud" and read_state["error"]:
        lines.append(f"Последняя ошибка локального опроса: {str(read_state['error'])[:160]}")
    last_ok = meta_get("last_ok_ts")
    lines.append(f"Последние показания: {fmt_ts(float(last_ok)) if last_ok else 'ещё не снимались'}")
    if SENSORS and DAILY_READ_HOUR >= 0:
        lines.append(f"Автосъём показаний: ежедневно в {DAILY_READ_HOUR:02d}:00, тревога через {NO_DATA_ALERT_DAYS:g} дн без данных")
    if MONTHLY_REPORT_DAY:
        lines.append(f"Ежемесячный отчёт: {MONTHLY_REPORT_DAY} числа в {MONTHLY_REPORT_HOUR:02d}:00")
    if MOSRU_MODE != "off":
        lines.append(
            f"mos.ru: режим {MOSRU_MODE}, {MOSRU_SUBMIT_DAY} числа в {MOSRU_SUBMIT_HOUR:02d}:00, "
            f"сессия {'есть' if mosru_svc.has_session() else 'нет (/mosru_login)'}, "
            f"последняя передача: {meta_get('mosru_last') or 'ещё не было'}"
        )
    lines.append(f"Вызовов Tuya API в этом месяце: {api_usage()} из {API_MONTHLY_LIMIT}")
    lines.append("⚠️ Активные проблемы: " + ", ".join(sorted(sent_alerts)) if sent_alerts else "Проблем нет")
    return "\n".join(lines)


# --- бот ------------------------------------------------------------------
bot = Bot(TG_TOKEN)
dp = Dispatcher()
dp.message.filter(F.chat.id == CHAT_ID)  # отвечаем только владельцу
dp.callback_query.filter(F.message.chat.id == CHAT_ID)

sent_alerts = {}  # key -> время последнего уведомления


async def notify(text):
    try:
        if NOTIFY_CHAT_ID == CHAT_ID:
            await bot.send_message(CHAT_ID, text, reply_markup=KEYBOARD)
        else:
            await bot.send_message(NOTIFY_CHAT_ID, text)
        return True
    except Exception:
        log.exception("не удалось отправить уведомление")
        return False


async def raise_alert(key, text, repeat=None):
    """Шлёт уведомление и повторяет его не чаще repeat секунд (по умолчанию ALERT_REPEAT_HOURS)."""
    now = time.time()
    last = sent_alerts.get(key)
    if repeat is None:
        repeat = ALERT_REPEAT_HOURS * 3600
    if last is None or now - last >= repeat:
        sent_alerts[key] = now
        await notify(text)


async def clear_alert(key, text):
    if key in sent_alerts:
        del sent_alerts[key]
        await notify(text)


async def answer_long(m: Message, text: str):
    for i in range(0, len(text), 3500):
        await m.answer(text[i : i + 3500], reply_markup=KEYBOARD)


@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Выберите действие:", reply_markup=KEYBOARD)


@dp.message(F.text == BTN_NOW)
async def now(m: Message):
    try:
        status = await get_status()  # облако: 1 вызов API
        record_readings(status)
        await m.answer(fmt_now(status), reply_markup=KEYBOARD)
    except Exception as e:
        log.exception("now")
        await m.answer(f"Ошибка: {e}")


@dp.message(F.text == BTN_HEALTH)
async def health_cmd(m: Message):
    await m.answer(fmt_health(), reply_markup=KEYBOARD)


@dp.message(F.text == BTN_RAW)
async def raw(m: Message):
    try:
        data = await get_status()
        where = "локально, без API" if read_state["source"] == "local" else "облако"
        await answer_long(m, f"🔧 Все DP ({where}):\n" + json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        log.exception("raw")
        await m.answer(f"Ошибка: {e}")


@dp.message(Command("lan"))
async def lan_cmd(m: Message):
    await m.answer("Проверяю связь с устройством по локальной сети, это займёт до минуты…")
    try:
        lines = await asyncio.to_thread(_lan_diag_sync)
    except Exception as e:
        log.exception("lan")
        lines = [f"Ошибка диагностики: {e}"]
    await answer_long(m, "🔎 Диагностика LAN\n" + "\n".join(lines))


@dp.message(Command("diag"))
async def diag(m: Message):
    try:
        data = await asyncio.to_thread(_diag_sync)
        await answer_long(m, "🧪 Диагностика (облако, 4 вызова API):\n" + json.dumps(data, ensure_ascii=False, indent=1))
    except Exception as e:
        log.exception("diag")
        await m.answer(f"Ошибка: {e}")


# --- ежемесячный отчёт (облако, 1 вызов в месяц) -----------------------------
def monthly_due(now):
    if not MONTHLY_REPORT_DAY or not SENSORS:
        return False
    day = min(MONTHLY_REPORT_DAY, calendar.monthrange(now.year, now.month)[1])
    if (now.day, now.hour) < (day, MONTHLY_REPORT_HOUR):
        return False
    return meta_get("monthly_last") != now.strftime("%Y-%m")


async def maybe_send_monthly():
    now_dt = datetime.now()
    if not monthly_due(now_dt):
        return
    status = await get_status()
    record_readings(status)
    prev = json.loads(meta_get("monthly_snapshot") or "null")
    prev_date = meta_get("monthly_snapshot_date") or ""
    if await notify(fmt_monthly(status, prev, prev_date, now_dt)):
        snap = {}
        for code, *_ in SENSORS:
            try:
                snap[code] = float(status[code])
            except (KeyError, TypeError, ValueError):
                pass
        meta_set("monthly_snapshot", json.dumps(snap))
        meta_set("monthly_snapshot_date", now_dt.strftime("%d.%m.%Y"))
        meta_set("monthly_last", now_dt.strftime("%Y-%m"))
        log.info("Ежемесячный отчёт отправлен")


async def monthly_loop():
    while True:
        try:
            await maybe_send_monthly()
        except Exception:
            log.exception("monthly")
        await asyncio.sleep(600)


# --- автосъём раз в сутки и тревога «нет данных» ------------------------------
async def check_no_data():
    if not SENSORS or DAILY_READ_HOUR < 0:
        return
    last_ok = float(meta_get("last_ok_ts") or meta_get("started_at") or time.time())
    gap = time.time() - last_ok
    if gap >= NO_DATA_ALERT_DAYS * 86400:
        await raise_alert(
            "no_data",
            f"📭 Нет показаний счётчиков уже {fmt_dur(gap)} (последние: {fmt_ts(last_ok)}).\n"
            "Проверьте устройство, связь с Tuya Cloud (ключи, триал IoT Core) и коды в SENSORS.",
        )
    else:
        await clear_alert("no_data", "✅ Показания счётчиков снова поступают.")


async def daily_loop():
    last_attempt = 0.0
    while True:
        try:
            now_dt = datetime.now()
            today = now_dt.strftime("%Y-%m-%d")
            if (
                SENSORS
                and DAILY_READ_HOUR >= 0
                and now_dt.hour >= DAILY_READ_HOUR
                and meta_get("daily_last") != today
                and time.time() - last_attempt >= DAILY_RETRY_MINUTES * 60
            ):
                last_attempt = time.time()
                try:
                    status = await get_status()
                    if record_readings(status):
                        meta_set("daily_last", today)
                        log.info("Суточное снятие показаний выполнено")
                        if DAILY_REPORT:
                            await notify(fmt_daily(status))
                    else:
                        log.warning("В ответе Tuya нет показаний из SENSORS")
                except Exception as e:
                    log.warning("Суточное снятие не удалось: %s", e)
            await check_no_data()
        except Exception:
            log.exception("daily_loop")
        await asyncio.sleep(300)


# --- передача показаний на mos.ru -----------------------------------------------
mosru_state = {"wait": None, "pending": None, "task": None, "places": None}


def mosru_error_text(e):
    if isinstance(e, mosru.NeedLogin):
        return "🔑 Нет действующей сессии mos.ru. Выполните /mosru_login."
    if isinstance(e, (mosru.NeedSetup, mosru.MosRuError)):
        return f"⚠️ {e}"
    if isinstance(e, MosRuTemporaryError):
        return "⏳ mos.ru временно недоступен, попробуйте позже."
    return f"⚠️ Ошибка mos.ru: {e}"


def neptun_values(status):
    """Показания Neptun -> целые м³ для портала (портал принимает только целые)."""
    scales = {code: scale for code, _, scale, _ in SENSORS}
    if MOSRU_COLD_CODE == MOSRU_HOT_CODE:
        raise mosru.MosRuError("MOSRU_COLD_CODE и MOSRU_HOT_CODE должны различаться")
    out = {}
    for label, code in (("cold", MOSRU_COLD_CODE), ("hot", MOSRU_HOT_CODE)):
        if code not in scales:
            raise mosru.MosRuError(f"Код {code} не найден в SENSORS")
        if code not in status:
            raise mosru.MosRuError(f"В ответе Tuya нет показания {code}")
        try:
            out[label] = mosru.normalized_reading(float(status[code]) * scales[code])
        except (TypeError, ValueError, MosRuApiError) as e:
            raise mosru.MosRuError(f"Недопустимое показание {code}: {status[code]}") from e
    return out


async def mosru_prompt(chat_id):
    """Показывает, что будет отправлено, и просит подтверждение кнопкой."""
    try:
        status = await get_status()
        record_readings(status)
        values = neptun_values(status)
        rows = await asyncio.to_thread(mosru_svc.plan, values)
    except Exception as e:
        if not isinstance(e, (mosru.NeedLogin, mosru.NeedSetup, mosru.MosRuError, MosRuTemporaryError)):
            log.exception("mosru_prompt")
        await bot.send_message(chat_id, mosru_error_text(e))
        return
    text = "📤 Передача показаний на mos.ru\n" + mosru.describe_rows(rows)
    if all(r["status"] == "already" for r in rows):
        meta_set("mosru_last", datetime.now().strftime("%Y-%m"))
        await bot.send_message(chat_id, text + "\n\nЗа этот месяц показания уже внесены.")
        return
    if any(r["status"] in ("closed", "anomaly", "no_prev") for r in rows):
        await bot.send_message(chat_id, text + "\n\nОтправка недоступна, пока не устранена причина выше.")
        return
    mosru_state["pending"] = {"values": values, "ts": time.time()}
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ Отправить", callback_data="mosru_send"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="mosru_cancel"),
        ]]
    )
    await bot.send_message(chat_id, text + "\n\nОтправить эти показания?", reply_markup=kb)


async def mosru_do_submit(values, from_auto=False):
    try:
        rows = await asyncio.to_thread(mosru_svc.submit, values)
    except Exception as e:
        if not isinstance(e, (mosru.NeedLogin, mosru.NeedSetup, mosru.MosRuError, MosRuTemporaryError)):
            log.exception("mosru_submit")
        text = mosru_error_text(e)
        if from_auto:
            await raise_alert("mosru_submit", "📤 mos.ru: не удалось передать показания.\n" + text, repeat=86400)
        else:
            await notify("📤 mos.ru: не удалось передать показания.\n" + text)
        return False
    text = "📤 Показания на mos.ru:\n" + mosru.describe_rows(rows)
    done = all(r.get("outcome") in ("submitted", "already") for r in rows)
    if done:
        meta_set("mosru_last", datetime.now().strftime("%Y-%m"))
        sent_alerts.pop("mosru_submit", None)
    else:
        text += "\n\n⚠️ Передача завершилась частично. Автоматически повторяться не буду, проверьте портал."
    await notify(text)
    return done


@dp.message(F.text == BTN_MOSRU)
async def mosru_button(m: Message):
    if MOSRU_MODE == "off":
        await m.answer("Передача на mos.ru выключена (MOSRU_MODE=off).")
        return
    await mosru_prompt(m.chat.id)


@dp.callback_query(F.data == "mosru_send")
async def mosru_cb_send(cb: CallbackQuery):
    pending = mosru_state["pending"]
    if not pending or time.time() - pending["ts"] > 1800:
        mosru_state["pending"] = None
        await cb.answer("Предложение устарело, нажмите «📤 mos.ru» заново.", show_alert=True)
        return
    mosru_state["pending"] = None
    await cb.answer("Отправляю…")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await mosru_do_submit(pending["values"])


@dp.callback_query(F.data == "mosru_cancel")
async def mosru_cb_cancel(cb: CallbackQuery):
    mosru_state["pending"] = None
    await cb.answer("Отменено")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


# --- вход в mos.ru по QR ---------------------------------------------------------
async def send_qr(chat_id, link):
    buf = io.BytesIO()
    qrcode.make(link).save(buf, format="PNG")
    await bot.send_photo(
        chat_id,
        BufferedInputFile(buf.getvalue(), "mosru_qr.png"),
        caption=(
            "Отсканируйте QR в приложении «mos.ru» или «Госуслуги Москвы» (кнопка сканера).\n"
            "Если открыли чат на телефоне, можно просто перейти по ссылке:\n" + link
        ),
    )


async def mosru_choose_place(chat_id, place):
    counters, cold, hot = await asyncio.to_thread(mosru_svc.select_place, place)
    _, cold_id, hot_id = mosru_svc.ids()
    lines = [f"✅ Вход выполнен. Квартира: {mosru.place_title(place)}"]
    if cold_id and hot_id:
        lines.append("Счётчики ХВС и ГВС определены. Проверьте командой /mosru_status.")
    else:
        lines.append("Не удалось однозначно определить счётчики. Найденные на портале:")
        for c in counters:
            lines.append(f"• {c['type']} №{c['name']}, id {c['id']}")
        lines.append("Задайте MOSRU_COLD_ID и MOSRU_HOT_ID в .env и перезапустите бота.")
    await bot.send_message(chat_id, "\n".join(lines))
    await clear_alert("mosru_auth", "✅ Сессия mos.ru восстановлена.")


async def mosru_finish_login(chat_id):
    places = await asyncio.to_thread(mosru_svc.login_finish)
    if not places:
        await bot.send_message(chat_id, "В профиле «Электронный дом» нет квартир. Добавьте квартиру на ed.mos.ru и повторите /mosru_login.")
        return
    wanted = os.environ.get("MOSRU_USER_PLACE_ID", "").strip()
    chosen = next((p for p in places if p["user_place_id"] == wanted), None) if wanted else None
    if chosen is None and len(places) == 1:
        chosen = places[0]
    if chosen is not None:
        await mosru_choose_place(chat_id, chosen)
        return
    mosru_state["places"] = places
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=mosru.place_title(p)[:60], callback_data=f"mosru_place:{i}")]
                         for i, p in enumerate(places)]
    )
    await bot.send_message(chat_id, "Выберите квартиру:", reply_markup=kb)


@dp.callback_query(F.data.startswith("mosru_place:"))
async def mosru_cb_place(cb: CallbackQuery):
    places = mosru_state["places"] or []
    try:
        place = places[int(cb.data.split(":", 1)[1])]
    except (ValueError, IndexError):
        await cb.answer("Список устарел, выполните /mosru_login", show_alert=True)
        return
    await cb.answer()
    mosru_state["places"] = None
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    try:
        await mosru_choose_place(cb.message.chat.id, place)
    except Exception as e:
        log.exception("select_place")
        await bot.send_message(cb.message.chat.id, mosru_error_text(e))


async def mosru_login_flow(chat_id):
    try:
        qr = await asyncio.to_thread(mosru_svc.login_start)
        await send_qr(chat_id, qr["link"])
        confirm_sent = False
        for _ in range(150):  # до 5 минут
            await asyncio.sleep(2)
            cmd = await asyncio.to_thread(mosru_svc.login_poll)
            if cmd == "needComplete":
                state = await asyncio.to_thread(mosru_svc.login_complete)
                if state == "totp_required":
                    mosru_state["wait"] = "totp"
                    await bot.send_message(chat_id, "Введите 6-значный код из приложения-аутентификатора (одним сообщением).")
                elif state == "sms_required":
                    mosru_state["wait"] = "sms"
                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📩 Прислать SMS-код", callback_data="mosru_sms")]])
                    await bot.send_message(
                        chat_id,
                        "mos.ru просит код подтверждения. Введите уже полученный 6-значный код или запросите SMS кнопкой.",
                        reply_markup=kb,
                    )
                else:
                    await mosru_finish_login(chat_id)
                return
            if cmd == "askForConfirm" and not confirm_sent:
                confirm_sent = True
                await bot.send_message(chat_id, "Подтвердите вход в приложении mos.ru.")
            elif cmd == "needRefresh":
                qr = await asyncio.to_thread(mosru_svc.login_refresh)
                await send_qr(chat_id, qr["link"])
        await bot.send_message(chat_id, "Время ожидания QR истекло. Повторите /mosru_login.")
    except MosRuAuthError as e:
        await bot.send_message(chat_id, f"Вход не завершён: {e}")
    except Exception as e:
        log.exception("mosru_login")
        await bot.send_message(chat_id, f"Ошибка входа в mos.ru: {e}")
    finally:
        mosru_state["task"] = None


@dp.message(Command("mosru_login"))
async def mosru_login_cmd(m: Message):
    task = mosru_state["task"]
    if task and not task.done():
        await m.answer("Вход уже выполняется, отсканируйте последний QR.")
        return
    mosru_state["wait"] = None
    mosru_state["task"] = asyncio.create_task(mosru_login_flow(m.chat.id))


@dp.callback_query(F.data == "mosru_sms")
async def mosru_cb_sms(cb: CallbackQuery):
    try:
        await asyncio.to_thread(mosru_svc.login_request_sms)
        await cb.answer("SMS запрошено")
        await bot.send_message(cb.message.chat.id, "Введите код из SMS одним сообщением.")
    except Exception as e:
        await cb.answer(f"Не удалось запросить SMS: {e}"[:190], show_alert=True)


@dp.message(F.text.regexp(r"^\s*\d{6}\s*$"))
async def mosru_code(m: Message):
    wait = mosru_state["wait"]
    if not wait:
        return
    submit = mosru_svc.login_submit_totp if wait == "totp" else mosru_svc.login_submit_sms
    try:
        await asyncio.to_thread(submit, m.text.strip())
    except MosRuAuthError as e:
        await m.answer(f"{e}. Введите код ещё раз или начните заново: /mosru_login")
        return
    except Exception as e:
        log.exception("mosru_code")
        await m.answer(f"Ошибка: {e}")
        return
    mosru_state["wait"] = None
    try:
        await m.delete()  # код одноразовый, но в чате ему делать нечего
    except Exception:
        pass
    try:
        await mosru_finish_login(m.chat.id)
    except Exception as e:
        log.exception("mosru_finish")
        await m.answer(mosru_error_text(e))


@dp.message(Command("mosru_status"))
async def mosru_status_cmd(m: Message):
    try:
        info = await asyncio.to_thread(mosru_svc.get_info)
    except Exception as e:
        await m.answer(mosru_error_text(e))
        return
    _, cold_id, hot_id = mosru_svc.ids()
    lines = ["📋 Счётчики на портале:"]
    for cid, inf in info.items():
        mark = " (ХВС для бота)" if cid == cold_id else " (ГВС для бота)" if cid == hot_id else ""
        lines.append(
            f"• {inf['type']} №{inf['number']}{mark}: {inf['current_reading']} м³, период {inf['reading_period']}, "
            f"приём показаний {'закрыт' if inf['readonly'] else 'открыт'}, поверка до {inf['inspection_date']}"
        )
    await answer_long(m, "\n".join(lines))


@dp.message(Command("mosru_logout"))
async def mosru_logout_cmd(m: Message):
    mosru_svc.logout()
    await m.answer("Сессия mos.ru удалена из бота. Для повторного входа: /mosru_login")


# --- фоновые задачи mos.ru -----------------------------------------------------------
def mosru_due(now):
    if MOSRU_MODE == "off" or not MOSRU_SUBMIT_DAY:
        return False
    day = min(MOSRU_SUBMIT_DAY, calendar.monthrange(now.year, now.month)[1])
    if (now.day, now.hour) < (day, MOSRU_SUBMIT_HOUR):
        return False
    return meta_get("mosru_last") != now.strftime("%Y-%m")


async def mosru_loop():
    """В назначенный день: auto = отправить сразу, confirm = спросить кнопкой (напоминание раз в сутки)."""
    last_attempt = 0.0
    while True:
        try:
            now_dt = datetime.now()
            if mosru_due(now_dt):
                if MOSRU_MODE == "auto" and time.time() - last_attempt >= 3600:
                    last_attempt = time.time()
                    try:
                        status = await get_status()
                        record_readings(status)
                        await mosru_do_submit(neptun_values(status), from_auto=True)
                    except Exception as e:
                        log.warning("mos.ru auto: %s", e)
                        await raise_alert("mosru_submit", "📤 mos.ru: не удалось передать показания.\n" + mosru_error_text(e), repeat=86400)
                elif MOSRU_MODE == "confirm":
                    prompted = float(meta_get("mosru_prompt_ts") or 0)
                    if time.time() - prompted >= 86400:
                        meta_set("mosru_prompt_ts", str(time.time()))
                        await mosru_prompt(CHAT_ID)
        except Exception:
            log.exception("mosru_loop")
        await asyncio.sleep(600)


async def mosru_keepalive_loop():
    """Раз в 45 минут трогаем портал, чтобы сессия не протухла между отправками (как в HA-интеграции)."""
    await asyncio.sleep(60)
    while True:
        try:
            if MOSRU_MODE != "off":
                if mosru_svc.has_session():
                    try:
                        await asyncio.to_thread(mosru_svc.get_info)
                        await clear_alert("mosru_auth", "✅ Сессия mos.ru активна.")
                    except mosru.NeedLogin:
                        await raise_alert("mosru_auth", "🔑 Сессия mos.ru истекла. Выполните /mosru_login, иначе показания не уйдут.", repeat=86400)
                    except (mosru.NeedSetup, MosRuTemporaryError, MosRuApiError) as e:
                        log.warning("mos.ru keepalive: %s", e)
                elif meta_get("mosru_user_place_id"):
                    await raise_alert("mosru_auth", "🔑 Сессия mos.ru недоступна. Выполните /mosru_login.", repeat=86400)
        except Exception:
            log.exception("mosru_keepalive")
        await asyncio.sleep(45 * 60)


# --- служебное -------------------------------------------------------------
class UpdateLogger(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            inner = getattr(event, event.event_type, None)
            chat = getattr(inner, "chat", None)
            log.info(
                "Обновление: type=%s chat.id=%s chat.type=%s",
                event.event_type, getattr(chat, "id", None), getattr(chat, "type", None),
            )
        except Exception:
            log.exception("UpdateLogger")
        return await handler(event, data)


dp.update.outer_middleware(UpdateLogger())

fallback = Router()


@fallback.message()
async def unknown_chat(m: Message):
    if m.chat.id == CHAT_ID:
        return  # владелец написал что-то, для чего нет обработчика
    log.warning(
        "Сообщение из чужого чата: chat.id=%s chat.type=%s user.id=%s (ожидался %s)",
        m.chat.id, m.chat.type, m.from_user.id if m.from_user else None, CHAT_ID,
    )
    await m.answer(f"Доступ закрыт. id этого чата: {m.chat.id}\nОжидается: {CHAT_ID}")


dp.include_router(fallback)


async def main():
    log.info("Старт. Владелец chat_id=%s, уведомления в %s", CHAT_ID, NOTIFY_CHAT_ID)
    if not meta_get("started_at"):
        meta_set("started_at", str(time.time()))
    asyncio.create_task(daily_loop())
    asyncio.create_task(mosru_loop())
    asyncio.create_task(mosru_keepalive_loop())
    asyncio.create_task(monthly_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
