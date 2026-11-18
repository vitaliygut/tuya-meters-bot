"""
tuya-meters-bot

Облако Tuya (API): только показания счётчиков (по кнопке и ежемесячный отчёт).
Локальная сеть (tinytuya): алерты (протечка, краны, связь с устройством), без лимитов API.
"""
import asyncio
import calendar
import io
import json
import logging
import os
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
API_ID = os.environ["TUYA_ACCESS_ID"]
API_SECRET = os.environ["TUYA_ACCESS_SECRET"]
DEVICE_ID = os.environ["TUYA_DEVICE_ID"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])  # чат, где работают кнопки
NOTIFY_CHAT_ID = int(os.environ.get("NOTIFY_CHAT_ID") or CHAT_ID)  # куда слать алерты и отчёты

DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")
API_MONTHLY_LIMIT = int(os.environ.get("API_MONTHLY_LIMIT", "26000"))  # только для отображения

# локальный режим (алерты)
LOCAL_IP = os.environ.get("TUYA_LOCAL_IP", "").strip()
LOCAL_KEY_ENV = os.environ.get("TUYA_LOCAL_KEY", "").strip()  # пусто = взять из облака один раз
LOCAL_VERSION = os.environ.get("TUYA_LOCAL_VERSION", "auto").strip()  # auto, 3.3, 3.4, 3.5
LOCAL_POLL_INTERVAL = int(os.environ.get("LOCAL_POLL_INTERVAL", "15"))
OFFLINE_ALERT_MINUTES = float(os.environ.get("OFFLINE_ALERT_MINUTES", "10"))
ALERT_REPEAT_HOURS = float(os.environ.get("ALERT_REPEAT_HOURS", "24"))
LEAK_REPEAT_MINUTES = float(os.environ.get("LEAK_REPEAT_MINUTES", "30"))

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


def parse_value(raw):
    """Строка из .env -> значение DP: true/false, null, число или текст."""
    low = raw.strip().lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none", ""):
        return None
    try:
        return int(low)
    except ValueError:
        pass
    try:
        return float(low)
    except ValueError:
        return raw.strip()


def parse_watch(raw):
    """code:Название:норма,..."""
    out = []
    for part in filter(None, (p.strip() for p in raw.split(","))):
        it = part.split(":")
        out.append((it[0], it[1] if len(it) > 1 else it[0], parse_value(it[2]) if len(it) > 2 else None))
    return out


SENSORS = parse_sensors(os.environ.get("SENSORS", ""))

DEFAULT_WATCH = (
    "status_wire_line:Проводные датчики:0,"
    "status_rf_sensor:Беспроводные датчики:null,"
    "notifications:Уведомления Neptun:0,"
    "status_crane_group_1:Кран группа 1:true,"
    "status_crane_group_2:Кран группа 2:true"
)
LEAK_WATCH = parse_watch(os.environ.get("LEAK_WATCH", DEFAULT_WATCH))

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
cloud = tinytuya.Cloud(apiRegion=REGION, apiKey=API_ID, apiSecret=API_SECRET, apiDeviceID=DEVICE_ID)
_cloud_lock = threading.Lock()


def cloud_get(url):
    """Любой запрос к облаку: под замком и со счётчиком вызовов."""
    with _cloud_lock:
        count_api_call()
        return cloud.cloudrequest(url)


def load_dp_map():
    """code -> числовой id DP (нужен, чтобы читать локальные данные по кодам)."""
    try:
        return json.loads(meta_get("dp_map") or "{}")
    except ValueError:
        return {}


def save_dp_map(new):
    current = load_dp_map()
    current.update(new)
    meta_set("dp_map", json.dumps(current))


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
    ids = {p["code"]: str(p["dp_id"]) for p in props if p.get("code") and p.get("dp_id") is not None}
    if ids:
        save_dp_map(ids)
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


async def get_status():
    return await asyncio.to_thread(_get_status_sync)


def _fetch_dp_map_sync():
    """Один раз узнаём соответствие code -> dp_id (shadow, затем specifications)."""
    _try_source("shadow")
    if load_dp_map():
        return
    res = cloud_get(f"/v1.0/devices/{DEVICE_ID}/specifications")
    if res and res.get("success"):
        result = res.get("result") or {}
        ids = {}
        for item in (result.get("functions") or []) + (result.get("status") or []):
            if item.get("code") and item.get("dp_id") is not None:
                ids[item["code"]] = str(item["dp_id"])
        if ids:
            save_dp_map(ids)


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


# --- локальный доступ (алерты) --------------------------------------------
_local = {"dev": None, "version": None, "key_fetched": 0.0}
local_state = {"ts": None, "dps": None, "fail_since": None, "fails": 0, "error": None}


def _refresh_local_key_sync():
    res = cloud_get(f"/v1.0/devices/{DEVICE_ID}")
    key = ((res or {}).get("result") or {}).get("local_key") if res and res.get("success") else None
    if not key:
        raise RuntimeError(f"не удалось получить local_key из облака: {res}")
    meta_set("local_key", key)
    _local["key_fetched"] = time.time()
    return key


def _get_local_key_sync():
    return LOCAL_KEY_ENV or meta_get("local_key") or _refresh_local_key_sync()


def _make_device(version, key):
    d = tinytuya.Device(DEVICE_ID, LOCAL_IP, key, version=float(version))
    for name, arg in (("set_socketTimeout", 5), ("set_socketRetryLimit", 1)):
        try:
            getattr(d, name)(arg)
        except Exception:
            pass
    return d


def _local_status_sync():
    key = _get_local_key_sync()
    if _local["dev"] is None:
        if _local["version"]:
            versions = [_local["version"]]
        elif LOCAL_VERSION != "auto":
            versions = [LOCAL_VERSION]
        else:
            versions = ["3.4", "3.3", "3.5", "3.2"]
        last = None
        for v in versions:
            d = _make_device(v, key)
            res = d.status()
            if res and "dps" in res:
                _local.update(dev=d, version=v)
                log.info("Локальное подключение: %s, протокол %s", LOCAL_IP, v)
                return res["dps"]
            last = res
        raise RuntimeError(f"локальный опрос не удался: {last}")
    res = _local["dev"].status()
    if not res or "dps" not in res:
        _local["dev"] = None
        raise RuntimeError(f"локальный опрос не удался: {res}")
    return res["dps"]


def to_codes(dps):
    """Локальные ключи ('1', '101') -> коды DP ('counter7'), если соответствие известно."""
    rev = {v: k for k, v in load_dp_map().items()}
    return {rev.get(str(k), str(k)): v for k, v in dps.items()}


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
    if not LOCAL_IP:
        lines.append("Локальный режим выключен (не задан TUYA_LOCAL_IP), алертов нет")
    elif local_state["ts"] and not local_state["fail_since"]:
        lines.append(f"Связь по LAN: 🟢 ответ {fmt_dur(time.time() - local_state['ts'])} назад")
        lines.append(f"Локальный опрос: {LOCAL_IP}, протокол {_local['version']}, каждые {LOCAL_POLL_INTERVAL} с")
    elif local_state["fail_since"]:
        lines.append(f"Связь по LAN: 🔴 нет ответа уже {fmt_dur(time.time() - local_state['fail_since'])}")
        if local_state["error"]:
            lines.append(f"Ошибка: {str(local_state['error'])[:200]}")
    else:
        lines.append("Связь по LAN: ожидание первого ответа")
    if MONTHLY_REPORT_DAY:
        lines.append(f"Ежемесячный отчёт: {MONTHLY_REPORT_DAY} числа в {MONTHLY_REPORT_HOUR:02d}:00")
    if MOSRU_MODE != "off":
        lines.append(
            f"mos.ru: режим {MOSRU_MODE}, {MOSRU_SUBMIT_DAY} числа в {MOSRU_SUBMIT_HOUR:02d}:00, "
            f"сессия {'есть' if mosru_svc.has_session() else 'нет (/mosru_login)'}, "
            f"последняя передача: {meta_get('mosru_last') or 'ещё не было'}"
        )
    last_ok = meta_get("last_ok_ts")
    lines.append(f"Последние показания: {fmt_ts(float(last_ok)) if last_ok else 'ещё не снимались'}")
    if SENSORS and DAILY_READ_HOUR >= 0:
        lines.append(f"Автосъём показаний: ежедневно в {DAILY_READ_HOUR:02d}:00, тревога через {NO_DATA_ALERT_DAYS:g} дн без данных")
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
        if local_state["dps"] is not None:
            data = to_codes(local_state["dps"])
            title = "🔧 Все DP (локально, без API):\n"
        else:
            data = await get_status()
            title = "🔧 Все DP (облако):\n"
        await answer_long(m, title + json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        log.exception("raw")
        await m.answer(f"Ошибка: {e}")


@dp.message(Command("diag"))
async def diag(m: Message):
    try:
        data = await asyncio.to_thread(_diag_sync)
        await answer_long(m, "🧪 Диагностика (облако, 4 вызова API):\n" + json.dumps(data, ensure_ascii=False, indent=1))
    except Exception as e:
        log.exception("diag")
        await m.answer(f"Ошибка: {e}")


# --- алерты по локальной сети -------------------------------------------------
async def check_leaks(status):
    for code, name, norm in LEAK_WATCH:
        if code not in status:
            continue
        value = status[code]
        key = f"leak:{code}"
        if value != norm:
            await raise_alert(
                key,
                f"🚨 {name}: {fmt_val(value)} (в норме {fmt_val(norm)}).\n"
                "Возможна протечка или сработала защита. Проверьте датчики и краны!",
                repeat=LEAK_REPEAT_MINUTES * 60,
            )
        else:
            await clear_alert(key, f"✅ {name}: снова в норме ({fmt_val(value)}).")


async def local_loop():
    if not LOCAL_IP:
        log.warning("TUYA_LOCAL_IP не задан: локальные алерты выключены")
        return
    last_map_try = 0.0
    while True:
        now_ts = time.time()
        try:
            if not load_dp_map() and now_ts - last_map_try > 600:
                last_map_try = now_ts
                try:
                    await asyncio.to_thread(_fetch_dp_map_sync)
                except Exception:
                    log.exception("не удалось получить соответствие DP")
            dps = await asyncio.to_thread(_local_status_sync)
            local_state.update(ts=now_ts, dps=dps, fails=0, error=None)
            if local_state["fail_since"] is not None:
                local_state["fail_since"] = None
                await clear_alert("offline", "🟢 Связь с устройством по локальной сети восстановлена.")
            await check_leaks(to_codes(dps))
        except Exception as e:
            log.warning("Локальный опрос: %s", e)
            local_state["fails"] += 1
            local_state["error"] = str(e)
            if local_state["fail_since"] is None:
                local_state["fail_since"] = now_ts
            down = now_ts - local_state["fail_since"]
            if down >= OFFLINE_ALERT_MINUTES * 60:
                await raise_alert(
                    "offline",
                    f"🔴 Нет связи с устройством по локальной сети уже {fmt_dur(down)}.\n"
                    f"Проверьте питание и Wi-Fi модуля, IP ({LOCAL_IP}), local key.",
                )
            # local_key мог смениться (устройство переподключали): раз в час берём новый из облака
            if not LOCAL_KEY_ENV and local_state["fails"] >= 5 and now_ts - _local["key_fetched"] > 3600:
                try:
                    await asyncio.to_thread(_refresh_local_key_sync)
                    _local["dev"] = None
                except Exception:
                    log.exception("обновление local_key")
        await asyncio.sleep(LOCAL_POLL_INTERVAL)


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
    asyncio.create_task(local_loop())
    asyncio.create_task(daily_loop())
    asyncio.create_task(mosru_loop())
    asyncio.create_task(mosru_keepalive_loop())
    asyncio.create_task(monthly_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
