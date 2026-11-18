"""
Передача показаний на mos.ru («Электронный дом», ed.mos.ru).

Работа с порталом построена на клиенте из проекта kostinos/hass-mosru-water (MIT),
файл mosru_api.py. Здесь только обвязка для бота: хранение сессии, проверки перед
отправкой и сама отправка.
"""
import json
import logging
import threading
import time
from datetime import datetime

from mosru_api import (
    MosRuAlreadySubmittedError,
    MosRuApiError,
    MosRuAuthError,
    MosRuClient,
    MosRuTemporaryError,
    normalized_reading,
    pick_counters,
    place_label,
)

log = logging.getLogger("mosru")

ED_REFRESH_SECONDS = 40 * 60  # как в оригинальной интеграции: куки ed.mos.ru живут около часа
WRITE_COOLDOWN = 60           # не чаще одной записи в минуту
LABELS = {"cold": "Холодная", "hot": "Горячая"}


class NeedLogin(Exception):
    """Нет сессии или она истекла: нужен вход по QR (/mosru_login)."""


class NeedSetup(Exception):
    """Вход есть, но не выбрана квартира или не определены счётчики."""


class MosRuError(Exception):
    """Понятная пользователю причина, почему отправка невозможна."""


def current_month():
    return datetime.now().strftime("%Y-%m")


class MosRuService:
    def __init__(self, meta_get, meta_set, *, place_id="", cold_id="", hot_id="", max_delta=100):
        self._get, self._set = meta_get, meta_set
        self._env_place, self._env_cold, self._env_hot = place_id, cold_id, hot_id
        self.max_delta = max_delta
        self._client = None
        self._ed_at = None
        self._last_write = 0.0
        self._lock = threading.Lock()
        self._login = None  # клиент незавершённого QR-входа

    # --- сессия -------------------------------------------------------------
    def has_session(self):
        return bool(self._get("mosru_cookies"))

    def _save_cookies(self, client):
        self._set("mosru_cookies", json.dumps(client.get_session_cookies()))

    def _invalidate(self):
        self._client = None
        self._ed_at = None
        self._set("mosru_cookies", "")

    def _get_client(self):
        if self._client is None:
            raw = self._get("mosru_cookies")
            if not raw:
                raise NeedLogin("Нет сессии mos.ru")
            client = MosRuClient()
            client.restore_session(json.loads(raw))
            self._client = client
        return self._client

    def ids(self):
        place = self._env_place or self._get("mosru_user_place_id") or ""
        cold = self._env_cold or self._get("mosru_cold_id") or ""
        hot = self._env_hot or self._get("mosru_hot_id") or ""
        return place, cold, hot

    def _prepare(self):
        """Живая сессия ed.mos.ru и userPlaceId. Вызывать под замком."""
        client = self._get_client()
        try:
            if not client.try_refresh_acst():
                self._invalidate()
                raise NeedLogin("Сессия mos.ru истекла")
            if self._ed_at is None or time.monotonic() - self._ed_at >= ED_REFRESH_SECONDS:
                client.authorize_ed()
                self._ed_at = time.monotonic()
        except MosRuAuthError as err:
            self._invalidate()
            raise NeedLogin(str(err)) from err
        place, cold, hot = self.ids()
        if not place:
            raise NeedSetup("Не выбрана квартира: выполните /mosru_login")
        if not cold or not hot:
            raise NeedSetup("Не определены счётчики ХВС и ГВС: укажите MOSRU_COLD_ID и MOSRU_HOT_ID")
        self._save_cookies(client)
        return client, place

    def _acquire(self):
        if not self._lock.acquire(timeout=60):
            raise MosRuTemporaryError("Операция с mos.ru уже выполняется")

    # --- чтение -------------------------------------------------------------
    def get_info(self):
        """{counter_id: {type, number, current_reading, reading_period, readonly, ...}}"""
        self._acquire()
        try:
            client, place = self._prepare()
            try:
                info = client.get_device_info(place)
            except MosRuAuthError:
                # чтение безопасно повторить один раз после обновления сессии
                self._ed_at = None
                client, place = self._prepare()
                info = client.get_device_info(place)
            self._save_cookies(client)
            return info
        except MosRuAuthError as err:
            self._invalidate()
            raise NeedLogin(str(err)) from err
        finally:
            self._lock.release()

    # --- проверка и отправка -----------------------------------------------
    def check(self, values, info):
        """Сверяет значения с порталом. values: {"cold": int, "hot": int}."""
        _, cold_id, hot_id = self.ids()
        rows = []
        for label, cid in (("cold", cold_id), ("hot", hot_id)):
            inf = info.get(cid)
            if not inf:
                raise MosRuError(f"Счётчик {LABELS[label].lower()} (id {cid}) не найден в квартире")
            value = values[label]
            period = str(inf.get("reading_period") or "")
            row = {
                "label": label, "counter_id": cid, "value": value,
                "number": inf.get("number"), "period": period, "prev": None, "status": "ok",
            }
            if inf.get("readonly", True):
                row["status"] = "already" if period[:7] == current_month() else "closed"
            try:
                prev = float(inf.get("current_reading"))
                normalized_reading(prev)
                row["prev"] = prev
            except (TypeError, ValueError, MosRuApiError):
                if row["status"] == "ok":
                    row["status"] = "no_prev"
            if row["status"] == "ok" and (value < row["prev"] or value - row["prev"] > self.max_delta):
                row["status"] = "anomaly"
            rows.append(row)
        return rows

    def plan(self, values):
        return self.check(values, self.get_info())

    def submit(self, values):
        """Отправляет показания. Ничего не удаляет и не перезаписывает."""
        self._acquire()
        try:
            if time.monotonic() - self._last_write < WRITE_COOLDOWN:
                raise MosRuError("Подождите минуту перед повторной отправкой")
            client, place = self._prepare()
            try:
                info = client.get_device_info(place)
            except MosRuAuthError as err:
                self._invalidate()
                raise NeedLogin(str(err)) from err
            rows = self.check(values, info)
            blocking = [r for r in rows if r["status"] in ("closed", "anomaly", "no_prev")]
            if blocking:
                raise MosRuError("Отправка отменена, ничего не записано:\n" + describe_rows(rows))
            self._last_write = time.monotonic()
            sent_any = False
            for row in rows:
                if row["status"] == "already":
                    row["outcome"] = "already"
                    continue
                try:
                    client.send_reading(place, row["counter_id"], row["value"])
                    row["outcome"] = "submitted"
                    sent_any = True
                except MosRuAlreadySubmittedError:
                    row["outcome"] = "already"
                except (MosRuAuthError, MosRuApiError) as err:
                    row["outcome"] = "error"
                    row["error"] = str(err)
                    if isinstance(err, MosRuAuthError):
                        self._invalidate()
                    if not sent_any and not isinstance(err, MosRuAuthError):
                        # ничего не записано: можно честно сообщить об ошибке
                        raise MosRuError(f"Портал отклонил запись: {err}") from err
                    # часть уже ушла: повторять автоматически нельзя, чтобы не получить дубль
                    break
            self._save_cookies(client) if self._client else None
            return rows
        finally:
            self._lock.release()

    # --- вход по QR ---------------------------------------------------------
    def login_start(self):
        self._login = MosRuClient()
        return self._login.start_qr_session()

    def login_poll(self):
        return self._login.poll_qr()

    def login_refresh(self):
        return self._login.refresh_qr()

    def login_complete(self):
        return self._login.complete_qr_auth()

    def login_request_sms(self):
        self._login.request_sms_code()

    def login_submit_sms(self, code):
        self._login.submit_sms_and_trust(code)

    def login_submit_totp(self, code):
        self._login.submit_totp(code)

    def login_finish(self):
        """Вход подтверждён: прогреваем сессию, заходим в ed.mos.ru, берём квартиры."""
        client = self._login
        client.warm_session()
        client.authorize_ed()
        places = client.list_places()
        self._client = client
        self._ed_at = time.monotonic()
        self._save_cookies(client)
        self._login = None
        return places

    def select_place(self, place):
        """Запоминает квартиру и пытается определить счётчики ХВС и ГВС."""
        self._set("mosru_user_place_id", place["user_place_id"])
        counters = self._client.get_counters(place["user_place_id"])
        cold, hot = pick_counters(counters)
        if cold:
            self._set("mosru_cold_id", cold)
        if hot:
            self._set("mosru_hot_id", hot)
        return counters, cold, hot

    def logout(self):
        self._invalidate()
        self._login = None


def place_title(place):
    return place_label(place)


def describe_rows(rows):
    lines = []
    for r in rows:
        name = LABELS[r["label"]] + (f" (№{r['number']})" if r.get("number") else "")
        line = f"{name}: {r['value']} м³"
        if r["prev"] is not None:
            line += f" (на портале {r['prev']:g}, {r['value'] - r['prev']:+g})"
        status = r["status"]
        if status == "already":
            line += f" — уже внесено за {r['period'][:7]}"
        elif status == "closed":
            line += " — портал сейчас не принимает показания"
        elif status == "anomaly":
            line += f" — ⚠️ показание уменьшилось или выросло больше чем на лимит, проверьте вручную"
        elif status == "no_prev":
            line += " — не удалось прочитать прежнее показание портала"
        outcome = r.get("outcome")
        if outcome == "submitted":
            line += " ✅ отправлено"
        elif outcome == "already":
            line += " ✔️ уже было внесено"
        elif outcome == "error":
            line += f" ❌ ошибка: {r.get('error', '')[:100]}"
        lines.append(line)
    return "\n".join(lines)
