# msgautomatica.py
from flask import Flask, request, redirect, url_for, render_template_string, jsonify, flash
from flask import request, redirect, flash  # garante que isso está no topo do arquivo
import json
from typing import Dict, Any
import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from flask import Flask
import math
from typing import Dict, Any, List
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
import threading
import os, json, time, socket, threading, re, uuid, shutil
from datetime import datetime, time as dtime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote_plus
import unicodedata
from flask import (
    Flask, request, redirect, url_for, render_template_string, flash,
    send_from_directory, make_response, jsonify
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, SessionNotCreatedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium import webdriver






APP_TITLE   = "Mensageiro Automático"
CONFIG_FILE = "msgauto_config.json"
UPLOAD_DIR  = os.path.abspath("./uploads_msg")
PROFILE_DIR = os.path.abspath("./.chrome_profile_whatsapp")
LOG_LIMIT   = 2000
PORT        = 5100

DEVTOOLS_PORT = 9224
DEVTOOLS_ADDR = f"127.0.0.1:{DEVTOOLS_PORT}"


os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PROFILE_DIR, exist_ok=True)


DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "use_24h": True,
    "start_time": "08:00",
    "end_time": "18:00",
    "weekdays": [1,2,3,4,5],
    "frequency_minutes": 60,
    "message_text": "",
    "numbers": [],
    "groups": [],
    "attachments": [],
    "attachments_mode": "files", # files | folder | both
    "attachments_folder": "",
    "file_captions": {},
    # Itens personalizados:
    # {"id":"...","type":"file","path":"C:\\a.pdf","text":"Indicador 1","interval":5}
    # {"id":"...","type":"folder","path":"uploads_msg\\folder_20251101_174500_123456","text":"Relatórios","interval":10,"origin":"C:\\Origem","autosync":true}
    "custom_items": [],
    "item_states": {},
    "close_after_send": True,
    "last_run": None,
    "run_mode": "visible",       # visible | hidden
    # Pasta geral (modo folder/both) – origem/auto-sync
    "general_folder_origin": "",
    "general_folder_autosync": False,
}

app = Flask(__name__)
app.secret_key = "msgauto_secret_2025"
scheduler = BackgroundScheduler(daemon=True)
scheduler.start()
_job_id = "msgauto_job"

_logs: list[str] = []
LOG_LIMIT = 300  # mantém no máximo 300 linhas

# =================== Chrome / WhatsApp ===================
_driver_lock = threading.Lock()
_driver: Optional[webdriver.Chrome] = None
_driver_opened_by_app: bool = False

def criar_driver():
    options = Options()

    # Se quiser ver o Chrome abrindo, remove headless
    # options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-popup-blocking")
    options.add_experimental_option("detach", True)

    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(1300, 900)
    return driver

def _devnull() -> str:
    return "NUL" if os.name == "nt" else "/dev/null"
def load_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        log(f"[config] Config carregada de {CONFIG_FILE}")
        return cfg
    except FileNotFoundError:
        log(f"[config] Arquivo {CONFIG_FILE} não encontrado. Usando cfg vazia.")
        return {}
    except Exception as e:
        log(f"[config] Erro ao carregar {CONFIG_FILE}: {e}")
        return {}
    
# =================== util/log ===================
def criar_driver(headless=False):
    options = Options()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--window-size=1920,1080")

    # Usa o chromedriver do sistema
    service = Service("/usr/bin/chromedriver")

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def _ceil_to_next_multiple(dt: datetime, minutes: int) -> datetime:
    if minutes <= 0:
        return dt
    base = dt.replace(second=0, microsecond=0)
    delta_min = (base.minute % minutes)
    if delta_min == 0 and dt.second == 0 and dt.microsecond == 0:
        return base + timedelta(minutes=minutes)
    step = minutes - delta_min
    return base + timedelta(minutes=step)

def _add_minutes_until_future(start: datetime, minutes: int, now: datetime) -> datetime:
    if minutes <= 0:
        return now
    diff = (now - start).total_seconds()
    if diff < 0:
        return start
    steps = math.floor(diff / (minutes * 60)) + 1
    return start + timedelta(minutes=steps * minutes)
def _build_item_states_on_enable(cfg: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    now = datetime.now()
    states: Dict[str, Dict[str, str]] = cfg.get("item_states") or {}
    if not isinstance(states, dict):
        states = {}

    custom_items: List[Dict[str, Any]] = cfg.get("custom_items") or []
    for it in custom_items:
        iid = str(it.get("id") or "")
        if not iid:
            continue
        interval = it.get("interval") or 0
        st = states.get(iid, {}) or {}
        if interval and interval > 0:
            next_due_dt = _ceil_to_next_multiple(now, int(interval))
            st["next_due"] = next_due_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            st.pop("next_due", None)
        states[iid] = st

    freq = int(cfg.get("frequency_minutes") or 1)
    next_global_due_dt = _ceil_to_next_multiple(now, freq)
    cfg["enabled_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    cfg["next_global_due"] = next_global_due_dt.strftime("%Y-%m-%d %H:%M:%S")
    cfg["item_states"] = states
    return states

def _split_items_by_mode(cfg: Dict[str, Any]):
    items = cfg.get("custom_items") or []
    general_items = []
    individual_items = []
    for it in items:
        interval = it.get("interval") or 0
        if interval and int(interval) > 0:
            individual_items.append(it)
        else:
            general_items.append(it)
    return general_items, individual_items

def send_items_to_chat(cfg: Dict[str, Any], items: List[Dict[str, Any]]):
    if not items:
        return
    nums = cfg.get("numbers") or []
    groups = cfg.get("groups") or []
    for it in items:
        try:
            tipo = it.get("type")
            caminho = it.get("path")
            msg = it.get("text") or (cfg.get("message_text") or "")
            # chame aqui seu pipeline real de envio (browser, selenium, etc.)
            # ex: whatsapp_send(cfg, tipo, caminho, msg, nums, groups)
            log(f"[envio] {tipo} → {caminho} | nums={len(nums)} groups={len(groups)}")
        except Exception as e:
            log(f"[erro-envio] {e}")

def run_general_once(cfg: Dict[str, Any]):
    log("[agenda-geral] execução iniciada")
    general_items, _ = _split_items_by_mode(cfg)

    # 1) anexos gerais (seu handler atual)
    # ex: send_general_attachments(cfg)

    # 2) itens sem intervalo (rodam pela agenda geral)
    if general_items:
        send_items_to_chat(cfg, general_items)

    cfg["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("[agenda-geral] execução concluída")
    
def toggle_on(cfg: Dict[str, Any]):
    if cfg.get("enabled"):
        log("Já estava ligado")
        return
    cfg["enabled"] = True
    _build_item_states_on_enable(cfg)
    log(f"[agenda] Ligado às {cfg['enabled_at']}. Próxima execução geral: {cfg['next_global_due']}")

def toggle_off(cfg: Dict[str, Any]):
    if not cfg.get("enabled"):
        log("Já estava desligado")
        return
    cfg["enabled"] = False
    log("[agenda] Desligado")
def verifier_loop(cfg_provider, stop_flag):
    log("Mensageiro LIGADO – verificação a cada 1 minuto (intervalos individuais ativos).")
    while not stop_flag.is_set():
        try:
            cfg = cfg_provider()
            scheduler_tick(cfg)
        except Exception as e:
            log(f"[verificador-erro] {e}")
        stop_flag.wait(60)
    

def scheduler_tick(cfg: Dict[str, Any]):
    if not cfg.get("enabled"):
        return
    now = datetime.now()

    freq = int(cfg.get("frequency_minutes") or 1)
    next_global_due_s = cfg.get("next_global_due")
    next_global_due = datetime.strptime(next_global_due_s, "%Y-%m-%d %H:%M:%S") if next_global_due_s else _ceil_to_next_multiple(now, freq)

    if now >= next_global_due:
        run_general_once(cfg)
        next_global_due = _add_minutes_until_future(next_global_due, freq, now)
        cfg["next_global_due"] = next_global_due.strftime("%Y-%m-%d %H:%M:%S")
        log(f"[agenda] Próxima execução geral: {cfg['next_global_due']}")

    states: Dict[str, Dict[str, str]] = cfg.get("item_states") or {}
    _, individual_items = _split_items_by_mode(cfg)
    if individual_items:
        due_batch = []
        for it in individual_items:
            iid = str(it.get("id"))
            interval = int(it.get("interval") or 0)
            if interval <= 0:
                continue
            st = states.get(iid) or {}
            nd_s = st.get("next_due")
            if not nd_s:
                nd = _ceil_to_next_multiple(now, interval)
                st["next_due"] = nd.strftime("%Y-%m-%d %H:%M:%S")
                states[iid] = st
                continue
            nd = datetime.strptime(nd_s, "%Y-%m-%d %H:%M:%S")
            if now >= nd:
                due_batch.append(it)

        if due_batch:
            send_items_to_chat(cfg, due_batch)
            cycle_end = datetime.now()
            for it in due_batch:
                iid = str(it.get("id"))
                interval = int(it.get("interval") or 0)
                st = states.get(iid) or {}
                st["last_sent"] = cycle_end.strftime("%Y-%m-%d %H:%M:%S")
                next_dt = _reschedule_from_cycle_end(cycle_end, interval, cfg)
                st["next_due"] = next_dt.strftime("%Y-%m-%d %H:%M:%S")
                states[iid] = st
                log(f"[agenda-individual] item {iid} próximo: {st['next_due']}")

    cfg["item_states"] = states


_logs: List[str] = []
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _logs.append(line)
    if len(_logs) > LOG_LIMIT:
        del _logs[:len(_logs)-LOG_LIMIT]
        

def load_cfg() -> Dict[str, Any]:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            merged = {**DEFAULT_CONFIG, **cfg}
            merged["weekdays"]           = cfg.get("weekdays", DEFAULT_CONFIG["weekdays"])
            merged["numbers"]            = cfg.get("numbers", DEFAULT_CONFIG["numbers"])
            merged["groups"]             = cfg.get("groups", DEFAULT_CONFIG["groups"])
            merged["attachments"]        = cfg.get("attachments", DEFAULT_CONFIG["attachments"])
            merged["attachments_mode"]   = cfg.get("attachments_mode", DEFAULT_CONFIG["attachments_mode"])
            merged["attachments_folder"] = cfg.get("attachments_folder", "")
            merged["custom_items"]       = cfg.get("custom_items", [])
            merged["file_captions"]      = cfg.get("file_captions", {})
            merged["item_states"]        = cfg.get("item_states", {})
            if merged.get("run_mode") not in ("visible","hidden"): merged["run_mode"] = "visible"
            if merged.get("attachments_mode") not in ("files","folder","both"): merged["attachments_mode"] = "files"
            merged["close_after_send"]   = bool(cfg.get("close_after_send", True))
            # defaults para novos campos:
            merged["general_folder_origin"]   = cfg.get("general_folder_origin", "")
            merged["general_folder_autosync"] = bool(cfg.get("general_folder_autosync", False))
            return merged
        except Exception as e:
            log(f"Erro lendo config: {e}")
    return DEFAULT_CONFIG.copy()

def save_cfg(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def _parse_hhmm(s: str) -> Optional[dtime]:
    try:
        hh,mm=s.strip().split(":"); return dtime(int(hh),int(mm))
    except Exception:
        return None

def _parse_weekdays(text: str) -> List[int]:
    t = (text or "").strip()
    if not t: return [1,2,3,4,5]
    try:
        arr = json.loads(t)
        if isinstance(arr, list):
            return [int(x) for x in arr if int(x) in range(1,8)]
    except Exception:
        pass
    try:
        arr = [int(x.strip()) for x in t.split(",") if x.strip()]
        arr = [x for x in arr if x in range(1,8)]
        return arr or [1,2,3,4,5]
    except Exception:
        return [1,2,3,4,5]

def _now_in_window(cfg: Dict[str, Any]) -> bool:
    if cfg.get("use_24h", True): return True
    wd = int(datetime.now().isoweekday())
    if wd not in (cfg.get("weekdays") or [1,2,3,4,5]): return False
    st = _parse_hhmm(cfg.get("start_time","08:00")) or dtime(0,0)
    et = _parse_hhmm(cfg.get("end_time","18:00"))   or dtime(23,59)
    nowt = datetime.now().time()
    return (st <= nowt <= et) if st <= et else (nowt >= st or nowt <= et)

# =================== Chrome / WhatsApp ===================
_driver_lock = threading.Lock()
_driver: Optional[webdriver.Chrome] = None
_driver_opened_by_app: bool = False

def _devnull() -> str:
    return "NUL" if os.name == "nt" else "/dev/null"

def _make_chrome_options(run_mode: str) -> webdriver.ChromeOptions:
    o = webdriver.ChromeOptions()

    # Perfil e DevTools (mantidos)
    o.add_argument(f"--user-data-dir={PROFILE_DIR}")
    o.add_argument(f"--remote-debugging-port={DEVTOOLS_PORT}")

    # Flags importantes pra rodar em container (Railway/Docker)
    o.add_argument("--no-sandbox")
    o.add_argument("--disable-dev-shm-usage")
    o.add_argument("--disable-gpu")
    o.add_argument("--disable-extensions")
    o.add_argument("--disable-popup-blocking")
    o.add_argument("--disable-notifications")
    o.add_argument("--window-size=1920,1080")
    o.add_argument("--log-level=3")
    o.add_experimental_option("excludeSwitches", ["enable-logging"])

    # No servidor SEMPRE headless – isso funciona local também
    o.add_argument("--headless=new")

    return o

def _attach_to_existing() -> Optional[webdriver.Chrome]:
    """Tenta conectar em um Chrome já aberto com DevTools na porta configurada."""
    try:
        # testa se a porta do DevTools está aberta
        s = socket.socket()
        s.settimeout(0.5)
        try:
            host, port_str = DEVTOOLS_ADDR.split(":")
            s.connect((host, int(port_str)))
            s.close()
        except OSError:
            log("Sem Chrome aberto no DevTools, pulando attach.")
            return None

        o = webdriver.ChromeOptions()
        o.debugger_address = DEVTOOLS_ADDR
        o.add_argument(f"--user-data-dir={PROFILE_DIR}")

        # Selenium Manager resolve o driver (sem webdriver_manager)
        drv = webdriver.Chrome(options=o)
        log("Anexado a um Chrome existente via DevTools.")
        return drv
    except SessionNotCreatedException:
        log("Sem Chrome aberto para anexar.")
        return None
    except WebDriverException as e:
        log(f"Falha ao anexar: {e.__class__.__name__}")
        return None
    except Exception as e:
        log(f"Falha ao anexar: {e}")
        return None
        
def _launch_new(run_mode: str) -> webdriver.Chrome:
    """Lança um novo Chrome controlado pelo Selenium usando o chromedriver do sistema."""
    o = _make_chrome_options(run_mode)
    service = Service("/usr/bin/chromedriver")
    drv = webdriver.Chrome(service=service, options=o)
    log("Novo Chrome lançado.")
    return drv


def _get_driver(run_mode: str = "visible") -> webdriver.Chrome:
    global _driver, _driver_opened_by_app
    if _driver is not None:
        return _driver

    drv = _attach_to_existing()
    if drv is None:
        drv = _launch_new(run_mode)
        _driver_opened_by_app = True
    else:
        _driver_opened_by_app = False

    _driver = drv
    return drv

def _get_driver(run_mode: str="visible") -> webdriver.Chrome:
    global _driver, _driver_opened_by_app
    if _driver is not None: return _driver
    drv = _attach_to_existing()
    if drv is None:
        drv = _launch_new(run_mode); _driver_opened_by_app = True
    else:
        _driver_opened_by_app = False
    _driver = drv; return drv

def _find_whatsapp_tab(drv: webdriver.Chrome) -> bool:
    try:
        for h in drv.window_handles:
            drv.switch_to.window(h)
            if "web.whatsapp.com" in (drv.current_url or ""): return True
        return False
    except Exception:
        return False

def _is_logged_in(drv: webdriver.Chrome) -> bool:
    try:
        WebDriverWait(drv, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[contenteditable='true']")))
        return True
    except Exception:
        return False
    


# =================== helpers UI ===================
from datetime import datetime, timedelta
import math

def _reschedule_from_cycle_end(cycle_end: datetime, step_min: int, cfg: Dict[str, Any]) -> datetime:
    step_min = max(1, int(step_min or 1))
    nd = cycle_end + timedelta(minutes=step_min)
    while not _within_time_window(cfg, nd):
        nd += timedelta(minutes=step_min)
    return nd


def _reschedule_from_cycle_end(cycle_end: datetime, step_min: int, cfg: Dict[str, Any]) -> datetime:
    step_min = max(1, int(step_min or 1))
    nd = cycle_end + timedelta(minutes=step_min)
    while not _within_time_window(cfg, nd):
        nd += timedelta(minutes=step_min)
    return nd


def _mark_items_sent_after_cycle(cfg: Dict[str, Any], sent_item_ids: set, cycle_end: datetime) -> None:
    if not sent_item_ids:
        return
    items = cfg.get("custom_items") or []
    for it in items:
        it_id = str(it.get("id") or "")
        if it_id in sent_item_ids:
            try:
                iv = int(it.get("interval") or 0)
            except Exception:
                iv = 0
            if iv > 0:
                it["next_due"] = _dtfmt(cycle_end + timedelta(minutes=iv))
    save_cfg(cfg)



def _dtfmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _dtparse(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _build_item_states_on_enable(cfg: Dict[str, Any]) -> Dict[str, Any]:
    states = cfg.setdefault("item_states", {}) or {}
    now = datetime.now()

    # agenda global
    try:
        freq = max(1, int(cfg.get("frequency_minutes") or 60))
    except Exception:
        freq = 60
    cfg["enabled_at"] = _dtfmt(now)
    cfg["next_global_due"] = _dtfmt(now + timedelta(minutes=freq))

    # agenda individual por ITEM (somente os que têm intervalo)
    for it in (cfg.get("custom_items") or []):
        try:
            ival = int(it.get("interval") or 0)
        except Exception:
            ival = 0
        if ival <= 0:
            continue

        iid = str(it.get("id") or "")
        if not iid:
            continue

        st = states.get(iid) or {}
        st["last_sent"] = None
        st["next_due"]  = _dtfmt(now + timedelta(minutes=ival))
        states[iid] = st

    cfg["item_states"] = states
    save_cfg(cfg)
    return states

def _next_individual_due(cfg: Dict[str, Any]) -> Optional[datetime]:
    states = cfg.get("item_states") or {}
    nexts: list[datetime] = []

    for it in (cfg.get("custom_items") or []):
        try:
            ival = int(it.get("interval") or 0)
        except Exception:
            ival = 0
        if ival <= 0:
            continue

        iid = str(it.get("id") or "")
        if not iid:
            continue

        st = states.get(iid) or {}
        nd = _dtparse(st.get("next_due"))
        if nd:
            nexts.append(nd)

    return min(nexts) if nexts else None

def _items_due(cfg: Dict[str, Any], force: bool = False, global_cycle: bool = False) -> List[Dict[str, Any]]:
    now = datetime.now()
    items = cfg.get("custom_items") or []

    if force:
        # tudo que for válido
        out = []
        for it in items:
            t = (it.get("type") or "file").lower()
            p = (it.get("path") or "").strip()
            if (t == "file" and os.path.isfile(p)) or (t == "folder" and os.path.isdir(p)):
                out.append(it)
        return out

    states = cfg.get("item_states") or {}
    due: List[Dict[str, Any]] = []

    for it in items:
        t = (it.get("type") or "file").lower()
        p = (it.get("path") or "").strip()
        if not ((t == "file" and os.path.isfile(p)) or (t == "folder" and os.path.isdir(p))):
            continue

        try:
            ival = int(it.get("interval") or 0)
        except Exception:
            ival = 0

        if ival > 0:
            iid = str(it.get("id") or "")
            st = states.get(iid) or {}
            nd = _dtparse(st.get("next_due"))
            if nd and now >= nd:
                due.append(it)
        else:
            # itens sem intervalo só entram no ciclo global
            if global_cycle:
                due.append(it)

    return due

def _mark_item_sent(cfg: Dict[str, Any], item_id: str) -> None:
    states = cfg.setdefault("item_states", {}) or {}
    st = states.get(item_id) or {}
    now = datetime.now()
    st["last_sent"] = _dtfmt(now)

    # pega intervalo do próprio item
    ival = 0
    for it in (cfg.get("custom_items") or []):
        if str(it.get("id") or "") == item_id:
            try:
                ival = int(it.get("interval") or 0)
            except Exception:
                ival = 0
            break

    if ival > 0:
        st["next_due"] = _dtfmt(now + timedelta(minutes=ival))

    states[item_id] = st
    cfg["item_states"] = states
    save_cfg(cfg)



def _run_sequence_numbers_then_groups(drv, cfg: dict, items_due: list[dict], mode: str) -> None:
    nums = list(cfg.get("numbers") or [])
    grps = list(cfg.get("groups") or [])

    # --- NÚMEROS primeiro (um por vez) ---
    for num in nums:
        try:
            _open_number_chat(drv, num, "")
            if mode == "general":
                # _send_all_to_chat já cria seu sent_paths próprio por chat
                _send_all_to_chat(drv, cfg, num, items_due or [])
            else:
                # IMPORTANTÍSSIMO: zera o controle por chat
                sent_paths: set = set()
                _send_items_to_chat(drv, cfg, num, items_due or [], sent_paths)
            log(f"[{num}] Envio concluído.")
        except Exception as e:
            log(f"[{num}] Falha: {e}")
        time.sleep(0.25)

    # --- Depois GRUPOS (um por vez) ---
    for grp in grps:
        try:
            _open_whatsapp(drv)
            _open_group_chat(drv, grp)
            if mode == "general":
                _send_all_to_chat(drv, cfg, grp, items_due or [])
            else:
                # Reinicia para cada grupo também
                sent_paths: set = set()
                _send_items_to_chat(drv, cfg, grp, items_due or [], sent_paths)
            log(f"[{grp}] Envio concluído.")
        except Exception as e:
            log(f"[{grp}] Falha: {e}")
        time.sleep(0.25)



import math
from datetime import datetime, timedelta

def _advance_until_future(start_dt: datetime, step_min: int, now: datetime) -> datetime:
    if step_min <= 0:
        return now
    if now <= start_dt:
        return start_dt
    steps = math.floor((now - start_dt).total_seconds() / (step_min * 60)) + 1
    return start_dt + timedelta(minutes=steps * step_min)

def _next_individual_due(cfg: dict):
    states = cfg.get("item_states") or {}
    best = None
    for it in (cfg.get("custom_items") or []):
        try:
            interval = int(it.get("interval") or 0)
        except Exception:
            interval = 0
        if interval <= 0:
            continue
        iid = str(it.get("id") or "")
        if not iid: 
            continue
        nd_s = (states.get(iid) or {}).get("next_due")
        if not nd_s:
            continue
        try:
            nd = datetime.strptime(nd_s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if (best is None) or (nd < best):
            best = nd
    return best

def _get_item_by_id(cfg: dict, iid: str) -> dict | None:
    for it in (cfg.get("custom_items") or []):
        if str(it.get("id") or "") == str(iid):
            return it
    return None

def _mark_item_sent(cfg: dict, item_id: str) -> None:
    states = cfg.setdefault("item_states", {})
    st = states.get(item_id) or {}
    st["last_sent"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    interval = 0
    for it in (cfg.get("custom_items") or []):
        if str(it.get("id") or "") == item_id:
            try:
                interval = int(it.get("interval") or 0)
            except Exception:
                interval = 0
            break

    if interval > 0:
        now = datetime.now()
        base_s = st.get("next_due") or st.get("anchor") or cfg.get("enabled_at") or now.strftime("%Y-%m-%d %H:%M:%S")
        try:
            base_dt = datetime.strptime(base_s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            base_dt = now
        nxt = _advance_until_future(base_dt, interval, now)
        st["next_due"] = nxt.strftime("%Y-%m-%d %H:%M:%S")

    states[item_id] = st
    save_cfg(cfg)
    
# --- quais INDIVIDUAIS venceram agora, segundo a pré-agenda (anchor/next_due)
def _due_individuals_now(cfg: dict, now: datetime) -> list[dict]:
    due = []
    states = cfg.get("item_states") or {}
    for it in (cfg.get("custom_items") or []):
        try:
            interval = int(it.get("interval") or 0)
        except Exception:
            interval = 0
        if interval <= 0:
            continue

        iid = str(it.get("id") or "")
        if not iid:
            continue

        st = states.get(iid) or {}

        # se ainda não tem next_due (ligou agora / item novo), cria a partir da âncora
        nd_s = st.get("next_due")
        if not nd_s:
            anchor_s = st.get("anchor") or cfg.get("enabled_at") or now.strftime("%Y-%m-%d %H:%M:%S")
            anchor   = datetime.strptime(anchor_s, "%Y-%m-%d %H:%M:%S")
            st["next_due"] = (anchor + timedelta(minutes=interval)).strftime("%Y-%m-%d %H:%M:%S")
            states[iid] = st
            nd_s = st["next_due"]

        try:
            nd = datetime.strptime(nd_s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        if now >= nd:
            due.append(it)

    if due:
        cfg["item_states"] = states
        save_cfg(cfg)

    return due

# --- envio só dos INDIVIDUAIS vencidos (não mexe na agenda geral)
def perform_send_individuals(cfg: dict, items_due: list[dict]) -> tuple[bool, str]:
    if not items_due:
        return True, "Sem individuais vencidos."

    __resync_all_folders_force(cfg)  # atualiza pastas antes de enviar

    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."

    if not _now_in_window(cfg):
        return False, "Fora da janela de funcionamento."

    ok_any = False
    global _driver, _driver_opened_by_app
    with _driver_lock:
        try:
            drv = _get_driver(cfg.get("run_mode","visible"))
            _open_whatsapp(drv)
            if not _is_logged_in(drv):
                log("Aguardando login (QR) no WhatsApp Web…"); time.sleep(10)
        except Exception as e:
            log(f"Erro no WhatsApp: {e}")
            try:
                if _driver_opened_by_app and _driver: _driver.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app = False
            return False, f"Erro no WhatsApp Web: {e}"

        # NÚMEROS primeiro
        for num in list(cfg.get("numbers") or []):
            try:
                _open_number_chat(drv, num, "")
                # sent_paths deve ser POR CHAT
                _send_items_to_chat(drv, cfg, num, items_due, sent_paths=set())
                ok_any = True
            except Exception as e:
                log(f"[{num}] Falha: {e}")

        # Depois GRUPOS
        for grp in list(cfg.get("groups") or []):
            try:
                _open_whatsapp(drv)
                _open_group_chat(drv, grp)
                _send_items_to_chat(drv, cfg, grp, items_due, sent_paths=set())
                ok_any = True
            except Exception as e:
                log(f"[{grp}] Falha: {e}")

        if cfg.get("close_after_send", True) and _driver_opened_by_app:
            try: drv.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            log("Navegador fechado (aberto pelo app).")

    return (True if ok_any else False), ("Individuais enviados." if ok_any else "Falha nos individuais.")


def perform_send_individuals(cfg: dict, items: list[dict]) -> tuple[bool, str]:
    if not items:
        return False, "Nenhum item individual devido."
    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."

    global _driver, _driver_opened_by_app
    with _driver_lock:
        try:
            drv = _get_driver(cfg.get("run_mode","visible"))
            _open_whatsapp(drv)
            if not _is_logged_in(drv):
                log("Aguardando login (QR) no WhatsApp Web... 10s")
                time.sleep(10)
        except Exception as e:
            log(f"Erro no WhatsApp (individual): {e}")
            try:
                if _driver_opened_by_app and _driver: _driver.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            return False, f"Erro no WhatsApp Web (individual): {e}"

        _run_sequence_numbers_then_groups(drv, cfg, items, mode="individual")

        for it in items:
            iid = str(it.get("id") or "")
            if iid:
                _mark_item_sent(cfg, iid)

        if cfg.get("close_after_send", True) and _driver_opened_by_app:
            try: drv.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            log("Navegador fechado (individuais).")

    return True, "Individuais enviados."


# --- envio GERAL (não envia itens com intervalo individual definido)
def perform_send_general(cfg: dict) -> tuple[bool, str]:
    __resync_all_folders_force(cfg)

    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."
    if not _now_in_window(cfg):
        return False, "Fora da janela de funcionamento."

    # apenas itens SEM intervalo (ou 0/None)
    items_no_interval = []
    for it in (cfg.get("custom_items") or []):
        try:
            interval = int(it.get("interval") or 0)
        except Exception:
            interval = 0
        if interval <= 0:
            items_no_interval.append(it)

    ok_any = False
    global _driver, _driver_opened_by_app
    with _driver_lock:
        try:
            drv = _get_driver(cfg.get("run_mode","visible"))
            _open_whatsapp(drv)
            if not _is_logged_in(drv):
                log("Aguardando login (QR) no WhatsApp Web…"); time.sleep(10)
        except Exception as e:
            log(f"Erro no WhatsApp: {e}")
            try:
                if _driver_opened_by_app and _driver: _driver.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            return False, f"Erro no WhatsApp Web: {e}"

        # números primeiro
        for num in list(cfg.get("numbers") or []):
            try:
                _open_number_chat(drv, num, "")
                _send_all_to_chat(drv, cfg, num, items_no_interval)
                ok_any = True
            except Exception as e:
                log(f"[{num}] Falha: {e}")

        # grupos depois
        for grp in list(cfg.get("groups") or []):
            try:
                _open_whatsapp(drv)
                _open_group_chat(drv, grp)
                _send_all_to_chat(drv, cfg, grp, items_no_interval)
                ok_any = True
            except Exception as e:
                log(f"[{grp}] Falha: {e}")

        if cfg.get("close_after_send", True) and _driver_opened_by_app:
            try: drv.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            log("Navegador fechado (aberto pelo app).")

    return (True if ok_any else False), ("Geral enviado." if ok_any else "Falha no envio geral.")


def _build_item_states_on_enable(cfg: dict) -> dict:
    now = datetime.now()
    enabled_at = now.strftime("%Y-%m-%d %H:%M:%S")
    cfg["enabled_at"] = enabled_at

    try:
        freq = int(cfg.get("frequency_minutes") or 0)
    except Exception:
        freq = 0

    cfg["global_anchor"] = enabled_at
    cfg["next_global_due"] = (now + timedelta(minutes=freq)).strftime("%Y-%m-%d %H:%M:%S") if freq > 0 else None

    states = cfg.get("item_states") or {}
    for it in (cfg.get("custom_items") or []):
        iid = str(it.get("id") or "")
        if not iid:
            continue
        try:
            interval = int(it.get("interval") or 0)
        except Exception:
            interval = 0
        if interval > 0:
            st = states.get(iid) or {}
            st["anchor"]   = enabled_at
            st["next_due"] = (now + timedelta(minutes=interval)).strftime("%Y-%m-%d %H:%M:%S")
            states[iid] = st

    cfg["item_states"] = states
    save_cfg(cfg)
    return states

def _schedule(cfg: dict) -> None:
    try:
        j = scheduler.get_job(_job_id)
        if j:
            scheduler.remove_job(_job_id)
    except Exception:
        pass

    if not cfg.get("enabled"):
        log("Mensageiro está DESLIGADO.")
        return

    now = datetime.now()

    # RECONSTRUI A PRÉ-AGENDA AQUI
    states = _build_item_states_on_enable(cfg)

    # LOG GERAL
    log(f"[agenda] Ligado às {cfg['enabled_at']}. Próxima execução geral: {cfg['next_global_due']}")
    log(f"[agenda] Verificação a cada 1 minuto (individuais ativos).")

    # LOG INDIVIDUAL de cada arquivo com pré-agenda
    for it in (cfg.get("custom_items") or []):
        try:
            if int(it.get("interval") or 0) > 0:
                st = states.get(str(it.get("id") or ""), {})
                if st.get("next_due"):
                    log(f"[agenda-ind] {os.path.basename(it.get('path',''))} → {st['next_due']}")
        except Exception:
            pass

    # Cria job de 1 em 1 minuto apenas para checar vencimentos
    scheduler.add_job(
        func=_job_wrapper,
        trigger=IntervalTrigger(minutes=1),
        id=_job_id,
        max_instances=1,
        coalesce=True,
        replace_existing=True
    )

def scheduler_tick(cfg: dict):
    if not cfg.get("enabled"):
        return

    now  = datetime.now()
    freq = int(cfg.get("frequency_minutes") or 1)

    ngd_s = cfg.get("next_global_due")
    if not ngd_s:
        anchor_s = cfg.get("global_anchor") or cfg.get("enabled_at") or now.strftime("%Y-%m-%d %H:%M:%S")
        anchor   = datetime.strptime(anchor_s, "%Y-%m-%d %H:%M:%S")
        cfg["next_global_due"] = (anchor + timedelta(minutes=freq)).strftime("%Y-%m-%d %H:%M:%S")
        ngd_s = cfg["next_global_due"]

    ngd = datetime.strptime(ngd_s, "%Y-%m-%d %H:%M:%S")

    if now >= ngd:
        run_general_once(cfg)
        ngd = _advance_until_future(ngd, freq, now)
        cfg["next_global_due"] = ngd.strftime("%Y-%m-%d %H:%M:%S")
        log(f"[agenda] Próxima execução geral: {cfg['next_global_due']}")

    states = cfg.get("item_states") or {}
    items  = cfg.get("custom_items") or []
    due    = []

    for it in items:
        interval = int(it.get("interval") or 0)
        if interval <= 0:
            continue
        iid = str(it.get("id") or "")
        if not iid:
            continue
        st = states.get(iid) or {}
        nd_s = st.get("next_due")
        if not nd_s:
            anchor = now
            st["anchor"]   = anchor.strftime("%Y-%m-%d %H:%M:%S")
            st["next_due"] = (anchor + timedelta(minutes=interval)).strftime("%Y-%m-%d %H:%M:%S")
            states[iid] = st
            nd_s = st["next_due"]
        nd = datetime.strptime(nd_s, "%Y-%m-%d %H:%M:%S")
        if now >= nd:
            due.append(it)

    if due:
        send_items_to_chat(cfg, due)
        cycle_end = datetime.now()
        for it in due:
            iid = str(it.get("id") or "")
            if not iid:
                continue
            interval = int(it.get("interval") or 0)
            st = states.get(iid) or {}
            st["last_sent"] = cycle_end.strftime("%Y-%m-%d %H:%M:%S")
            next_dt = _reschedule_from_cycle_end(cycle_end, interval, cfg)
            st["next_due"]  = next_dt.strftime("%Y-%m-%d %H:%M:%S")
            states[iid] = st
            log(f"[agenda-individual] item {iid} próximo: {st['next_due']}")

    cfg["item_states"] = states


def _bump_next_due(start: datetime, minutes: int, cfg: Dict[str, Any]) -> datetime:
    """Avança em passos de `minutes` até ficar > agora e dentro da janela."""
    minutes = max(1, int(minutes or 1))
    now = datetime.now()
    nd = start
    while nd <= now or not _within_time_window(cfg, nd):
        nd += timedelta(minutes=minutes)
    return nd

def _ceil_next_tick(now: datetime, minutes: int) -> datetime:
    """Próximo múltiplo de `minutes` acima de `now` (exclui o próprio now)."""
    minutes = max(1, int(minutes or 1))
    base = now.replace(second=0, microsecond=0)
    delta = (base.minute % minutes)
    if delta == 0 and base == now:
        # ex.: 10:00 crava próximo 10:10 (não executa agora)
        return base + timedelta(minutes=minutes)
    bump = minutes - delta if delta else minutes
    if base < now:
        base += timedelta(minutes=1)
    base = base.replace(second=0, microsecond=0)
    # reavalia delta pós ajuste
    delta = (base.minute % minutes)
    bump = minutes - delta if delta else minutes
    return base + timedelta(minutes=bump)

def _within_time_window(cfg: Dict[str, Any], dt: datetime) -> bool:
    """Checa janela/dias para um datetime específico (não apenas 'agora')."""
    if cfg.get("use_24h", True):
        return True
    wd = int(dt.isoweekday())
    if wd not in (cfg.get("weekdays") or [1,2,3,4,5]):
        return False
    st = _parse_hhmm(cfg.get("start_time","08:00")) or dtime(0,0)
    et = _parse_hhmm(cfg.get("end_time","18:00"))   or dtime(23,59)
    tt = dt.time()
    return (st <= tt <= et) if st <= et else (tt >= st or tt <= et)

def _scroll_bottom(drv):
    try: drv.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception: pass

def _count_messages(drv: webdriver.Chrome) -> int:
    try:
        return len(drv.find_elements(By.CSS_SELECTOR, "div.message-in, div.message-out, div[role='row']"))
    except Exception:
        return 0

def _preview_open(drv) -> bool:
    """
    Detecta o modal de preview no layout novo/antigo.
    """
    try:
        sels = [
            "[data-testid='media-editor-root']",
            "[data-testid='media-preview']",
            "div[role='dialog'] div[data-testid*='media']",
            "div[role='dialog'] a[href$='.pdf']",           # título do pdf (layout novo)
            "div[role='dialog'] footer",                    # rodapé do modal
            "div[aria-label*='Pré-visualização']",
        ]
        for css in sels:
            els = drv.find_elements(By.CSS_SELECTOR, css)
            if any(e.is_displayed() for e in els):
                return True
    except Exception:
        pass
    return False
    
def _find_modal_root(drv):
    """
    Raiz do modal de preview no layout novo/antigo.
    """
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='media-editor-root']"),
        (By.CSS_SELECTOR, "[data-testid='media-preview']"),
        (By.CSS_SELECTOR, "div[role='dialog']"),
    ]
    for by, sel in candidates:
        try:
            els = drv.find_elements(by, sel)
            els = [e for e in els if e.is_displayed()]
            if els:
                return els[-1]
        except Exception:
            continue
    return None

def _cancel_preview_if_open(drv):
    """Fecha o preview se estiver aberto (ESC + botões comuns)."""
    try:
        if not _preview_open(drv):
            return
        for by, sel in [
            (By.CSS_SELECTOR, "[data-testid='media-editor-cancel']"),
            (By.CSS_SELECTOR, "button[aria-label*='Cancelar']"),
            (By.CSS_SELECTOR, "div[aria-label*='Cancelar']"),
            (By.CSS_SELECTOR, "[data-testid='sticker-send-cancel']"),
        ]:
            btns = drv.find_elements(by, sel)
            if btns:
                drv.execute_script("arguments[0].click()", btns[0])
                time.sleep(0.2)
                break
        try:
            ActionChains(drv).send_keys(Keys.ESCAPE).perform()
        except Exception:
            pass
        # aguarda fechar
        for _ in range(40):
            if not _preview_open(drv):
                break
            time.sleep(0.12)
    except Exception:
        pass

def _find_first_displayed(drv, candidates: List[Tuple[str,str]]):
    for by, sel in candidates:
        try:
            els = drv.find_elements(by, sel)
            els = [e for e in els if e.is_displayed()]
            if els: return els[0]
        except Exception:
            continue
    return None

def _get_chat_box(drv):
    """
    Retorna o campo de digitação do chat ativo (não o campo de busca).
    Garante foco correto mesmo após envio de anexo.
    """
    candidates = [
        # composer real do chat (preferência)
        (By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab]"),
        (By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input']"),
        # alternativas seguras
        (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
        (By.XPATH, "//footer//div[@contenteditable='true']"),
    ]

    for by, sel in candidates:
        try:
            els = drv.find_elements(by, sel)
            els = [e for e in els if e.is_displayed()]
            for el in els:
                # ignora campo de busca (fica fora do footer)
                anc = drv.execute_script("return arguments[0].closest('header, footer')", el)
                if anc and anc.tag_name.lower() == "footer":
                    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    el.click()
                    return el
        except Exception:
            continue

    # fallback: espera explicitamente o campo de composer no footer
    el = WebDriverWait(drv, 8).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab]")
        )
    )
    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    el.click()
    return el


def _composer_has_text(drv) -> bool:
    """Retorna True se a caixa do chat ainda contém texto não-vazio."""
    try:
        el = _get_chat_box(drv)
        if not el: return False
        val = (el.text or "") + (el.get_attribute("innerText") or "") + (el.get_attribute("value") or "")
        return bool(val.strip())
    except Exception:
        return False


def _get_caption_box(drv):
    """
    Localiza o campo de LEGENDA *dentro do modal*.
    Prioriza o <p.selectable-text.copyable-text> com <span data-lexical-text>.
    Retorna WebElement ou None.
    """
    # aguarda abrir
    end = time.time() + 12
    while time.time() < end and not _preview_open(drv):
        time.sleep(0.1)

    root = _find_modal_root(drv)
    if not root:
        return None

    # 1) alvo preferencial (WhatsApp novo com Lexical)
    try:
        nodes = root.find_elements(
            By.CSS_SELECTOR,
            "p.selectable-text.copyable-text[dir] span.selectable-text.copyable-text[data-lexical-text='true']"
        )
        nodes = [n for n in nodes if n.is_displayed()]
        if nodes:
            drv.execute_script("arguments[0].scrollIntoView({block:'center'});", nodes[-1])
            return nodes[-1]
    except Exception:
        pass

    # 2) contenteditable dentro do modal (fallbacks)
    candidates = [
        (By.CSS_SELECTOR, "[contenteditable='true']"),
        (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
        (By.XPATH, ".//div[@contenteditable='true' and not(@aria-hidden='true')]"),
        (By.XPATH, ".//p[@contenteditable='true']"),
    ]
    limit = time.time() + 8
    while time.time() < limit:
        for by, sel in candidates:
            try:
                els = root.find_elements(by, sel)
                els = [e for e in els if e.is_displayed()]
                if els:
                    el = els[-1]
                    try:
                        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    except Exception:
                        pass
                    return el
            except Exception:
                continue
        time.sleep(0.15)
    return None

def _ensure_caption_text(drv, el, text: str) -> bool:
    """
    Garante que 'text' entrou na LEGENDA do modal:
    tenta teclado, depois JS/paste, e valida lendo innerText/value.
    """
    if not text:
        return True

    ok = False
    try:
        el.click()
        try:
            el.clear()  # se suportado
        except Exception:
            try:
                el.send_keys(Keys.CONTROL, 'a'); el.send_keys(Keys.DELETE)
            except Exception:
                pass
    except Exception:
        pass

    # 1) teclado
    try:
        el.click()
        el.send_keys(text)
        ok = True
    except Exception:
        ok = False

    # 2) fallback JS/paste
    if not ok:
        try:
            drv.execute_script("""
                const el = arguments[0], txt = arguments[1];
                el.focus();
                try{ document.execCommand('selectAll', false, null); document.execCommand('delete', false, null);}catch(e){}
                try{ document.execCommand('insertText', false, txt); }
                catch(e){
                  const dt = new DataTransfer(); dt.setData('text/plain', txt);
                  el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
                }
                el.dispatchEvent(new InputEvent('input', {bubbles:true}));
            """, el, text)
            ok = True
        except Exception:
            ok = False

    # 3) valida
    try:
        val = ((el.text or "") + (el.get_attribute("innerText") or "") + (el.get_attribute("value") or "")).strip()
        if text.strip() not in val:
            ok = False
    except Exception:
        ok = False

    log(f"[caption] {'OK' if ok else 'FALHA'} ao fixar legenda.")
    return ok

def _type_in(el, text: str):
    try:
        el.click()
        el.send_keys(text)
        return True
    except Exception:
        return False

def _type_via_js_in(drv, el, text: str):
    try:
        drv.execute_script("""
            const el = arguments[0], text = arguments[1];
            el.focus();
            try{ document.execCommand('selectAll', false, null); document.execCommand('delete', false, null);}catch(e){}
            try{ document.execCommand('insertText', false, text); }
            catch(e){
                const dt = new DataTransfer(); dt.setData('text/plain', text);
                el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
            }
            el.dispatchEvent(new InputEvent('input', {bubbles:true}));
        """, el, text)
        return True
    except Exception:
        return False

# ---------- BOTÃO ENVIAR ----------
def _find_send_button(drv):
    """
    Botão ENVIAR dentro do preview — cobre variações novas/antigas.
    """
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='media-editor-send']"),
        (By.CSS_SELECTOR, "[data-testid='media-send']"),
        (By.CSS_SELECTOR, "div[role='dialog'] [data-testid*='send']"),
        (By.CSS_SELECTOR, "div[role='dialog'] button[aria-label*='Enviar']"),
        (By.XPATH, "//*[contains(@data-icon,'send')]/ancestor::*[self::button or self::div][1]"),
        (By.CSS_SELECTOR, "footer [data-testid*='send']"),
    ]
    return _find_first_displayed(drv, candidates)


def _wait_send_enabled(drv, timeout=90):
    """
    Espera o botão ENVIAR habilitar (upload concluído).
    **VERSÃO AGRESSIVA**:
      - Tenta clicar mesmo que aria-disabled esteja 'true' (há builds que não atualizam o atributo).
      - Re-testa a cada iteração e confirma por fechamento do preview/entrada de nova mensagem.
    Retorna (btn, clicked_now) — clicked_now=True se o clique foi feito durante a espera.
    """
    end = time.time() + timeout
    last_log = 0
    clicked = False

    while time.time() < end:
        btn = _find_send_button(drv)
        if btn:
            try:
                disabled = (btn.get_attribute("aria-disabled") or "").lower()
            except Exception:
                disabled = ""

            # se já parecer habilitado, retorna direto
            if disabled in ("", "false"):
                return btn, False

            # TENTA CLIQUE MESMO ASSIM (alguns layouts aceitam)
            if _click(btn, drv):
                clicked = True
                # dá um respiro e verifica se o preview fechou
                for _ in range(12):
                    time.sleep(0.25)
                    if not _preview_open(drv):
                        return btn, True
                # se não fechou, continua tentando até o timeout
        if time.time() - last_log > 5:
            log("[attach] aguardando botão ENVIAR habilitar…")
            last_log = time.time()
        time.sleep(0.35)

    # tempo esgotado — devolve o último botão visto (pode estar clicável)
    return _find_send_button(drv), clicked

def _click(btn, drv):
    try:
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        btn.click()
        return True
    except Exception:
        try:
            drv.execute_script("arguments[0].click()", btn); return True
        except Exception:
            return False


# =================== envio de texto ===================
def _send_text_only(drv: webdriver.Chrome, text: str, allow_duplicates: bool = False) -> None:
    """
    Envia apenas TEXTO no chat atual (sem anexo).
    - Se allow_duplicates=True, ignora os guards anti-duplicidade e envia mesmo que igual ao último.
    """
    text = (text or "").strip()
    if not text:
        return

    try:
        chat_id = _get_chat_id(drv)

        # --- anti-duplicidade (pula se for legenda pós-anexo) ---
        if not allow_duplicates:
            if _has_outgoing_text(drv, text):
                log("[texto] Já existe bolha igual no chat. Ignorado.")
                _clear_composer(drv)
                return
            if not _chat_guard_should_send(chat_id, text):
                log("[texto] Ignorado (guard por chat).")
                _clear_composer(drv)
                return

        # Encontra SEMPRE o composer do FOOTER (evita cair na busca do topo)
        box = _get_chat_box(drv)  # já faz foco no footer
        drv.execute_script("arguments[0].focus();", box)

        # Limpa qualquer resíduo
        try:
            box.send_keys(Keys.CONTROL, 'a'); box.send_keys(Keys.DELETE)
        except Exception:
            pass

        # Insere texto via JS (mais confiável que send_keys nos builds recentes)
        try:
            drv.execute_script("""
                const el = arguments[0], txt = arguments[1];
                if (el.tagName === 'P' || el.getAttribute('data-lexical-editor') === 'true') {
                  el.textContent = txt;
                } else {
                  el.innerText = txt;
                }
                el.dispatchEvent(new InputEvent('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            """, box, text)
        except Exception:
            try:
                box.send_keys(text)
            except Exception as e:
                log(f"[texto] Falha ao inserir via JS e teclado: {e}")
                return

        # Reforça se vazio
        try:
            val = ((box.text or "") + (box.get_attribute("innerText") or "") + (box.get_attribute("value") or "")).strip()
            if not val:
                drv.execute_script("""
                    const el = arguments[0], txt = arguments[1];
                    try{
                      const dt = new DataTransfer(); dt.setData('text/plain', txt);
                      el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
                      el.dispatchEvent(new InputEvent('input', {bubbles:true}));
                    }catch(e){}
                """, box, text)
        except Exception:
            pass

        before = _count_messages(drv)

        # Envia
        send_btn = _find_first_displayed(drv, [
            (By.CSS_SELECTOR, "footer [data-testid*='send']"),
            (By.CSS_SELECTOR, "footer button[aria-label*='Enviar']"),
            (By.CSS_SELECTOR, "[data-testid='compose-btn-send']")
        ])
        if send_btn:
            _click(send_btn, drv)
        else:
            try:
                ActionChains(drv).send_keys(Keys.ENTER).perform()
            except Exception:
                pass

        # Confirmação
        sent = False
        end = time.time() + 12
        while time.time() < end:
            time.sleep(0.25)
            if _count_messages(drv) > before or _has_outgoing_text(drv, text):
                sent = True
                break

        if sent:
            log(f"[texto] Mensagem confirmada: {text[:60]}")
        else:
            log(f"[texto] Falha ao confirmar envio de: {text[:60]}")

    except Exception as e:
        log(f"[texto] Falha ao enviar texto: {e}")
    finally:
        _clear_composer(drv)


def _clear_composer(drv):
    try:
        box = _get_chat_box(drv)
        box.send_keys(Keys.CONTROL, 'a'); box.send_keys(Keys.DELETE)
    except Exception:
        pass

def _freeze_composer(drv):
    """Desabilita temporariamente o composer para evitar texto ‘fantasma’."""
    try:
        drv.execute_script("""
          const box = document.querySelector("[data-testid='conversation-compose-box-input']") ||
                      document.querySelector("footer div[contenteditable='true'][data-tab]") ||
                      document.querySelector("div[role='textbox'][contenteditable='true']");
          if (!box) return;
          box.setAttribute("data-msgauto-frozen","1");
          box.setAttribute("contenteditable","false");
          box.style.pointerEvents = "none";
        """)
    except Exception:
        pass

def _unfreeze_composer(drv):
    """Reabilita o composer após o envio do anexo."""
    try:
        drv.execute_script("""
          const box = document.querySelector("[data-testid='conversation-compose-box-input']") ||
                      document.querySelector("footer div[contenteditable='true'][data-tab]") ||
                      document.querySelector("div[role='textbox'][contenteditable='true']");
          if (!box) return;
          if (box.getAttribute("data-msgauto-frozen") === "1") {
            box.setAttribute("contenteditable","true");
            box.style.pointerEvents = "";
            box.removeAttribute("data-msgauto-frozen");
          }
        """)
    except Exception:
        pass

ATTACH_WAIT_SECONDS = 5  # tempo de espera após abrir o menu de anexos

# =================== anexos ===================
def _open_attach_menu(drv: webdriver.Chrome) -> bool:
    """
    Abre o menu do clipe e espera até ATTACH_WAIT_SECONDS para o menu renderizar.
    """
    for by, sel in [
        (By.CSS_SELECTOR, "[data-testid='clip']"),
        (By.CSS_SELECTOR, "button[aria-label*='Anexar']"),
        (By.CSS_SELECTOR, "div[aria-label*='Anexar']"),
        (By.CSS_SELECTOR, "span[data-icon='attach-menu-plus']"),
    ]:
        try:
            eles = drv.find_elements(by, sel)
            eles = [e for e in eles if e.is_displayed()]
            if eles:
                drv.execute_script("arguments[0].click()", eles[0])
                break
        except Exception:
            continue

    # aguarda o menu abrir
    end = time.time() + ATTACH_WAIT_SECONDS
    while time.time() < end:
        try:
            menu = drv.find_elements(By.XPATH, "//*[contains(text(),'Documento')]")
            if menu:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False

def _click_attach_document(drv: webdriver.Chrome):
    """
    Clica no item 'Documento' do menu (layout novo/antigo).
    """
    candidates = [
        (By.XPATH, "//button[.//text()[contains(.,'Documento')]]"),
        (By.XPATH, "//*[contains(@data-testid,'attach')]//*[contains(text(),'Documento')]/ancestor::*[self::button or self::div][1]"),
        (By.CSS_SELECTOR, "[data-testid='attach-document']"),  # legado
        (By.XPATH, "//*[contains(@data-icon,'attach-document')]/ancestor::*[self::button or self::div][1]"),
    ]
    btn = _find_first_displayed(drv, candidates)
    if not btn:
        raise NoSuchElementException("Botão 'Documento' não encontrado no menu.")
    try:
        drv.execute_script("arguments[0].click()", btn)
    except Exception:
        btn.click()
    time.sleep(0.6)  # dá tempo do input ser injetado
        
def _find_document_file_input(drv: webdriver.Chrome):
    """
    Procura o input[type=file] mais adequado para PDF.
    Prioriza o criado após clicar em 'Documento'.
    """
    scopes = [
        "[data-testid='attach-menu']",
        "div[role='dialog']",
        "footer",
        "body",
    ]
    best = None
    best_score = -1
    for scope in scopes:
        try:
            nodes = drv.find_elements(By.CSS_SELECTOR, f"{scope} input[type='file']")
        except Exception:
            nodes = []
        for n in nodes:
            if not n.is_enabled():
                continue
            sc = _score_input_for_pdf(n)
            if sc > best_score:
                best, best_score = n, sc
    if not best or best_score < 0:
        raise NoSuchElementException("Nenhum input[type=file] compatível com PDF foi encontrado.")
    return best


def _attach_one_file(drv: webdriver.Chrome, file_path: str) -> None:
    """
    Abre o menu, clica 'Documento' e envia o arquivo para UM ÚNICO input[type=file]
    compatível com PDF. Não tenta vários inputs para evitar anexos duplicados.
    Aguarda o preview abrir (mas não reanexa).
    """
    if not (file_path and os.path.isfile(file_path)):
        raise FileNotFoundError(file_path)

    _clear_composer(drv)

    if not _open_attach_menu(drv):
        raise RuntimeError("Menu de anexos não abriu.")

    # tenta clicar 'Documento' (layout novo)
    try:
        _click_attach_document(drv)
    except Exception as e:
        log(f"[attach] Aviso: {e} — seguindo mesmo assim.")

    abs_path = os.path.abspath(file_path)

    # escolhe UM input com melhor compatibilidade p/ PDF
    inp = _find_document_file_input(drv)   # usa o scorer de accept/application/pdf
    inp.send_keys(abs_path)

    # espera o preview aparecer; não tenta novo input para não duplicar
    end = time.time() + 8.0
    while time.time() < end:
        if _preview_open(drv):
            return
        time.sleep(0.15)

    # mesmo que não tenha detectado (variação de DOM), seguimos — o _send_file_with_text manda ENTER
    log("[attach] Preview não detectado, mas arquivo enviado ao input; seguiremos para ENTER.")

ENTER_AFTER_ATTACH_SECONDS = 2.0  # espera 2s após anexar e então envia com ENTER

PREVIEW_ENTER_DELAY = 0.25  # pequeno respiro (seg) antes do ENTER

def _get_last_toast_text(drv) -> str:
    try:
        # WhatsApp costuma renderizar como role=status / aria-live=polite
        nodes = drv.find_elements(By.CSS_SELECTOR, "[role='status'], [aria-live='polite'], [data-testid*='toast']")
        nodes = [n for n in nodes if n.is_displayed()]
        for n in nodes[-3:]:
            txt = (n.text or n.get_attribute("innerText") or "").strip()
            if txt:
                return txt
    except Exception:
        pass
    return ""

def _score_input_for_pdf(inp) -> int:
    """Maior score = mais compatível para PDF (Documento)."""
    try:
        acc = (inp.get_attribute("accept") or "").lower()
    except Exception:
        acc = ""
    if "image" in acc or "video" in acc or "audio" in acc:
        return -1  # inputs de midia
    if "application/pdf" in acc or ".pdf" in acc:
        return 100
    if "application/" in acc:
        return 90
    if "*/*" in acc or acc == "":
        return 80
    return 10  # outros aceitáveis



def _send_file_with_text(
    drv: webdriver.Chrome,
    file_path: str,
    text: str,
    send_fallback_text_after: bool = True
) -> None:
    """
    Anexa o arquivo; 2s depois dá ENTER para enviar; depois envia o texto.
    Sem reanexar e sem duplicar miniaturas.
    """
    if not (file_path and os.path.isfile(file_path)):
        raise FileNotFoundError(file_path)

    text = (text or "").strip()
    attempts, last_err = 2, None  # não precisamos de muitas tentativas agora

    for attempt in range(1, attempts + 1):
        try:
            _clear_composer(drv)
            before = _count_messages(drv)

            _attach_one_file(drv, file_path)
            log(f"[attach] Arquivo preparado: {os.path.basename(file_path)} (tentativa {attempt}).")

            # foca o modal (se existir) e dá ENTER após 2s
            try:
                root = _find_modal_root(drv)
                if root:
                    drv.execute_script("arguments[0].focus();", root)
                    try: drv.execute_script("arguments[0].click()", root)
                    except Exception: pass
            except Exception:
                pass

            time.sleep(ENTER_AFTER_ATTACH_SECONDS)
            try:
                ActionChains(drv).send_keys(Keys.ENTER).perform()
                log("[attach] ENVIAR via ENTER (após 2s).")
            except Exception:
                # fallback no botão
                btn = _find_send_button(drv)
                if btn and _click(btn, drv):
                    log("[attach] ENVIAR clicado (fallback botão).")
                else:
                    raise RuntimeError("Não foi possível acionar o envio do preview.")

            # confirma que saiu do preview ou que surgiu nova mensagem
            end = time.time() + 35
            while time.time() < end:
                time.sleep(0.25)
                if not _preview_open(drv) or _count_messages(drv) > before:
                    break

            # envia o texto logo depois
            if text:
                time.sleep(0.5)
                _send_text_only(drv, text, allow_duplicates=True)
                log(f"[texto] Mensagem enviada após anexo: {text[:60]}")

            _clear_composer(drv)
            return

        except Exception as e:
            last_err = e
            log(f"[attach] Falha ao enviar {os.path.basename(file_path)}: {e}")
            _cancel_preview_if_open(drv)
            _clear_composer(drv)
            time.sleep(0.8)

    raise last_err or RuntimeError("Falha ao enviar arquivo.")



def _is_attach_menu_open(drv: webdriver.Chrome) -> bool:
    try:
        return bool(drv.find_elements(By.CSS_SELECTOR, "[data-testid='attach-menu']"))
    except Exception:
        return False

# =================== abrir chat ===================
def _sanitize_chat_state(drv):
    _clear_composer(drv)
    _scroll_bottom(drv)
    _cancel_preview_if_open(drv)
    
def _wait_app_ready(drv: webdriver.Chrome, max_wait: int = 120) -> None:
    """
    Espera o WhatsApp Web estar operacional:
    - QR (canvas) sumiu OU composer disponível OU sidebar de chats renderizada.
    - Robusto contra carregamentos lentos.
    """
    start = time.time()
    while time.time() - start < max_wait:
        try:
            # 1) composer disponível
            if drv.find_elements(By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"):
                return
            # 2) lista de chats carregada
            if drv.find_elements(By.CSS_SELECTOR, "div[role='grid'], [data-testid='chat-list']"):
                return
            # 3) se ainda está no QR, apenas aguarda
            if drv.find_elements(By.CSS_SELECTOR, "canvas[aria-label*='Scan']"):
                # usuário precisa estar logado — deixamos mais tempo
                pass
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutException("WhatsApp Web não ficou pronto a tempo.")


def _open_whatsapp(drv: webdriver.Chrome, timeout: int = 120) -> None:
    """
    Garante uma aba do WhatsApp Web ativa e pronta para interações.
    """
    try:
        if not _find_whatsapp_tab(drv):
            drv.get("https://web.whatsapp.com/")
        # espera o primeiro paint do app/QR
        WebDriverWait(drv, timeout).until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[aria-label*='Scan']")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "div#app, [data-testid='app']"))
            )
        )
        _wait_app_ready(drv, max_wait=timeout)
        log("WhatsApp Web pronto.")
    except Exception as e:
        log(f"Falha ao abrir WhatsApp Web: {e}")
        raise


def _inject_store(drv: webdriver.Chrome) -> bool:
    """
    Injeta (se preciso) o acesso à Store interna do WhatsApp (igual ao whatsapp-web.js).
    Retorna True se window.Store estiver disponível.
    """
    try:
        has_store = drv.execute_script("return !!(window.Store && window.Store.Chat && window.Store.WidFactory);")
        if has_store:
            return True

        # Bootstrap do webpackChunk para expor módulos (técnica usada pelo whatsapp-web.js)
        drv.execute_script("""
            if (!window.Store) {
              window.Store = {};
            }
            (function() {
              const needed = [
                'Chat',
                'WidFactory',
                'Cmd',
              ];
              function load() {
                if (window.Store.Chat && window.Store.WidFactory) return true;
                const wp = window.webpackChunkwhatsapp_web_client || window.webpackChunkbuild || window.webpackChunkwhatsapp || [];
                if (!wp.push) return false;
                const id = Date.now();
                wp.push([[id], {}, function(o) {
                  const modules = [];
                  for (let idx in o.m) { modules.push(o(idx)); }
                  for (const mod of modules) {
                    if (!mod) continue;
                    // tenta encontrar objetos chave
                    if (mod && mod.default && mod.default.Chat && !window.Store.Chat) {
                      window.Store.Chat = mod.default.Chat;
                    }
                    if (mod && mod.WidFactory && !window.Store.WidFactory) {
                      window.Store.WidFactory = mod.WidFactory;
                    }
                    if (mod && mod.Cmd && !window.Store.Cmd) {
                      window.Store.Cmd = mod.Cmd;
                    }
                    if (window.Store.Chat && window.Store.WidFactory) break;
                  }
                }]);
                return !!(window.Store.Chat && window.Store.WidFactory);
              }
              load();
            })();
        """)
        has_store2 = drv.execute_script("return !!(window.Store && window.Store.Chat && window.Store.WidFactory);")
        return bool(has_store2)
    except Exception:
        return False


def _open_number_chat(drv: webdriver.Chrome, number: str, warm_text: str = "") -> None:
    """
    Abre o chat do número:
    1) Tenta API interna (Store)
    2) Fallback com URL (send?phone=)
    3) Fallback via busca interna
    Em caso de falha (ex.: tela de QR), espera 10s e tenta novamente (1 retry).
    """
    number = (number or "").strip()
    if not number:
        raise ValueError("Número vazio.")

    def _try_open():
        # Garante app aberto e pronto (sem bloquear em login)
        _open_whatsapp(drv, timeout=120)

        # --- 1) API interna ---
        try:
            if _inject_store(drv):
                log(f"[chat] (Store) Abrindo {number}…")
                drv.execute_script("""
                    const num = arguments[0].replace(/[^0-9]/g,'');
                    const warm = arguments[1] || '';
                    const wid = window.Store.WidFactory.createWid(num + "@c.us");
                    return window.Store.Chat.find(wid)
                        .then(c => c || window.Store.Chat.findOrCreateChat(wid))
                        .then(chat => {
                            if (warm) {
                                chat.sendMessage && chat.sendMessage(warm, {linkPreview:false, quotedMsg:null});
                            }
                            if (window.Store.Cmd && window.Store.Cmd.openChatAt) {
                                window.Store.Cmd.openChatAt(chat);
                            }
                            return true;
                        });
                """, number, "")
                WebDriverWait(drv, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"))
                )
                _sanitize_chat_state(drv)
                log(f"[chat] Conversa {number} aberta (Store).")
                return True
            else:
                log("[chat] Store não disponível, usando fallback.")
        except Exception as e:
            log(f"[chat] Store falhou: {e} — tentando fallback URL.")

        # --- 2) Fallback por URL ---
        try:
            js_url = f"https://web.whatsapp.com/send?phone={re.sub('[^0-9]','',number)}"
            drv.execute_script("window.location.assign(arguments[0]);", js_url)
            WebDriverWait(drv, 90).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"))
            )
            _sanitize_chat_state(drv)
            log(f"[chat] Conversa {number} aberta (URL).")
            return True
        except Exception as e:
            log(f"[chat] Fallback URL falhou: {e} — tentando busca interna.")

        # --- 3) Fallback por busca ---
        try:
            search = _find_first_displayed(drv, [
                (By.CSS_SELECTOR, "header input[type='text']"),
                (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
            ])
            if not search:
                raise NoSuchElementException("Campo de busca não encontrado.")
            search.click()
            for _ in range(20): search.send_keys(Keys.BACKSPACE)
            digits = re.sub(r"[^0-9]", "", number)
            search.send_keys(digits); time.sleep(1)
            items = drv.find_elements(By.CSS_SELECTOR, "[role='listitem']")
            for it in items:
                try:
                    t = it.find_element(By.CSS_SELECTOR, "span[title]")
                    title = (t.get_attribute("title") or t.text or "")
                    if digits[-8:] in re.sub(r"[^0-9]", "", title):
                        drv.execute_script("arguments[0].click()", it)
                        WebDriverWait(drv, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input']"))
                        )
                        _sanitize_chat_state(drv)
                        log(f"[chat] Conversa {number} aberta (busca).")
                        return True
                except Exception:
                    continue
            raise NoSuchElementException("Contato não localizado na busca.")
        except Exception as e:
            log(f"[chat] Busca interna falhou: {e}")
            return False

    # Tenta abrir; se falhar (ex.: QR), espera 10s e tenta novamente
    if _try_open():
        return
    log("[chat] Primeira tentativa falhou — aguardando 10s e tentando novamente…")
    time.sleep(10)
    if not _try_open():
        raise RuntimeError(f"Não foi possível abrir a conversa {number} após retry.")

    
def _refresh_current_chat(drv: webdriver.Chrome, timeout: int = 60) -> None:
    """Recarrega a conversa e espera até o campo de texto estar funcional."""
    try:
        url = drv.current_url or ""
        log("[chat] Atualizando conversa...")
        try:
            drv.execute_script("location.reload()")
        except Exception:
            if url:
                drv.get(url)

        # Espera campo de digitação reaparecer
        WebDriverWait(drv, timeout).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"
            ))
        )
        time.sleep(1.5)
        _sanitize_chat_state(drv)
        log("[chat] Conversa atualizada e pronta.")
    except Exception as e:
        log(f"[chat] Falha ao atualizar conversa: {e}")


def _open_group_chat(drv: webdriver.Chrome, group_name: str) -> None:
    """Abre o chat de um grupo no WhatsApp Web."""
    import unicodedata, re
    def normalize(s: str):
        s = (s or "").strip().lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))
        return re.sub(r"\s+", " ", s)

    target = normalize(group_name)
    if not target:
        raise ValueError("Nome do grupo vazio.")

    _open_whatsapp(drv)

    # tenta via Store (abrir direto o chat)
    try:
        if _inject_store(drv):
            ok = drv.execute_script("""
                const wanted = arguments[0];
                const norm = t => (t||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/\s+/g,' ').trim();
                const chats = (window.Store && window.Store.Chat && window.Store.Chat._models) ? window.Store.Chat._models : [];
                for (const c of chats){
                    const name = norm(c.formattedTitle || c.formattedName || c.name || c.contact?.name || '');
                    if (name && (name===wanted || name.includes(wanted))){
                        if (window.Store.Cmd && window.Store.Cmd.openChatAt){
                            window.Store.Cmd.openChatAt(c);
                        } else { c.markOpened && c.markOpened(); }
                        return true;
                    }
                }
                return false;
            """, target)
            if ok:
                WebDriverWait(drv, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab], [data-testid='conversation-compose-box-input']"))
                )
                _sanitize_chat_state(drv)
                log(f"[grupo] Conversa '{group_name}' aberta (Store).")
                return
    except Exception as e:
        log(f"[grupo] Store falhou: {e}")

    # fallback: busca lateral e clique
    try:
        search_box = None
        for by, sel in [
            (By.CSS_SELECTOR, "aside [contenteditable='true'][data-tab]"),
            (By.CSS_SELECTOR, "[aria-label*='Pesquisar'] [contenteditable='true']"),
            (By.CSS_SELECTOR, "header div[role='textbox'][contenteditable='true']"),
            (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
        ]:
            els = drv.find_elements(by, sel)
            els = [e for e in els if e.is_displayed()]
            for el in els:
                anc = drv.execute_script("return arguments[0].closest('footer')", el)
                if not anc:
                    search_box = el
                    break
            if search_box:
                break

        if not search_box:
            raise NoSuchElementException("Campo de busca não encontrado.")

        search_box.click()
        search_box.send_keys(Keys.CONTROL, 'a')
        search_box.send_keys(Keys.DELETE)
        search_box.send_keys(group_name)
        time.sleep(1.2)

        rows = drv.find_elements(By.CSS_SELECTOR, "[data-testid='cell-frame-container'], [role='listitem']")
        rows = [r for r in rows if r.is_displayed()]
        clicked = False
        for r in rows:
            try:
                titles = r.find_elements(By.CSS_SELECTOR, "span[title], div[title]")
                for t in titles:
                    title = (t.get_attribute("title") or t.text or "").strip()
                    if not title:
                        continue
                    if normalize(title) == target or target in normalize(title):
                        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", t)
                        try:
                            t.click()
                        except Exception:
                            drv.execute_script("arguments[0].click()", r)
                        clicked = True
                        break
                if clicked:
                    break
            except Exception:
                continue

        if not clicked:
            try:
                search_box.send_keys(Keys.ENTER)
                clicked = True
            except Exception:
                pass

        if not clicked:
            raise NoSuchElementException(f"Grupo '{group_name}' não encontrado na busca.")

        WebDriverWait(drv, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab], [data-testid='conversation-compose-box-input']"))
        )
        _sanitize_chat_state(drv)
        log(f"[grupo] Conversa '{group_name}' aberta e pronta.")
    except Exception as e:
        log(f"[grupo] Falha ao abrir grupo: {e}")
        raise


# =================== anexos/legendas ===================
def _list_files_in_folder(folder: str) -> List[str]:
    """
    Lista arquivos 'ao vivo' de uma pasta do sistema (sem cache),
    retornando ordenado por data de modificação ascendente.
    """
    if not folder:
        return []
    folder = os.path.abspath(folder)
    if not os.path.isdir(folder):
        return []
    try:
        files = [os.path.join(folder, nm) for nm in os.listdir(folder)
                 if os.path.isfile(os.path.join(folder, nm))]
        files.sort(key=lambda p: os.path.getmtime(p))
        return files
    except Exception:
        return []

def _is_snapshot_folder(path: str) -> bool:
    try:
        p = os.path.abspath(path or "")
        return os.path.commonpath([p, UPLOAD_DIR]) == UPLOAD_DIR
    except Exception:
        return False

def _dedupe_keep_newest(paths: List[str]) -> List[str]:
    best: Dict[str, Tuple[float, str]] = {}
    for p in paths:
        try:
            bn = os.path.basename(p)
            mt = os.path.getmtime(p)
            cur = best.get(bn)
            if (cur is None) or (mt > cur[0]):
                best[bn] = (mt, p)
        except Exception:
            continue
    return [v[1] for _, v in sorted(best.items(), key=lambda kv: kv[1][0])]

def _resnapshot_folder(origin: str, dest_root: str) -> int:
    """
    Repopula o snapshot (dest_root) com os arquivos atuais da pasta de origem.
    * Achata a hierarquia (copia apenas arquivos do nível raiz da origem).
    """
    if not (origin and os.path.isdir(origin)):
        return 0
    try:
        os.makedirs(dest_root, exist_ok=True)
        # Limpa destino
        for nm in os.listdir(dest_root):
            p = os.path.join(dest_root, nm)
            try:
                if os.path.isdir(p): shutil.rmtree(p, ignore_errors=True)
                else: os.remove(p)
            except Exception:
                pass
        count = 0
        for nm in os.listdir(origin):
            src = os.path.join(origin, nm)
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(dest_root, os.path.basename(nm)))
                count += 1
        return count
    except Exception as e:
        log(f"[resnapshot] Falha ao reimportar '{origin}' → '{dest_root}': {e}")
        return 0

# ===== Reimportação AUTOMÁTICA (ignora autosync) =====
def _resnapshot_item_folder_force(item: Dict[str,Any]) -> None:
    """
    Reimporta SEMPRE o item de pasta, se:
    - 'path' aponta para snapshot (uploads_msg/…)
    - 'origin' está preenchido e existe
    """
    if (item.get("type") != "folder"):
        return
    path   = (item.get("path") or "").strip()
    origin = (item.get("origin") or "").strip()
    if not (path and origin):
        return
    if not _is_snapshot_folder(path):
        return
    if not os.path.isdir(origin):
        log(f"[resync-item] Origem inexistente: {origin}")
        return
    n = _resnapshot_folder(origin, path)
    log(f"[resync-item] Pasta item atualizada: {n} arquivo(s) de '{origin}' → '{path}'.")

def _resnapshot_general_force(cfg: Dict[str,Any]) -> None:
    """
    Reimporta SEMPRE a pasta geral (se modo for folder/both), se:
    - 'attachments_folder' é snapshot
    - 'general_folder_origin' existe
    """
    if cfg.get("attachments_mode") not in ("folder", "both"):
        return
    dest   = (cfg.get("attachments_folder") or "").strip()
    origin = (cfg.get("general_folder_origin") or "").strip()
    if not (dest and origin):
        return
    if not _is_snapshot_folder(dest):
        return
    if not os.path.isdir(origin):
        log(f"[resync-geral] Origem inexistente: {origin}")
        return
    n = _resnapshot_folder(origin, dest)
    log(f"[resync-geral] Pasta geral atualizada: {n} arquivo(s) de '{origin}' → '{dest}'.")

# ========= NOVO: reimporta TUDO no começo de cada execução =========
def __resync_all_folders_force(cfg: Dict[str, Any]) -> None:
    """
    1) Reimporta a PASTA GERAL (se origin válido e snapshot).
    2) Reimporta CADA ITEM do tipo pasta (se origin válido e snapshot).
    Ignora flags de autosync.
    """
    _resnapshot_general_force(cfg)
    for it in (cfg.get("custom_items") or []):
        _resnapshot_item_folder_force(it)

def _resolve_general_attachments(cfg: Dict[str, Any]) -> List[str]:
    mode = cfg.get("attachments_mode", "files")
    atts: List[str] = []

    # uploads explícitos (fixos)
    if mode in ("files", "both"):
        atts.extend([p for p in (cfg.get("attachments") or []) if os.path.isfile(p)])

    # pasta dinâmica geral (pode ser snapshot; já foi reimportada)
    if mode in ("folder", "both"):
        atts.extend(_list_files_in_folder(cfg.get("attachments_folder", "")))

    atts = _dedupe_keep_newest(atts)

    seen = set(); out = []
    for p in atts:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def _caption_for_file(cfg: Dict[str,Any], file_path: str) -> str:
    base = os.path.basename(file_path)
    name_no_ext = os.path.splitext(base)[0]
    caps: Dict[str,str] = cfg.get("file_captions") or {}
    # 1) legenda específica por nome exato
    if (caps.get(base) or "").strip():
        return caps[base].strip()
    # 2) legenda por nome sem extensão
    if (caps.get(name_no_ext) or "").strip():
        return caps[name_no_ext].strip()
    # 3) curinga *
    if (caps.get("*") or "").strip():
        return caps["*"].strip()
    # 4) padrão (__DEFAULT__) se existir (linha sem separador no formulário)
    if (caps.get("__DEFAULT__") or "").strip():
        return caps["__DEFAULT__"].strip()
    # 5) fallback inteligente
    m = re.search(r'(\d+)', name_no_ext)
    if m:
        return f"Indicador {m.group(1)}"
    return name_no_ext

# =================== regra de disparo por ITEM ===================
def _items_due(cfg: Dict[str,Any], force: bool=False, allow_general_now: bool=False) -> List[Dict[str,Any]]:
    """
    Itens com intervalo individual (>0) só disparam quando 'now >= item_states[iid].next_due'.
    Itens sem intervalo seguem a batida geral (allow_general_now=True).
    """
    if force:
        out = []
        for it in (cfg.get("custom_items") or []):
            p = (it.get("path") or "").strip()
            t = (it.get("type") or "file").lower()
            if (t == "file" and os.path.isfile(p)) or (t == "folder" and os.path.isdir(p)):
                out.append(it)
        log(f"[itens_due] FORÇADO: {len(out)} item(ns).")
        return out

    now = datetime.now()
    states = cfg.setdefault("item_states", {})

    try:
        freq_global = max(1, int(cfg.get("frequency_minutes") or 60))
    except Exception:
        freq_global = 60

    due: List[Dict[str,Any]] = []

    for it in (cfg.get("custom_items") or []):
        iid = it.get("id") or (it.setdefault("id", uuid.uuid4().hex))
        pth = (it.get("path") or "").strip()
        tpe = (it.get("type") or "file").lower()
        if tpe == "file" and not os.path.isfile(pth):
            continue
        if tpe == "folder" and not os.path.isdir(pth):
            continue

        try:
            individual = int(it.get("interval") or 0)
        except Exception:
            individual = 0

        st = states.get(iid) or {}
        states[iid] = st  # garante chave

        # Intervalo INDIVIDUAL: usa pré-agenda própria (st['next_due'])
        if individual > 0:
            nd_str = st.get("next_due")
            if not nd_str:
                # inicializa a pré-agenda individual sem disparar agora
                base = _ceil_next_tick(now, individual)
                st["next_due"] = base.strftime("%Y-%m-%d %H:%M:%S")
                continue
            try:
                nd = datetime.strptime(nd_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                nd = _ceil_next_tick(now, individual)
                st["next_due"] = nd.strftime("%Y-%m-%d %H:%M:%S")
                continue
            if now >= nd:
                due.append(it)
            continue

        # Sem intervalo: só libera na batida GERAL
        if allow_general_now:
            due.append(it)

    if due:
        save_cfg(cfg)
    return due

def _items_due(cfg: Dict[str,Any], force: bool=False, global_cycle: bool=False) -> List[Dict[str,Any]]:
    if force:
        out = []
        for it in (cfg.get("custom_items") or []):
            path = (it.get("path") or "").strip()
            tpe  = (it.get("type") or "file").lower()
            if (tpe == "file" and os.path.isfile(path)) or (tpe == "folder" and os.path.isdir(path)):
                out.append(it)
        return out

    now    = datetime.now()
    states = cfg.get("item_states") or {}
    due: List[Dict[str,Any]] = []

    for it in (cfg.get("custom_items") or []):
        iid = str(it.get("id") or "")
        if not iid:
            continue
        path = (it.get("path") or "").strip()
        tpe  = (it.get("type") or "file").lower()
        if tpe == "file" and not os.path.isfile(path):
            continue
        if tpe == "folder" and not os.path.isdir(path):
            continue

        try:
            interval = int(it.get("interval") or 0)
        except Exception:
            interval = 0

        if interval > 0:
            # individual: só quando next_due venceu
            nd_s = (states.get(iid) or {}).get("next_due")
            if not nd_s:
                continue
            try:
                nd = datetime.strptime(nd_s, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if now >= nd:
                due.append(it)
        else:
            # sem intervalo: só sai no ciclo geral
            if global_cycle:
                due.append(it)

    return due

def perform_send(cfg: Dict[str, Any], force: bool=False, global_cycle: bool=False) -> Tuple[bool, str]:
    __resync_all_folders_force(cfg)

    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."
    if not _now_in_window(cfg):
        return False, "Fora da janela de funcionamento."

    items_due = _items_due(cfg, force=force, global_cycle=global_cycle)
    ...


def _mark_item_sent(cfg: Dict[str,Any], item_id: str, interval_minutes: Optional[int]=None) -> None:
    st = cfg.setdefault("item_states", {}).get(item_id) or {}
    now = datetime.now()
    st["last_sent"] = now.strftime("%Y-%m-%d %H:%M:%S")
    if interval_minutes and int(interval_minutes) > 0:
        next_dt = _reschedule_from_cycle_end(now, int(interval_minutes), cfg)
        st["next_due"] = next_dt.strftime("%Y-%m-%d %H:%M:%S")
    cfg.setdefault("item_states", {})[item_id] = st
    save_cfg(cfg)


# =================== disparo ===================
def _send_items_to_chat(drv: webdriver.Chrome, cfg: Dict[str,Any], label:str,
                        items: List[Dict[str,Any]], sent_paths: set) -> None:
    base_text = (cfg.get("message_text") or "").strip()

    if base_text:
        _send_text_only(drv, base_text, allow_duplicates=False)

    for item in items:
        itype = (item.get("type") or "file").lower()
        path  = (item.get("path") or "").strip()
        text  = (item.get("text") or "").strip()
        iid   = item.get("id")

        try:
            try:
                el = _get_chat_box(drv)
                drv.execute_script("arguments[0].focus();", el)
            except Exception:
                pass

            if itype == "file":
                if not os.path.isfile(path):
                    log(f"[{label}] Item ignorado (arquivo não encontrado): {path}")
                elif path in sent_paths:
                    log(f"[{label}] Item ignorado (já enviado neste ciclo): {os.path.basename(path)}")
                else:
                    cap = text or _caption_for_file(cfg, path)
                    _send_file_with_text(drv, path, cap)
                    sent_paths.add(path)
                    if iid:
                        _mark_item_sent(cfg, iid, item.get("interval"))

            elif itype == "folder":
                if not os.path.isdir(path):
                    log(f"[{label}] Item ignorado (pasta não encontrada): {path}")
                else:
                    files = _dedupe_keep_newest(_list_files_in_folder(path))
                    if not files:
                        log(f"[{label}] Pasta vazia: {path}")
                    for fp in files:
                        if not os.path.isfile(fp) or fp in sent_paths:
                            continue
                        try:
                            el = _get_chat_box(drv)
                            drv.execute_script("arguments[0].focus();", el)
                        except Exception:
                            pass
                        cap = text or _caption_for_file(cfg, fp)
                        _send_file_with_text(drv, fp, cap)
                        sent_paths.add(fp)
                        time.sleep(0.25)
                    if iid:
                        _mark_item_sent(cfg, iid, item.get("interval"))
            else:
                log(f"[{label}] Tipo inválido no item: {itype!r}")

        except Exception as e:
            log(f"[{label}] Falha ao enviar item {path}: {e}")
            try:
                _sanitize_chat_state(drv)
            except Exception:
                pass
            continue

        time.sleep(0.35)


def _send_all_to_chat(drv: webdriver.Chrome, cfg: Dict[str,Any], label:str, items_due: List[Dict[str,Any]]) -> None:
    base_text = (cfg.get("message_text") or "").strip()

    general_files = _resolve_general_attachments(cfg)

    if base_text:
        _send_text_only(drv, base_text, allow_duplicates=False)
        log(f"[{label}] Texto avulso enviado (primeiro).")

    sent_paths: set = set()
    for fp in general_files:
        if not os.path.isfile(fp) or fp in sent_paths:
            continue
        try:
            caption = _caption_for_file(cfg, fp)
            _send_file_with_text(drv, fp, caption)
            sent_paths.add(fp)
            time.sleep(0.35)
        except Exception as e:
            log(f"[{label}] Falha anexo geral {fp}: {e}")

    if items_due:
        _send_items_to_chat(drv, cfg, label, items_due, sent_paths)



def _send_everything_for_number(drv: webdriver.Chrome, number: str, cfg: Dict[str, Any], items_due: List[Dict[str, Any]]) -> None:
    """
    Abre o chat do número informado e envia todos os anexos e mensagens configurados.
    """
    try:
        _open_number_chat(drv, number, "")
        _send_all_to_chat(drv, cfg, number, items_due)
        log(f"[{number}] Envio concluído com sucesso.")
    except Exception as e:
        log(f"[{number}] Erro no envio: {e}")
        try:
            _sanitize_chat_state(drv)
        except Exception:
            pass


def _send_everything_for_group(drv: webdriver.Chrome, group: str, cfg: Dict[str,Any], items_due: List[Dict[str,Any]]) -> None:
    _open_whatsapp(drv); _open_group_chat(drv, group)
    _send_all_to_chat(drv, cfg, group, items_due)

def perform_send(cfg: Dict[str, Any], force: bool=False) -> Tuple[bool, str]:
    # Reimportação de pastas no início do ciclo
    __resync_all_folders_force(cfg)

    # Sem destinos ou fora da janela → nada a fazer
    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."
    if not _now_in_window(cfg):
        return False, "Fora da janela de funcionamento."

    now = datetime.now()
    try:
        freq_global = max(1, int(cfg.get("frequency_minutes") or 60))
    except Exception:
        freq_global = 60

    # Controle da batida global
    next_due_str = cfg.get("next_global_due")
    if not next_due_str:
        # Se não existir (caso legado), cria a partir de agora
        nd = _ceil_next_tick(now, freq_global)
        # Se a próxima batida cair fora da janela, empurra até cair dentro
        while not _within_time_window(cfg, nd):
            nd += timedelta(minutes=freq_global)
        cfg["next_global_due"] = nd.strftime("%Y-%m-%d %H:%M:%S")
        save_cfg(cfg)
        next_due = nd
    else:
        try:
            next_due = datetime.strptime(next_due_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            nd = _ceil_next_tick(now, freq_global)
            cfg["next_global_due"] = nd.strftime("%Y-%m-%d %H:%M:%S")
            save_cfg(cfg)
            next_due = nd

    allow_general_now = (now >= next_due)

    # Seleciona itens
    items_due = _items_due(cfg, force=force, allow_general_now=allow_general_now)

    # Se nada estiver devido e não é batida global, apenas sai
    if not items_due and not force and not allow_general_now:
        return True, "Nada devido neste minuto."

    global _driver, _driver_opened_by_app
    with _driver_lock:
        try:
            drv = _get_driver(cfg.get("run_mode","visible"))
            _open_whatsapp(drv)
            if not _is_logged_in(drv):
                log("Aguardando login (QR) no WhatsApp Web... 10s")
                time.sleep(10)
        except Exception as e:
            log(f"Erro no WhatsApp: {e}")
            try:
                if _driver_opened_by_app and _driver: _driver.quit()
            except Exception:
                pass
            _driver = None; _driver_opened_by_app=False
            return False, f"Erro no WhatsApp Web: {e}"

        # Dispara para números
        for num in list(cfg.get("numbers") or []):
            try:
                _send_everything_for_number(drv, num, cfg, items_due)
            except Exception as e:
                log(f"Falha no número {num}: {e}")

        # Dispara para grupos
        for grp in list(cfg.get("groups") or []):
            try:
                _send_everything_for_group(drv, grp, cfg, items_due)
            except Exception as e:
                log(f"Falha no grupo '{grp}': {e}")

        if cfg.get("close_after_send", True) and _driver_opened_by_app:
            try: drv.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            log("Navegador fechado (aberto pelo app).")

    # Se executamos na batida global (mesmo que não houvesse item geral),
    # avançamos a próxima agenda em passos de freq até ficar no futuro e dentro da janela.
    if allow_general_now:
        nd = next_due
        while nd <= now or not _within_time_window(cfg, nd):
            nd += timedelta(minutes=freq_global)
        cfg["next_global_due"] = nd.strftime("%Y-%m-%d %H:%M:%S")
        save_cfg(cfg)
        log(f"[agenda] Próxima execução geral: {cfg['next_global_due']}")

    return True, "Ciclo de envio concluído."


# =================== Agendador ===================
def _schedule(cfg: Dict[str, Any]) -> None:
    try:
        j = scheduler.get_job(_job_id)
        if j:
            scheduler.remove_job(_job_id)
    except Exception:
        pass

    if not cfg.get("enabled"):
        log("Mensageiro está DESLIGADO.")
        return

    states = _build_item_states_on_enable(cfg)
    log(f"[agenda] Ligado às {cfg['enabled_at']}. Próxima execução geral: {cfg['next_global_due']}")

    # lista das pré-agendas individuais
    for it in (cfg.get("custom_items") or []):
        try:
            if int(it.get("interval") or 0) > 0:
                st = states.get(str(it.get("id") or ""), {})
                if st.get("next_due"):
                    log(f"[agenda-ind] {os.path.basename(it.get('path',''))} → {st['next_due']}")
        except Exception:
            pass

    scheduler.add_job(
        func=_job_wrapper,
        trigger=IntervalTrigger(minutes=1),
        id=_job_id,
        max_instances=1,
        coalesce=True,
        replace_existing=True
    )
    log("Mensageiro LIGADO – verificação a cada 1 minuto (intervalos individuais ativos).")

def perform_send(cfg: Dict[str, Any], force: bool = False, global_cycle: bool = False) -> Tuple[bool, str]:
    """
    Ciclo de envio.
      - force=True  => envia tudo que for válido (ignora agendas).
      - global_cycle=True  => ciclo GLOBAL (texto/arquivos gerais + itens SEM intervalo).
      - global_cycle=False => ciclo INDIVIDUAL (apenas itens com intervalo que venceram).
    """
    # Reimporta pastas no começo do ciclo (como já estava)
    __resync_all_folders_force(cfg)

    # Janela de funcionamento
    if not _now_in_window(cfg) and not force:
        return False, "Fora da janela de funcionamento."

    # Decisão do QUE enviar neste ciclo
    due_items = _items_due(cfg, force=force, global_cycle=global_cycle)

    # Se não há nada devido (nem forçado), não abre WhatsApp à toa
    if not due_items and not (force and (cfg.get("numbers") or cfg.get("groups"))):
        # LOG de diagnóstico de próximas janelas
        ind_next = _next_individual_due(cfg)
        gd_next  = _dtparse(cfg.get("next_global_due"))
        nxt_str  = (ind_next or gd_next)
        if nxt_str:
            return False, f"Aguardando janela: próximo às {(nxt_str).strftime('%Y-%m-%d %H:%M:%S')}"
        return False, "Nada devido para enviar."

    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."

    # ------ prepara Selenium uma única vez ------
    global _driver, _driver_opened_by_app
    with _driver_lock:
        try:
            drv = _get_driver(cfg.get("run_mode","visible"))
            _open_whatsapp(drv)  # garante app carregado
            if not _is_logged_in(drv):
                log("Aguardando login (QR) no WhatsApp Web... espera 10s")
                time.sleep(10)
        except Exception as e:
            log(f"Erro no WhatsApp: {e}")
            try:
                if _driver_opened_by_app and _driver: _driver.quit()
            except Exception:
                pass
            _driver = None; _driver_opened_by_app = False
            return False, f"Erro no WhatsApp Web: {e}"

        # --------- ENVIO: NÚMEROS primeiro, depois GRUPOS ----------
        # No ciclo GLOBAL: texto/arquivos gerais + itens sem intervalo (due_items)
        # No ciclo INDIVIDUAL: somente due_items (sem texto/arquivos gerais)
        def _send_to_target_list(open_fn, label_fn):
            """open_fn abre o chat (número ou grupo); label_fn devolve o rótulo do destino."""
            for target in list(label_fn["list"]) or []:
                try:
                    open_fn(drv, target)
                    if global_cycle and not force:
                        # GLOBAL: texto/arquivos gerais (+ itens sem intervalo)
                        _send_all_to_chat(drv, cfg, target, due_items)
                    else:
                        # INDIVIDUAL (ou force): SOMENTE itens previstos
                        sent_paths: set = set()
                        _send_items_to_chat(drv, cfg, target, due_items, sent_paths)
                    log(f"[{target}] Envio concluído com sucesso.")
                except Exception as e:
                    log(f"Falha no destino {target}: {e}")
                    try:
                        _sanitize_chat_state(drv)
                    except Exception:
                        pass

        # NÚMEROS
        _send_to_target_list(
            open_fn=lambda d, n: _open_number_chat(d, n, ""),
            label_fn={"list": cfg.get("numbers") or []}
        )
        # GRUPOS
        _send_to_target_list(
            open_fn=lambda d, g: (_open_whatsapp(d), _open_group_chat(d, g)),
            label_fn={"list": cfg.get("groups") or []}
        )

        # fecha navegador se foi aberto pelo app
        if cfg.get("close_after_send", True) and _driver_opened_by_app:
            try: drv.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app = False
            log("Navegador fechado (aberto pelo app).")

    return True, "Ciclo de envio concluído."

# --- JOB: roda a cada 1 min, mas SÓ dispara quando vence a agenda
def _job_wrapper():
    cfg = load_cfg()
    now = datetime.now()

    ngd = _dtparse(cfg.get("next_global_due"))
    nid = _next_individual_due(cfg)

    # qual vem antes?
    next_tick = None
    if ngd and nid:
        next_tick = min(ngd, nid)
    elif ngd:
        next_tick = ngd
    elif nid:
        next_tick = nid

    if next_tick and now < next_tick:
        log(f"[JOB] Aguardando janela: próximo às {next_tick.strftime('%Y-%m-%d %H:%M:%S')}")
        return

    global_due = bool(ngd and now >= ngd)

    ok, msg = perform_send(cfg, force=False, global_cycle=global_due)
    log(f"[JOB] {msg}")

    # se rodou o ciclo global, re-agenda “último + frequência”
    if global_due:
        try:
            freq = max(1, int(cfg.get("frequency_minutes") or 60))
        except Exception:
            freq = 60
        base = ngd if ngd and now >= ngd else now
        cfg["next_global_due"] = _dtfmt(base + timedelta(minutes=freq))
        save_cfg(cfg)



# --- De-dup para fallback de legenda (arquivo|texto) ---
_sent_caption_fallbacks: Dict[str, float] = {}
_SENT_CAP_TTL = 120.0  # segundos

# --- Anti-duplicidade por chat ---
_chat_recent_sent: Dict[Tuple[str, str], float] = {}
_CHAT_SENT_TTL = 90.0  # segundos

def _normalize(s: str) -> str:
    """lower + sem acentos + espaço único."""
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s


def _get_chat_id(drv) -> str:
    """
    Identifica o chat atual de forma estável (nome do cabeçalho ou phone= da URL).
    """
    try:
        # nome do contato/grupo no header
        t = drv.find_elements(By.CSS_SELECTOR, "header span[title], header div[role='button'] span[title]")
        for el in t:
            if el.is_displayed():
                name = (el.get_attribute("title") or el.text or "").strip()
                if name:
                    return f"name:{name.lower()}"
    except Exception:
        pass
    try:
        url = drv.current_url or ""
        if "web.whatsapp.com" in url:
            return f"url:{url}"
    except Exception:
        pass
    return "unknown"

def _has_outgoing_text(drv, text: str) -> bool:
    """
    Verifica se já existe uma bolha 'message-out' com exatamente esse texto
    visível na conversa (últimas mensagens).
    """
    if not text.strip():
        return False
    try:
        # pega apenas um pedaço do final para ficar rápido
        nodes = drv.find_elements(By.CSS_SELECTOR, "div.message-out")[-12:]
        for n in reversed(nodes):
            try:
                sel = n.find_elements(By.CSS_SELECTOR, "span.selectable-text, div._ao3e")
                for s in sel:
                    val = (s.text or "").strip()
                    if val == text.strip():
                        return True
            except Exception:
                continue
    except Exception:
        pass
    return False

def _chat_guard_should_send(chat_id: str, text: str) -> bool:
    """
    Evita reenvio do mesmo texto no mesmo chat por uma janela curta.
    """
    now = time.time()
    # limpa antigos
    for k, t in list(_chat_recent_sent.items()):
        if now - t > _CHAT_SENT_TTL:
            _chat_recent_sent.pop(k, None)
    key = (chat_id, text.strip())
    if not text.strip():
        return False
    if key in _chat_recent_sent:
        return False
    _chat_recent_sent[key] = now
    return True


def _should_send_caption_fallback(file_path: str, text: str) -> bool:
    """
    Evita duplicidade de texto fallback para o MESMO arquivo+texto
    em janelas curtas (ex.: carregou preview, mandou, não detectou a tempo e tentou de novo).
    """
    if not (file_path and text and text.strip()):
        return False
    key = f"{os.path.abspath(file_path)}|{text.strip()}"
    now = time.time()
    # limpeza
    for k, t in list(_sent_caption_fallbacks.items()):
        if now - t > _SENT_CAP_TTL:
            _sent_caption_fallbacks.pop(k, None)
    if key in _sent_caption_fallbacks:
        return False
    _sent_caption_fallbacks[key] = now
    return True


@app.template_filter("basename")
def _basename_filter(p):
    try: return os.path.basename(p or "")
    except Exception: return p

@app.route("/run_now", methods=["GET", "POST"])
def run_now():
    log("[run_now] Execução manual solicitada.")

    def worker():
        log("[run_now-bg] Execução manual iniciada em segundo plano.")
        try:
            cfg = load_config()
        except Exception as e:
            cfg = {}
            log(f"[run_now-bg] Erro ao carregar config, usando cfg vazio: {e}")
        try:
            ok, msg = perform_send(cfg, force=True, global_cycle=True)
            log(f"[run_now-bg] Finalizada. ok={ok} msg={msg}")
        except Exception as e:
            log(f"[run_now-bg] ERRO na execução manual: {e}")

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    # mensagem para aparecer na tela principal (flash já existia no projeto)
    flash("Execução manual iniciada em segundo plano. Verifique os logs.", "info")

    # volta para a página de onde veio o clique (Status)
    ref = request.headers.get("Referer")
    if ref:
        return redirect(ref)

    # fallback se acessar /run_now direto
    return "Execução manual iniciada em segundo plano. Verifique os logs."
    
@app.route("/")
def index():
    cfg = load_cfg()
    preview = ", ".join(os.path.basename(p) for p in _resolve_general_attachments(cfg)[:10]) or "—"
    return render_template_string(TPL, title=APP_TITLE, page="index",
                                  cfg=cfg, preview_attachments=preview,
                                  profile_dir=PROFILE_DIR)

@app.route("/config")
def config_page():
    cfg = load_cfg()
    folder_list = _list_files_in_folder(cfg.get("attachments_folder",""))[:8]
    folder_preview = ", ".join(os.path.basename(x) for x in folder_list)
    lines = []
    for k,v in (cfg.get("file_captions") or {}).items():
        if k == "__DEFAULT__": lines.append(v)
        else: lines.append(f"{k}|{v}")
    return render_template_string(TPL, title=APP_TITLE, page="config",
                                  cfg=cfg, folder_preview=folder_preview,
                                  file_captions_lines="\n".join(lines))

@app.route("/logs")
def logs_page():
    return render_template_string(TPL, title=APP_TITLE, page="logs", logs="\n".join(_logs))



_recent_texts: Dict[str, float] = {}
_RECENT_TTL = 90.0  # segundos
def _should_send_text(text: str) -> bool:
    global _recent_texts
    now = time.time()
    # limpa antigos
    for k, t in list(_recent_texts.items()):
        if now - t > _RECENT_TTL:
            _recent_texts.pop(k, None)
    key = (text or "").strip()
    if not key:
        return False
    if key in _recent_texts:
        return False
    _recent_texts[key] = now
    return True


@app.route("/clear_logs")
def clear_logs():
    _logs.clear(); flash("Logs limpos.", "success")
    return redirect(url_for("logs_page"))

@app.route("/download_logs")
def download_logs():
    content = "\n".join(_logs).encode("utf-8")
    resp = make_response(content)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=msgauto.log"
    return resp

@app.route("/uploads_msg/<path:fname>")
def serve_upload(fname):
    return send_from_directory(UPLOAD_DIR, fname, as_attachment=True)

# ====== upload de PASTA (snapshot em uploads_msg/) ======
@app.post("/toggle/on")
def toggle_on_route():
    cfg = load_cfg()
    cfg["enabled"] = True
    save_cfg(cfg)

    # Chama o schedule (que já monta a pré-agenda e loga)
    _schedule(cfg)

    flash("Mensageiro LIGADO.", "success")
    return redirect(url_for("index"))

@app.post("/upload_folder")
def upload_folder():
    files = request.files.getlist("folder_files")
    rels  = request.form.getlist("relpaths[]")
    if not files:
        return jsonify({"ok": False, "error": "Nenhum arquivo recebido."}), 400

    subdir = datetime.now().strftime("folder_%Y%m%d_%H%M%S_%f")
    dest_root = os.path.join(UPLOAD_DIR, subdir)
    os.makedirs(dest_root, exist_ok=True)

    count = 0
    for idx, file in enumerate(files):
        rel = rels[idx] if idx < len(rels) else file.filename
        rel = rel.replace("\\", "/")
        if "/" in rel:
            rel = rel.split("/")[-1]
        safe_name = os.path.basename(rel) or file.filename
        dest = os.path.join(dest_root, safe_name)
        try:
            file.save(dest)
            count += 1
        except Exception:
            continue

    return jsonify({"ok": True, "saved_path": dest_root, "count": count})

# ====== seletor NATIVO de pasta (Tkinter) ======
@app.get("/pick_folder_native")
def pick_folder_native():
    """
    Abre um diálogo nativo para escolher pasta e retorna o caminho absoluto.
    Útil para preencher 'Origem (servidor)' de reimportação.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askdirectory(title="Selecione a pasta de ORIGEM (servidor)")
        root.destroy()
        if not path:
            return jsonify({"ok": False, "error": "Seleção cancelada."})
        return jsonify({"ok": True, "path": path})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Não foi possível abrir o seletor nativo: {e}"}), 500

@app.post("/toggle/<action>")
def toggle(action):
    cfg = load_cfg()
    if action == "on":
        cfg["enabled"] = True; save_cfg(cfg); _schedule(cfg)
        flash("Mensageiro LIGADO.", "success")
    else:
        cfg["enabled"] = False; save_cfg(cfg); _schedule(cfg)
        flash("Mensageiro DESLIGADO.", "warning")
    return redirect(url_for("index"))




@app.post("/reset_states")
def reset_states():
    cfg = load_cfg()
    cfg["item_states"] = {}
    save_cfg(cfg)
    flash("Histórico de envio (item_states) zerado. Próximo ciclo enviará como 'novos'.", "warning")
    return redirect(url_for("config_page"))

@app.post("/save")
def save_config():
    cfg = load_cfg()
    f = request.form

    # --------- gerais ---------
    cfg["frequency_minutes"] = max(1, int(f.get("frequency_minutes","60") or "60"))
    cfg["message_text"]      = f.get("message_text","")
    cfg["use_24h"]           = (f.get("use_24h") == "on")
    cfg["start_time"]        = f.get("start_time","08:00")
    cfg["end_time"]          = f.get("end_time","18:00")
    cfg["weekdays"]          = _parse_weekdays(f.get("weekdays",""))
    cfg["numbers"]           = [n.strip() for n in f.get("numbers","").splitlines() if n.strip()]
    cfg["groups"]            = [g.strip() for g in f.get("groups","").splitlines() if g.strip()]
    cfg["run_mode"]          = f.get("run_mode","visible")
    if cfg["run_mode"] not in ("visible","hidden"): cfg["run_mode"] = "visible"
    cfg["attachments_mode"]  = f.get("attachments_mode","files")
    if cfg["attachments_mode"] not in ("files","folder","both"): cfg["attachments_mode"] = "files"
    cfg["attachments_folder"]= f.get("attachments_folder","").strip()
    cfg["close_after_send"]  = (f.get("close_after_send","1") == "1")

    # Pasta geral: origem/autosync (mantidos para UI; reimportação ocorre sempre no início do ciclo)
    cfg["general_folder_origin"]   = f.get("general_folder_origin","").strip()
    cfg["general_folder_autosync"] = (f.get("general_folder_autosync") == "1")

    # --------- legendas gerais (nome|mensagem) + DEFAULT/* ---------
    raw_fc = f.get("file_captions_lines","")
    prev_caps = cfg.get("file_captions") or {}
    caps: Dict[str,str] = {}
    any_line = False
    for ln in raw_fc.splitlines():
        s = (ln or "").strip()
        if not s:
            continue
        any_line = True
        if "|" in s:
            k, v = s.split("|", 1)
        elif ";" in s:
            k, v = s.split(";", 1)
        elif " - " in s:
            k, v = s.split(" - ", 1)
        else:
            # linha sem separador => legenda padrão
            caps["__DEFAULT__"] = s
            continue
        k, v = (k or "").strip(), (v or "").strip()
        if k and v:
            # aceita '*' como curinga, nome completo ou nome sem extensão
            caps[k] = v
    if raw_fc.strip() == "":
        cfg["file_captions"] = prev_caps
    else:
        cfg["file_captions"] = caps if (caps or any_line) else prev_caps

    # --------- itens existentes ---------
    new_custom = []
    idx = 0
    current_items = load_cfg().get("custom_items") or []
    while True:
        key_text = f"ctext_existing_{idx}"
        if key_text not in f:
            break

        iid   = f.get(f"cid_existing_{idx}") or uuid.uuid4().hex
        itype = (f.get(f"ctype_existing_{idx}") or "").strip().lower()
        text  = (f.get(key_text) or "").strip()
        ival  = (f.get(f"cinterval_existing_{idx}") or "").strip()
        interval = int(ival) if ival.isdigit() and int(ival) > 0 else None

        folder_path = (f.get(f"cpath_existing_{idx}") or "").strip()
        origin      = (f.get(f"corigin_existing_{idx}") or "").strip()
        autosync    = (f.get(f"cautosync_existing_{idx}") == "1")

        if itype not in ("file","folder"):
            itype = "folder" if folder_path else "file"

        if itype == "folder":
            path = folder_path
        else:
            path = ""
            cur = current_items[idx]["path"] if idx < len(current_items) else ""
            up = request.files.get(f"cfile_existing_{idx}")
            if up and up.filename:
                savep = os.path.join(UPLOAD_DIR, up.filename); up.save(savep)
                path = savep
            else:
                path = cur

        if path:
            new_custom.append({
                "id": iid,
                "type": itype,
                "path": path,
                "text": text,
                "interval": interval,
                "origin": origin if itype == "folder" else "",
                "autosync": bool(autosync) if itype == "folder" else False,
            })
        idx += 1

    # --------- itens novos ---------
    try:
        nnew = int(f.get("items_new_count","0") or "0")
    except Exception:
        nnew = 0

    for i in range(nnew):
        iid   = uuid.uuid4().hex
        itype = (f.get(f"ctype_new_{i}") or "").strip().lower()
        text  = (f.get(f"ctext_new_{i}") or "").strip()
        ival  = (f.get(f"cinterval_new_{i}") or "").strip()
        interval = int(ival) if ival.isdigit() and int(ival) > 0 else None

        folder_path = (f.get(f"cpath_new_{i}") or "").strip()
        origin      = (f.get(f"corigin_new_{i}") or "").strip()
        autosync    = (f.get(f"cautosync_new_{i}") == "1")

        if itype not in ("file","folder"):
            itype = "folder" if folder_path else "file"

        if itype == "folder":
            path = folder_path
        else:
            up = request.files.get(f"cfile_new_{i}")
            path = ""
            if up and up.filename:
                savep = os.path.join(UPLOAD_DIR, up.filename); up.save(savep)
                path = savep

        if path:
            new_custom.append({
                "id": iid,
                "type": itype,
                "path": path,
                "text": text,
                "interval": interval,
                "origin": origin if itype == "folder" else "",
                "autosync": bool(autosync) if itype == "folder" else False,
            })

    # --------- uploads gerais enviados no formulário ---------
    for fl in request.files.getlist("files"):
        if fl and fl.filename:
            savep = os.path.join(UPLOAD_DIR, fl.filename); fl.save(savep)
            if savep not in cfg["attachments"]:
                cfg["attachments"].append(savep)

    cfg["custom_items"] = new_custom
    save_cfg(cfg)
    _schedule(cfg)
    flash("Configuração salva.", "success")
    return redirect(url_for("config_page"))

@app.route("/clear_attachments")
def clear_attachments():
    cfg = load_cfg()
    cfg["attachments"] = []; save_cfg(cfg)
    flash("Uploads gerais limpos (arquivos permanecem em /uploads_msg).", "warning")
    return redirect(url_for("config_page"))

@app.route("/clear_items")
def clear_items():
    cfg = load_cfg()
    cfg["custom_items"] = []; save_cfg(cfg)
    flash("Itens personalizados limpos.", "warning")
    return redirect(url_for("config_page"))

# =================== Start ===================
def _startup():
    cfg = load_cfg(); _schedule(cfg); log("Servidor iniciado.")

# =================== UI/TEMPLATE ===================
TPL = r"""<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8"><title>{{ title }}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  body{ background:#fff; color:#0b3d2e; }
  .navbar{ background:#0b3d2e; }
  .card{ border-color:#e5ece7; box-shadow: 0 2px 6px rgba(0,0,0,.06); }
  .card-header{ background:#0b3d2e; color:#fff; text-align:center; font-weight:700; }
  .btn-leo{ background:#1e8b4d; border-color:#1e8b4d; color:#fff; }
  .btn-leo:hover{ background:#1a7b44; border-color:#1a7b44; }
  .small-muted{ color:#5c7a6b; font-size:.9rem; }
  .monos{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Courier New", monospace; }
  textarea{ font-family: inherit; }
  .help{ font-size:.85rem; color:#6b8a7a; }
</style></head>
<body>
<nav class="navbar navbar-dark navbar-expand">
  <div class="container-fluid">
    <span class="navbar-brand fw-bold">Mensageiro Automático</span>
    <div class="ms-auto d-flex gap-2">
      <a class="btn btn-sm btn-outline-light" href="{{ url_for('index') }}">Painel</a>
      <a class="btn btn-sm btn-outline-light" href="{{ url_for('config_page') }}">Configuração</a>
      <a class="btn btn-sm btn-outline-light" href="{{ url_for('logs_page') }}">Logs</a>
    </div>
  </div>
</nav>

<div class="container my-4">
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      {% for cat,msg in messages %}
        <div class="alert alert-{{cat}}">{{ msg }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  {% if page=='index' %}
    <div class="card">
      <div class="card-header">Status</div>
      <div class="card-body">
        <div class="d-flex flex-wrap align-items-center gap-3">
          {% if cfg.enabled %}<span class="badge bg-success px-3 py-2">LIGADO</span>
          {% else %}<span class="badge bg-secondary px-3 py-2">DESLIGADO</span>{% endif %}
          <div class="small-muted">
            Frequência geral: <b>{{ cfg.frequency_minutes }} min</b>
            {% if cfg.last_run %} · Última execução: <b>{{ cfg.last_run }}</b>{% endif %}
            {% if cfg.next_global_due %} · Próxima execução geral: <b>{{ cfg.next_global_due }}</b>{% endif %}
          </div>
        </div>

        <div class="d-flex gap-2 my-3">
          <form method="post" action="{{ url_for('toggle', action='on') }}"><button class="btn btn-leo btn-sm" {{ 'disabled' if cfg.enabled else '' }}>Ligar</button></form>
          <form method="post" action="{{ url_for('toggle', action='off') }}"><button class="btn btn-outline-danger btn-sm" {{ '' if cfg.enabled else 'disabled' }}>Desligar</button></form>
          <form method="post" action="{{ url_for('run_now') }}"><button class="btn btn-outline-primary btn-sm">Executar agora</button></form>
        </div>

        <hr>
        <div class="small-muted">Janela:</div>
        {% if cfg.use_24h %}<div>24 horas (todos os dias)</div>
        {% else %}<div>{{ cfg.start_time }} → {{ cfg.end_time }} · Dias: {{ cfg.weekdays }}</div>{% endif %}

        <div class="small-muted mt-2">Modo:</div><div><b>{{ 'Visível' if cfg.run_mode=='visible' else 'Oculto (headless)' }}</b></div>
        <div class="small-muted mt-2">Fechar ao final:</div><div><b>{{ 'Sim' if cfg.close_after_send else 'Não' }}</b></div>

        <div class="small-muted mt-2">Destinos:</div>
        <div class="monos">Números: {{ cfg.numbers }}</div>
        <div class="monos">Grupos: {{ cfg.groups }}</div>

        <div class="small-muted mt-3">Itens personalizados (com pré-agenda individual quando “Intervalo (min)” &gt; 0):</div>
        <div>
          {% if cfg.custom_items %}
            <ul class="small-muted">
              {% for it in cfg.custom_items %}
                {% set st = (cfg.item_states.get(it.id) if cfg.item_states else None) %}
                <li>
                  <b>[{{ it.type|upper }}]</b> {{ it.path }}
                  {% if it.interval %} · <b>{{ it.interval }} min</b>{% else %} · <i>freq. geral</i>{% endif %}
                  {% if st and st.next_due %} · próxima: {{ st.next_due }}{% endif %}
                  {% if st and st.last_sent %} · último envio: {{ st.last_sent }}{% endif %}
                  {% if it.text %} · “{{ it.text[:40] }}{% if it.text|length>40 %}…{% endif %}”{% endif %}
                  {% if it.origin %} · <i>origem:</i> {{ it.origin }}{% endif %}
                </li>
              {% endfor %}
            </ul>
          {% else %}—{% endif %}
        </div>

        <div class="small-muted mt-2">Anexos gerais (freq. geral):</div>
        <div class="monos">{{ preview_attachments }}</div>
      </div>
    </div>

  {% elif page=='config' %}
    <div class="card"><div class="card-header">Configuração</div>
      <div class="card-body">
        <form method="post" action="{{ url_for('save_config') }}" enctype="multipart/form-data" id="cfgform">
          <div class="row g-3">
            <div class="col-md-3"><label class="form-label">Frequência GERAL (min)</label>
              <input class="form-control" type="number" name="frequency_minutes" min="1" value="{{ cfg.frequency_minutes }}"></div>
            <div class="col-md-3"><label class="form-label">Modo de execução</label>
              <select class="form-select" name="run_mode">
                <option value="visible" {{ 'selected' if cfg.run_mode=='visible' else '' }}>Visível</option>
                <option value="hidden"  {{ 'selected' if cfg.run_mode=='hidden' else '' }}>Oculto (headless)</option>
              </select></div>
            <div class="col-md-3"><label class="form-label">Fechar navegador ao final</label>
              <select class="form-select" name="close_after_send">
                <option value="1" {{ 'selected' if cfg.close_after_send else '' }}>Sim</option>
                <option value="0" {{ '' if cfg.close_after_send else 'selected' }}>Não</option>
              </select></div>

            <div class="col-md-12"><label class="form-label">Mensagem avulsa (opcional)</label>
              <textarea class="form-control" name="message_text" rows="2">{{ cfg.message_text }}</textarea></div>

            <div class="col-12"><hr></div>

            <div class="col-12"><div class="form-check form-switch">
              <input class="form-check-input" type="checkbox" id="sw24" name="use_24h" {{ 'checked' if cfg.use_24h else '' }}>
              <label for="sw24" class="form-check-label">24 horas</label></div></div>
            <div class="col-md-3"><label class="form-label">Início (HH:MM)</label><input class="form-control" name="start_time" value="{{ cfg.start_time }}"></div>
            <div class="col-md-3"><label class="form-label">Fim (HH:MM)</label><input class="form-control" name="end_time" value="{{ cfg.end_time }}"></div>
            <div class="col-md-6"><label class="form-label">Dias (1=Seg … 7=Dom)</label>
              <input class="form-control" name="weekdays" value="{{ cfg.weekdays }}">
              <div class="form-text">Ex.: <code>1,2,3,4,5</code> ou <code>[1,2,3,4,5]</code></div></div>

            <div class="col-12"><hr></div>

            <div class="col-md-6"><label class="form-label">Números (um por linha, com DDI)</label>
              <textarea class="form-control monos" name="numbers" rows="4">{{ '\n'.join(cfg.numbers or []) }}</textarea></div>
            <div class="col-md-6"><label class="form-label">Grupos (um por linha, nome exato)</label>
              <textarea class="form-control monos" name="groups" rows="4">{{ '\n'.join(cfg.groups or []) }}</textarea></div>

            <div class="col-12"><hr></div>

            <div class="col-md-12"><label class="form-label">Legendas para anexos gerais (nome|mensagem, um por linha)</label>
              <textarea class="form-control monos" name="file_captions_lines" rows="5">{{ file_captions_lines }}</textarea>
              <div class="form-text">
                Ex.: <code>dashboard_caixas (18).pdf|Indicador 4</code> ·
                <b>OU</b> uma linha sem separador vira <b>legenda padrão</b> para todos (ex.: <code>Caio Cezar</code>) ·
                Aceita também <code>*|Texto</code> como curinga.
              </div></div>

            <div class="col-12"><hr></div>

            <div class="col-md-4"><label class="form-label">Anexos gerais: modo</label>
              <select class="form-select" name="attachments_mode">
                <option value="files"  {{ 'selected' if cfg.attachments_mode=='files' else '' }}>Arquivos (uploads)</option>
                <option value="folder" {{ 'selected' if cfg.attachments_mode=='folder' else '' }}>Pasta</option>
                <option value="both"   {{ 'selected' if cfg.attachments_mode=='both' else '' }}>Ambos</option>
              </select></div>
            <div class="col-md-8"><label class="form-label">Pasta de anexos (para Pasta/Ambos)</label>
              <div class="input-group">
                <input class="form-control monos" id="attachments_folder" name="attachments_folder" value="{{ cfg.attachments_folder }}" placeholder="C:\pasta\dos\anexos">
                <button class="btn btn-outline-secondary" type="button" id="btnPickGeneralFolder">Escolher Pasta</button>
              </div>
              {% if folder_preview %}<div class="small-muted mt-1">Prévia: {{ folder_preview }}</div>{% endif %}
              <div class="mt-2">
                <label class="form-label">Origem (servidor) para autoatualizar a pasta GERAL</label>
                <div class="input-group">
                  <input class="form-control monos" name="general_folder_origin" value="{{ cfg.general_folder_origin or '' }}" placeholder="ex.: C:\Relatorios\Diario ou \\servidor\pasta" id="general_folder_origin">
                  <button class="btn btn-outline-secondary" type="button" id="btnPickGeneralOrigin">Escolher Origem</button>
                </div>
                <div class="form-check mt-1">
                  <input class="form-check-input" type="checkbox" name="general_folder_autosync" value="1" {{ 'checked' if cfg.general_folder_autosync else '' }}>
                  <label class="form-check-label">Reimportar a cada execução (se a pasta acima for snapshot)</label>
                </div>
              </div>
            </div>

            <div class="col-md-12"><label class="form-label">Uploads gerais atuais</label>
              <div class="monos">{{ cfg.attachments }}</div></div>

            <div class="col-12"><hr></div>

            <div class="col-12">
              <label class="form-label d-flex align-items-center justify-content-between">
                Itens “+ Anexo ou Pasta” (1 item = arquivo OU pasta + mensagem + intervalo)
                <button class="btn btn-sm btn-success" type="button" id="btnAdd">+ Item</button>
              </label>
              <div id="itemsZone">
                {% for it in cfg.custom_items %}
                <div class="row g-2 mb-2 item-line">
                  <input type="hidden" name="cid_existing_{{ loop.index0 }}" value="{{ it.id }}">
                  <div class="col-md-2">
                    <label class="form-label">Tipo</label>
                    <select class="form-select ctype">
                      <option value="file" {{ 'selected' if it.type=='file' else '' }}>Arquivo</option>
                      <option value="folder" {{ 'selected' if it.type=='folder' else '' }}>Pasta</option>
                    </select>
                    <input type="hidden" name="ctype_existing_{{ loop.index0 }}" value="{{ it.type }}">
                  </div>
                  <div class="col-md-4 cfile-zone" style="{{ '' if it.type=='file' else 'display:none' }}">
                    <div class="form-text">Atual: {{ it.path }}</div>
                    <input class="form-control" type="file" name="cfile_existing_{{ loop.index0 }}">
                  </div>
                  <div class="col-md-4 cfolder-zone" style="{{ '' if it.type=='folder' else 'display:none' }}">
                    <label class="form-label">Pasta (snapshot OU real)</label>
                    <div class="input-group mb-1">
                      <input class="form-control monos folder-target" name="cpath_existing_{{ loop.index0 }}" value="{{ it.path }}" placeholder="Clique em 'Escolher Pasta' ou digite a pasta real">
                      <button class="btn btn-outline-secondary btnPickFolder" type="button">Escolher Pasta</button>
                    </div>
                    <div class="form-text small muted preview-done"></div>
                    <div class="mt-2">
                      <label class="form-label">Origem (servidor) para autoatualizar</label>
                      <div class="input-group mb-1">
                        <input class="form-control monos origin-target" name="corigin_existing_{{ loop.index0 }}" value="{{ it.origin or '' }}" placeholder="Selecione a pasta de origem">
                        <button class="btn btn-outline-secondary btnPickOrigin" type="button">Escolher Origem</button>
                      </div>
                      <div class="form-check mt-1">
                        <input class="form-check-input" type="checkbox" name="cautosync_existing_{{ loop.index0 }}" value="1" {{ 'checked' if it.autosync else '' }}>
                        <label class="form-check-label">Reimportar a cada execução (se acima for snapshot)</label>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-3">
                    <label class="form-label">Mensagem</label>
                    <input class="form-control" name="ctext_existing_{{ loop.index0 }}" value="{{ it.text or '' }}" placeholder="Mensagem do item">
                  </div>
                  <div class="col-md-2">
                    <label class="form-label">Intervalo (min)</label>
                    <input class="form-control" type="number" min="1" name="cinterval_existing_{{ loop.index0 }}" value="{{ it.interval or '' }}" placeholder="ex.: 10">
                  </div>
                  <div class="col-md-1 d-grid align-items-end">
                    <button class="btn btn-outline-danger btn-remove" type="button">X</button>
                  </div>
                </div>
                {% endfor %}
              </div>
              <input type="hidden" name="items_new_count" id="items_new_count" value="0">
              <div class="help mt-1">
                Ao escolher uma pasta, os arquivos serão copiados para <code>uploads_msg/...</code> e o caminho será preenchido automaticamente.
              </div>
              <div class="help mt-1">
                Para <b>pasta dinâmica</b> (conteúdo muda sempre), <b>digite</b> o caminho real do servidor
                (ex.: <code>C:\Relatorios\Diario</code> ou <code>\\servidor\pasta</code>).
                O botão “Escolher Pasta” faz uma <b>cópia estática</b> para <code>uploads_msg/...</code> (snapshot).
              </div>
            </div>
          </div>

          <div class="d-flex gap-2 mt-3">
            <button class="btn btn-leo">Salvar</button>
            <a class="btn btn-outline-secondary" href="{{ url_for('index') }}">Voltar</a>
            <a class="btn btn-outline-warning ms-auto" href="{{ url_for('clear_items') }}">Limpar itens</a>
            <a class="btn btn-outline-danger" href="{{ url_for('clear_attachments') }}">Limpar uploads gerais</a>
          </div>
        </form>

        <!-- Inputs ocultos para picker de pasta (fallback web) -->
        <input type="file" id="folderPicker" style="display:none" webkitdirectory directory multiple>
        <input type="file" id="generalFolderPicker" style="display:none" webkitdirectory directory multiple>
        <input type="file" id="originFolderPicker" style="display:none" webkitdirectory directory multiple>
      </div>
    </div>

<script>
  function applyType(row, value){
    const fileZone   = row.querySelector('.cfile-zone');
    const folderZone = row.querySelector('.cfolder-zone');
    const hidden     = row.querySelector('input[name^="ctype_"]');
    if (hidden) hidden.value = value;
    if (value === 'folder'){
      if (fileZone)   fileZone.style.display   = 'none';
      if (folderZone) folderZone.style.display = '';
    } else {
      if (fileZone)   fileZone.style.display   = '';
      if (folderZone) folderZone.style.display = 'none';
    }
  }

  function initExistingRows(){
    document.querySelectorAll('#itemsZone .item-line').forEach((row)=>{
      const sel = row.querySelector('select.ctype');
      if (sel) applyType(row, sel.value);
    });
  }
  initExistingRows();

  const itemsZone = document.getElementById('itemsZone');
  itemsZone.addEventListener('change', (ev)=>{
    const t = ev.target;
    if (t && t.matches('select.ctype')){
      const row = t.closest('.item-line');
      if (row) applyType(row, t.value);
    }
  });

  let newCount = 0;
  document.getElementById('btnAdd')?.addEventListener('click', ()=>{
    const row = document.createElement('div');
    row.className = 'row g-2 mb-2 item-line';
    row.innerHTML = `
      <div class="col-md-2">
        <label class="form-label">Tipo</label>
        <select class="form-select ctype">
          <option value="file" selected>Arquivo</option>
          <option value="folder">Pasta</option>
        </select>
        <input type="hidden" name="ctype_new_${newCount}" value="file">
      </div>
      <div class="col-md-4 cfile-zone">
        <label class="form-label">Arquivo</label>
        <input class="form-control" type="file" name="cfile_new_${newCount}">
      </div>
      <div class="col-md-4 cfolder-zone" style="display:none">
        <label class="form-label">Pasta (snapshot OU real)</label>
        <div class="input-group mb-1">
          <input class="form-control monos folder-target" name="cpath_new_${newCount}" placeholder="Clique em 'Escolher Pasta' ou digite a pasta real">
          <button class="btn btn-outline-secondary btnPickFolder" type="button">Escolher Pasta</button>
        </div>
        <div class="form-text small muted preview-done"></div>
        <div class="mt-2">
          <label class="form-label">Origem (servidor) para autoatualizar</label>
          <div class="input-group mb-1">
            <input class="form-control monos origin-target" name="corigin_new_${newCount}" placeholder="ex.: C:\\Relatorios\\Diario ou \\\\servidor\\pasta">
            <button class="btn btn-outline-secondary btnPickOrigin" type="button">Escolher Origem</button>
          </div>
          <div class="form-check mt-1">
            <input class="form-check-input" type="checkbox" name="cautosync_new_${newCount}" value="1">
            <label class="form-check-label">Reimportar a cada execução (se acima for snapshot)</label>
          </div>
        </div>
      </div>
      <div class="col-md-3">
        <label class="form-label">Mensagem</label>
        <input class="form-control" name="ctext_new_${newCount}" placeholder="Mensagem do item">
      </div>
      <div class="col-md-2">
        <label class="form-label">Intervalo (min)</label>
        <input class="form-control" type="number" min="1" name="cinterval_new_${newCount}" placeholder="ex.: 10">
      </div>
      <div class="col-md-1 d-grid align-items-end">
        <button class="btn btn-outline-danger btn-remove" type="button">X</button>
      </div>`;
    itemsZone.appendChild(row);
    document.getElementById('items_new_count').value = String(++newCount);
    applyType(row, 'file');
    wireFolderButtons(row);
    wireOriginButtons(row);
  });

  document.addEventListener('click', (e)=>{
    if (e.target.classList.contains('btn-remove')){
      e.target.closest('.item-line')?.remove();
    }
  });

  let currentFolderTarget = null;

  function wireFolderButtons(scope){
    scope.querySelectorAll('.btnPickFolder').forEach(btn=>{
      if (btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', ()=>{
        const row = btn.closest('.item-line');
        const sel = row.querySelector('select.ctype');
        if (sel){ sel.value = 'folder'; applyType(row, 'folder'); }
        currentFolderTarget = {
          input: row.querySelector('.folder-target'),
          preview: row.querySelector('.preview-done')
        };
        document.getElementById('folderPicker').click();
      });
    });
  }
  wireFolderButtons(document);

  async function uploadFolder(files, previewEl, targetInput){
    const fd = new FormData();
    for (let i=0;i<files.length;i++){
      const f = files[i];
      fd.append('folder_files', f, f.name);
      fd.append('relpaths[]', f.webkitRelativePath || f.name);
    }
    const resp = await fetch('{{ url_for("upload_folder") }}', { method:'POST', body: fd });
    const data = await resp.json();
    if (!data.ok){ alert(data.error || 'Falha ao importar pasta'); return; }
    targetInput.value = data.saved_path;
    if (previewEl) previewEl.textContent = `Importado: ${data.count} arquivo(s) → ${data.saved_path}`;
  }

  document.getElementById('folderPicker').addEventListener('change', async (ev)=>{
    const files = ev.target.files;
    if (!files || !files.length || !currentFolderTarget) return;
    try{
      await uploadFolder(files, currentFolderTarget.preview, currentFolderTarget.input);
    } finally {
      ev.target.value = '';
      currentFolderTarget = null;
    }
  });

  document.getElementById('btnPickGeneralFolder').addEventListener('click', ()=>{
    document.getElementById('generalFolderPicker').click();
  });
  document.getElementById('generalFolderPicker').addEventListener('change', async (ev)=>{
    const files = ev.target.files;
    if (!files || !files.length) return;
    const input = document.getElementById('attachments_folder');
    try{
      const fd = new FormData();
      for (let i=0;i<files.length;i++){
        const f = files[i];
        fd.append('folder_files', f, f.name);
        fd.append('relpaths[]', f.webkitRelativePath || f.name);
      }
      const resp = await fetch('{{ url_for("upload_folder") }}', { method:'POST', body: fd });
      const data = await resp.json();
      if (!data.ok){ alert(data.error || 'Falha ao importar pasta'); return; }
      input.value = data.saved_path;
      alert(`Pasta geral importada (${data.count} arquivos).`);
    } finally {
      ev.target.value = '';
    }
  });

  async function pickNativeFolder(){
    try{
      const r  = await fetch('{{ url_for("pick_folder_native") }}');
      const js = await r.json();
      if (js && js.ok && js.path) return js.path;
      return null;
    }catch(e){ return null; }
  }

  let currentOriginTarget = null;
  function wireOriginButtons(scope){
    scope.querySelectorAll('.btnPickOrigin').forEach(btn=>{
      if (btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', async ()=>{
        const row = btn.closest('.item-line');
        currentOriginTarget = row.querySelector('.origin-target');
        const npath = await pickNativeFolder();
        if (npath){
          currentOriginTarget.value = npath;
          currentOriginTarget = null;
          return;
        }
        document.getElementById('originFolderPicker').click();
      });
    });
  }
  wireOriginButtons(document);

  document.getElementById('originFolderPicker').addEventListener('change', (ev)=>{
    const files = ev.target.files;
    if (!files || !files.length || !currentOriginTarget) return;
    try {
      const rel = (files[0].webkitRelativePath || files[0].name || '');
      const root = rel.split('/')[0] || rel;
      currentOriginTarget.value = root;
    } finally {
      ev.target.value = '';
      currentOriginTarget = null;
    }
  });

  document.getElementById('btnPickGeneralOrigin').addEventListener('click', async ()=>{
    const input = document.getElementById('general_folder_origin');
    const npath = await pickNativeFolder();
    if (npath){ input.value = npath; return; }
    document.getElementById('originFolderPicker').onchange = (ev)=>{
      const files = ev.target.files;
      if (!files || !files.length) return;
      const rel = (files[0].webkitRelativePath || files[0].name || '');
      const root = rel.split('/')[0] || rel;
      input.value = root;
      ev.target.value = '';
      document.getElementById('originFolderPicker').onchange = null;
    };
    document.getElementById('originFolderPicker').click();
  });

  const mo = new MutationObserver((muts)=>{
    muts.forEach(m=>{
      m.addedNodes.forEach(n=>{
        if (n.nodeType===1 && n.classList.contains('item-line')){
          wireFolderButtons(n);
          wireOriginButtons(n);
          const sel = n.querySelector('select.ctype');
          if (sel) applyType(n, sel.value);
        }
      });
    });
  });
  mo.observe(itemsZone, {childList:true});
</script>

  {% elif page=='logs' %}
    <div class="card">
      <div class="card-header">Logs</div>
      <div class="card-body">
        <pre class="monos" style="max-height:70vh; overflow:auto; white-space:pre-wrap;">{{ logs }}</pre>
        <div class="mt-2">
          <a class="btn btn-outline-secondary" href="{{ url_for('index') }}">Voltar</a>
          <a class="btn btn-outline-primary ms-2" href="{{ url_for('download_logs') }}">Baixar .log</a>
          <a class="btn btn-outline-danger ms-2" href="{{ url_for('clear_logs') }}">Limpar</a>
        </div>
      </div>
    </div>
  {% endif %}
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body></html>
"""

if __name__ == "__main__":
    _startup()
    host, port = "0.0.0.0", PORT
    print("\n=== Mensageiro Automático ===")
    print(f"Acesse: http://127.0.0.1:{port}")
    try:
        ip = socket.gethostbyname(socket.gethostname())
        print(f"Na rede: http://{ip}:{port}")
    except Exception:
        pass
    print("================================\n")
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
 