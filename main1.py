#!/usr/bin/env python3
# tdata_checker.py
"""
Высокопроизводительный скрипт для проверки сессий Telegram (tdata).

Основные возможности:
- Автоматическое определение типов входных данных: папки tdata, файлы .session Telethon, CSV со StringSession.
- Изолированная и отказоустойчивая конвертация tdata в сессии Telethon для каждого аккаунта с таймаутом.
- Асинхронная проверка с использованием Telethon и пула воркеров (до 50).
- Ротация API-ключей и прокси (round-robin, random, sticky).
- Управление лимитами запросов и экспоненциальная задержка (backoff) при ошибках.
- Интерактивный прогресс-бар (tqdm) с оценкой времени выполнения (ETA/CPM).
- Детальный экспорт результатов в CSV с разделением на валидные и невалидные сессии.
- Гибкие настройки через аргументы командной строки: dry-run, keep-temp, таймауты, параллелизм и т.д.

Используйте ответственно: проверяйте только те аккаунты, которыми вы владеете или на использование которых у вас есть разрешение.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import shutil
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# --- Проверка и импорт зависимостей ---
try:
    from telethon import TelegramClient
    from telethon.errors import RPCError, FloodWaitError, SessionPasswordNeededError
    from telethon.sessions import StringSession
except ImportError:
    print("[ERROR] Библиотека telethon не найдена. Установите ее: pip install telethon", file=sys.stderr)
    sys.exit(1)

try:
    import socks  # PySocks
except ImportError:
    socks = None
    print("[WARNING] PySocks не найден. Функциональность прокси будет недоступна. Установите: pip install pysocks", file=sys.stderr)


try:
    from tqdm.asyncio import tqdm
except ImportError:
    print("[ERROR] Библиотека tqdm не найдена. Установите ее: pip install tqdm", file=sys.stderr)
    sys.exit(1)

# opentele опционален для конвертации tdata
try:
    from opentele.api import UseCurrentSession
    from opentele.td import TDesktop
    OPENTELE_AVAILABLE = True
except ImportError:
    OPENTELE_AVAILABLE = False


# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Структуры данных (Data classes) ---
@dataclass
class ApiPair:
    """Хранит пару api_id и api_hash."""
    api_id: int
    api_hash: str
    label: str

@dataclass
class ProxySpec:
    """Хранит разобранные данные прокси."""
    raw: str
    scheme: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None

@dataclass
class CheckResult:
    """Хранит результат проверки одной сессии."""
    key: str
    ok: bool
    error: Optional[str]
    user_id: Optional[int]
    username: Optional[str]
    first_name: Optional[str]
    phone: Optional[str]
    checked_at: str
    api_label: Optional[str]
    proxy: Optional[str]
    latency_ms: Optional[int]
    attempts: int
    duration_s: float

# --- Утилиты ---
def load_api_pairs(path: str) -> List[ApiPair]:
    """Загружает API-ключи из JSON файла."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError(f"Не удалось прочитать или декодировать файл API: {e}")

    api_pairs = []
    for i, item in enumerate(data):
        try:
            api_id = int(item["api_id"])
            api_hash = str(item["api_hash"])
            label = str(item.get("label") or f"api_{i}")
            api_pairs.append(ApiPair(api_id, api_hash, label))
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Некорректная запись API #{i}: {item}. Ошибка: {e}")

    if not api_pairs:
        raise ValueError("Файл API не содержит валидных ключей.")
    return api_pairs

def parse_proxy_line(line: str) -> ProxySpec:
    """
    Разбирает строку с прокси, используя urllib, для большей надежности.
    Форматы: scheme://user:pass@host:port или host:port.
    """
    line = line.strip()
    if not line:
        raise ValueError("Пустая строка прокси.")

    if "://" not in line:
        line = f"socks5://{line}"

    parsed = urlparse(line)
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"Не удалось извлечь хост и порт из '{line}'")

    return ProxySpec(
        raw=line,
        scheme=(parsed.scheme or "socks5").lower(),
        host=parsed.hostname,
        port=parsed.port,
        username=parsed.username,
        password=parsed.password,
    )

def load_proxies(path: str) -> List[ProxySpec]:
    """Загружает список прокси из файла."""
    proxies = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    proxies.append(parse_proxy_line(line))
                except ValueError as e:
                    logging.warning("Пропущена некорректная строка прокси '%s': %s", line, e)
    except FileNotFoundError:
        logging.error("Файл с прокси не найден: %s", path)
    return proxies

def get_telethon_proxy(proxy: ProxySpec) -> tuple:
    """Преобразует ProxySpec в кортеж, понятный Telethon."""
    if not socks:
        raise RuntimeError("Для использования прокси необходима библиотека PySocks (pip install pysocks)")

    proxy_map = {
        "socks5": socks.SOCKS5, "socks5h": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP, "https": socks.HTTP,
    }
    proxy_type = proxy_map.get(proxy.scheme, socks.SOCKS5)
    return (proxy_type, proxy.host, proxy.port, True, proxy.username, proxy.password)


def list_session_files(folder: Path) -> List[Path]:
    """Возвращает отсортированный список файлов .session в директории."""
    if not folder.is_dir():
        raise FileNotFoundError(f"Директория с сессиями не найдена: {folder}")
    return sorted(p for p in folder.glob("*.session") if p.is_file())


def load_string_sessions_csv(path: Path) -> List[Tuple[str, str]]:
    """Загружает String Sessions из CSV файла."""
    sessions = []
    if not path.is_file():
        raise FileNotFoundError(f"CSV файл с сессиями не найден: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, 1):
            if not row:
                continue
            if len(row) == 1:
                key, session_str = f"row_{i}", row[0].strip()
            else:
                key, session_str = row[0].strip(), row[1].strip()

            if not key or not session_str:
                logging.warning("Пропущена некорректная строка в CSV файле #%d", i)
                continue
            sessions.append((key, session_str))
    return sessions


# --- Конвертация TDATA ---
def detect_account_dirs(tdata_path: Path) -> List[str]:
    """Определяет директории аккаунтов внутри папки tdata."""
    # Список стандартных папок и файлов, которые не являются аккаунтами
    shared = {
        'countries', 'emoji', 'prefix', 'key_datas', 'settingss', 'user_data',
        'usertag', 'shortcuts-default.json', 'shortcuts-custom.json', 'dumps',
        'temp', 'map', 'logs'
    }
    candidates = []
    # Сначала ищем директории с именами длиннее 6 символов, что характерно для аккаунтов
    for p in sorted(tdata_path.iterdir()):
        if p.is_dir() and p.name not in shared and len(p.name) >= 16:
            candidates.append(p.name)
    
    # Если ничего не найдено, ищем любые директории, не входящие в стандартный список
    if not candidates:
        for p in sorted(tdata_path.iterdir()):
            if p.is_dir() and p.name not in shared:
                candidates.append(p.name)
    return candidates

def make_isolated_tdata_copy(src: Path, account_dirname: str) -> Path:
    """Создает временную изолированную копию tdata только с одним аккаунтом."""
    tmp_root = Path(tempfile.mkdtemp(prefix=f"tdata_iso_{account_dirname}_"))
    dst = tmp_root / "tdata"
    dst.mkdir()

    # Копируем только нужную папку аккаунта и общие файлы/папки
    shutil.copytree(src / account_dirname, dst / account_dirname)
    for entry in src.iterdir():
        if entry.name != account_dirname:
            try:
                if entry.is_dir():
                     if entry.name not in ['dumps', 'temp', 'log', 'logs']:
                        shutil.copytree(entry, dst / entry.name, dirs_exist_ok=True)
                else:
                    shutil.copy2(entry, dst / entry.name)
            except (IOError, OSError) as e:
                logging.warning("Не удалось скопировать '%s': %s", entry.name, e)
    return dst


async def convert_account_async(tdata_copy: Path, out_session: Path):
    """Асинхронно конвертирует один аккаунт tdata, используя opentele."""
    if not OPENTELE_AVAILABLE:
        raise RuntimeError("opentele не установлен.")

    tdesk = TDesktop(tdata_copy)
    if not tdesk.isLoaded():
        raise RuntimeError("opentele: не удалось загрузить аккаунты из копии tdata.")

    client = await tdesk.ToTelethon(session=str(out_session), flag=UseCurrentSession)
    try:
        await client.connect()
    finally:
        if client.is_connected():
            await client.disconnect()


async def _run_one_conversion(
    acc: str, src: Path, outp: Path, timeout: int, keep_temp: bool, sem: asyncio.Semaphore
) -> Dict[str, Any]:
    """Внутренняя функция для выполнения одной задачи конвертации."""
    async with sem:
        t0 = time.monotonic()
        tmp_copy = None
        session_out = outp / f"from_{acc}.session"
        result: Dict[str, Any] = {"account": acc, "status": "error"}

        try:
            tmp_copy = await asyncio.to_thread(make_isolated_tdata_copy, src, acc)
            coro = convert_account_async(tmp_copy, session_out)
            await asyncio.wait_for(coro, timeout=timeout)
            
            result.update({
                "status": "ok",
                "session": str(session_out),
                "temp": str(tmp_copy)
            })
        except asyncio.TimeoutError:
            result.update({"status": "timeout", "error": f"таймаут после {timeout}с"})
        except Exception as e:
            result.update({"error": str(e)})
            logging.error("Ошибка конвертации аккаунта %s: %s", acc, e, exc_info=False)
        finally:
            result["duration"] = time.monotonic() - t0
            if tmp_copy and not keep_temp:
                try:
                    await asyncio.to_thread(shutil.rmtree, tmp_copy.parent)
                except Exception as e:
                    logging.warning("Не удалось удалить временную папку %s: %s", tmp_copy.parent, e)
        return result

async def convert_tdata_per_account(
    src_tdata: Path, out_dir: Path, timeout: int, keep_temp: bool, parallel: int
) -> List[Dict[str, Any]]:
    """Асинхронно конвертирует каждый аккаунт из папки tdata в сессию Telethon."""
    if not src_tdata.is_dir():
        raise FileNotFoundError(f"Директория tdata не найдена: {src_tdata}")

    out_dir.mkdir(parents=True, exist_ok=True)
    accounts = detect_account_dirs(src_tdata)
    if not accounts:
        logging.warning("В папке %s не найдено директорий аккаунтов.", src_tdata)
        return []
    
    logging.info("Найдено %d аккаунтов для конвертации в %s.", len(accounts), src_tdata)

    sem = asyncio.Semaphore(parallel)
    tasks = [
        _run_one_conversion(acc, src_tdata, out_dir, timeout, keep_temp, sem)
        for acc in accounts
    ]
    
    results = []
    with tqdm(total=len(tasks), desc="Конвертация TDATA", unit="акк") as pbar:
        for fut in asyncio.as_completed(tasks):
            res = await fut
            results.append(res)
            pbar.update(1)
            if res['status'] != 'ok':
                pbar.set_postfix_str(f"Ошибка: {res['account']} - {res.get('error', '')[:50]}")

    return results


# --- Ядро проверки ---
class ApiRotator:
    """Циклически перебирает предоставленные API-ключи."""
    def __init__(self, apis: List[ApiPair]):
        self.apis = apis
        self.n = len(apis)
        if self.n == 0:
            raise ValueError("Список API-ключей не может быть пустым.")

    def pick(self, index: int) -> ApiPair:
        """Выбирает API-ключ по индексу."""
        return self.apis[index % self.n]

class ProxyRotator:
    """Выбирает прокси согласно заданной стратегии."""
    def __init__(self, proxies: List[ProxySpec], strategy: str):
        self.proxies = proxies
        self.strategy = strategy
        self.sticky_map: Dict[str, ProxySpec] = {}
        self.counter = 0

    def choose(self, key: str) -> Optional[ProxySpec]:
        """Выбирает прокси для ключа `key`."""
        if not self.proxies:
            return None
        if self.strategy == "random":
            return random.choice(self.proxies)
        if self.strategy == "sticky":
            if key not in self.sticky_map:
                self.sticky_map[key] = random.choice(self.proxies)
            return self.sticky_map[key]
        
        # 'round' (по умолчанию)
        proxy = self.proxies[self.counter % len(self.proxies)]
        self.counter += 1
        return proxy


@asynccontextmanager
async def telegram_client_manager(
    session: str | StringSession, api_id: int, api_hash: str, proxy: Optional[tuple], timeout: int
) -> AsyncGenerator[TelegramClient, None]:
    """Асинхронный контекстный менеджер для безопасного управления клиентом Telethon."""
    client = TelegramClient(session, api_id, api_hash, proxy=proxy, timeout=timeout)
    try:
        await client.connect()
        yield client
    finally:
        if client.is_connected():
            await client.disconnect()

async def check_session_async(
    key: str,
    session_value: Optional[str],
    session_file: Optional[Path],
    api_pair: ApiPair,
    proxy_spec: Optional[ProxySpec],
    rate_delay: float,
    connect_timeout: int,
    max_attempts: int,
    backoff_base: float,
) -> CheckResult:
    """Проверяет одну сессию с логикой повторов и обработкой ошибок."""
    start_time = time.monotonic()
    last_error: Optional[str] = "no_session"
    
    session_arg: Optional[str | StringSession] = None
    if session_value:
        session_arg = StringSession(session_value)
    elif session_file:
        session_arg = str(session_file)
    
    if not session_arg:
        return CheckResult(key, False, last_error, None, None, None, None, datetime.utcnow().isoformat(), api_pair.label, proxy_spec.raw if proxy_spec else None, None, 0, 0.0)

    proxy_tuple = None
    if proxy_spec:
        try:
            proxy_tuple = get_telethon_proxy(proxy_spec)
        except RuntimeError as e:
            return CheckResult(key, False, f"proxy_error: {e}", None, None, None, None, datetime.utcnow().isoformat(), api_pair.label, proxy_spec.raw, None, 0, 0.0)

    for attempt in range(1, max_attempts + 1):
        t0 = time.monotonic()
        try:
            async with telegram_client_manager(session_arg, api_pair.api_id, api_pair.api_hash, proxy_tuple, connect_timeout) as client:
                if rate_delay > 0:
                    await asyncio.sleep(rate_delay)
                
                if not await client.is_user_authorized():
                    latency = int((time.monotonic() - t0) * 1000)
                    return CheckResult(key, False, "unauthorized", None, None, None, None, datetime.utcnow().isoformat(), api_pair.label, proxy_spec.raw if proxy_spec else None, latency, attempt, time.monotonic() - start_time)

                me = await client.get_me()
                latency = int((time.monotonic() - t0) * 1000)
                
                return CheckResult(
                    key=key, ok=True, error=None,
                    user_id=getattr(me, 'id', None),
                    username=getattr(me, 'username', None),
                    first_name=(getattr(me, 'first_name', None) or getattr(me, 'last_name', None)),
                    phone=getattr(me, 'phone', None),
                    checked_at=datetime.utcnow().isoformat(),
                    api_label=api_pair.label,
                    proxy=proxy_spec.raw if proxy_spec else None,
                    latency_ms=latency, attempts=attempt,
                    duration_s=time.monotonic() - start_time,
                )
        except FloodWaitError as e:
            last_error = f"FloodWait:{e.seconds}s"
            wait_time = min(60, backoff_base**attempt + e.seconds)
            logging.warning("FloodWait для '%s'. Ожидание %.1f сек.", key, wait_time)
            await asyncio.sleep(wait_time)
        except (SessionPasswordNeededError,):
            last_error = "2FA_enabled"
            break  # Повторные попытки бессмысленны
        except RPCError as e:
            last_error = f"RPCError:{type(e).__name__}"
            await asyncio.sleep(min(15, backoff_base**attempt))
        except (asyncio.TimeoutError, OSError):
            last_error = "timeout_or_os_error"
            await asyncio.sleep(min(15, backoff_base**attempt))
        except Exception as e:
            last_error = f"UnknownError:{type(e).__name__}"
            logging.error("Неизвестная ошибка при проверке '%s': %s", key, e, exc_info=True)
            await asyncio.sleep(min(10, backoff_base**attempt))

    return CheckResult(key, False, last_error, None, None, None, None, datetime.utcnow().isoformat(), api_pair.label, proxy_spec.raw if proxy_spec else None, None, max_attempts, time.monotonic() - start_time)


# --- Управление процессом и отчетность ---
async def prepare_tasks(args: argparse.Namespace) -> Tuple[List[Tuple[str, Optional[str], Optional[Path]]], List[Dict[str, Any]]]:
    """Собирает все задачи из разных источников в единый список."""
    tasks: List[Tuple[str, Optional[str], Optional[Path]]] = []
    conv_results: List[Dict[str, Any]] = []

    if args.sessions:
        try:
            for f in list_session_files(Path(args.sessions)):
                tasks.append((f.stem, None, f))
        except FileNotFoundError as e:
            logging.warning(e)

    if args.strings:
        try:
            for k, s in load_string_sessions_csv(Path(args.strings)):
                tasks.append((k, s, None))
        except FileNotFoundError as e:
            logging.warning(e)

    if args.tdata:
        try:
            conv_results = await convert_tdata_per_account(
                Path(args.tdata),
                Path(args.convert_out),
                args.convert_timeout,
                args.keep_temp,
                max(1, min(8, args.convert_parallel))
            )
            for item in conv_results:
                if item.get("status") == "ok":
                    session_path = item.get("session")
                    if session_path:
                        key = f"tdata_{item.get('account')}"
                        tasks.append((key, None, Path(session_path)))
        except Exception as e:
            logging.error("Критическая ошибка при конвертации tdata: %s", e, exc_info=True)

    # Удаление дубликатов по ключу (названию сессии)
    seen_keys = set()
    unique_tasks = []
    for task in tasks:
        if task[0] not in seen_keys:
            unique_tasks.append(task)
            seen_keys.add(task[0])
    
    if len(tasks) != len(unique_tasks):
        logging.info("Удалено %d дублирующихся задач.", len(tasks) - len(unique_tasks))

    return unique_tasks, conv_results


def generate_reports(results: List[CheckResult], tasks: List[Tuple[str, Optional[str], Optional[Path]]], out_path: Path):
    """Генерирует CSV отчеты и перемещает файлы сессий."""
    fieldnames = list(CheckResult.__annotations__.keys())
    valid_path = out_path.with_name("valid.csv")
    invalid_path = out_path.with_name("invalid.csv")
    
    valid_dir = out_path.with_name("valid_sessions")
    invalid_dir = out_path.with_name("invalid_sessions")
    valid_dir.mkdir(exist_ok=True)
    invalid_dir.mkdir(exist_ok=True)

    results_map = {r.key: r for r in results}
    
    try:
        with out_path.open("w", newline="", encoding="utf-8") as main_f, \
             valid_path.open("w", newline="", encoding="utf-8") as valid_f, \
             invalid_path.open("w", newline="", encoding="utf-8") as invalid_f:
            
            main_w = csv.DictWriter(main_f, fieldnames=fieldnames)
            valid_w = csv.DictWriter(valid_f, fieldnames=fieldnames)
            invalid_w = csv.DictWriter(invalid_f, fieldnames=fieldnames)
            main_w.writeheader()
            valid_w.writeheader()
            invalid_w.writeheader()

            for r in results:
                row = asdict(r)
                main_w.writerow(row)
                if r.ok:
                    valid_w.writerow(row)
                else:
                    invalid_w.writerow(row)
        
        logging.info("Отчеты сохранены: %s, %s, %s", out_path.name, valid_path.name, invalid_path.name)
        
        # Перемещение файлов
        for key, _, session_file in tasks:
            if session_file and session_file.exists():
                res = results_map.get(key)
                if res:
                    dest_dir = valid_dir if res.ok else invalid_dir
                    try:
                        shutil.copy2(session_file, dest_dir / session_file.name)
                    except (IOError, OSError) as e:
                        logging.warning("Не удалось скопировать файл %s: %s", session_file.name, e)
        logging.info("Файлы сессий отсортированы по папкам: %s и %s", valid_dir.name, invalid_dir.name)

    except IOError as e:
        logging.error("Ошибка при записи отчета: %s", e)


async def run_checker(args: argparse.Namespace, tasks: List[Tuple[str, Optional[str], Optional[Path]]]):
    """Основная асинхронная функция для запуска проверки."""
    total = len(tasks)
    apis = load_api_pairs(args.apis)
    proxies = load_proxies(args.proxies) if args.proxies else []
    
    api_rot = ApiRotator(apis)
    proxy_rot = ProxyRotator(proxies, args.proxy_strategy)
    
    semaphore = asyncio.Semaphore(args.concurrency)

    async def worker(idx: int, task: Tuple[str, Optional[str], Optional[Path]]) -> CheckResult:
        key, s_val, s_file = task
        async with semaphore:
            api = api_rot.pick(idx)
            proxy = proxy_rot.choose(key)
            
            if args.dry_run:
                await asyncio.sleep(0.01)
                return CheckResult(key, False, "dry_run", None, None, None, None, datetime.utcnow().isoformat(), api.label, proxy.raw if proxy else None, None, 0, 0.0)
            
            return await check_session_async(
                key, s_val, s_file, api, proxy,
                args.rate_delay,
                args.connect_timeout,
                args.max_attempts,
                args.backoff_base,
            )

    results: List[CheckResult] = []
    
    coros = [worker(i, t) for i, t in enumerate(tasks)]
    
    for fut in tqdm.as_completed(coros, total=total, desc="Проверка сессий", unit="акк"):
        try:
            result = await fut
            results.append(result)
        except Exception as e:
            logging.error("Критическая ошибка в воркере: %s", e, exc_info=True)
            
    ok_count = sum(1 for r in results if r.ok)
    logging.info("Проверка завершена. Валидных: %d, невалидных: %d", ok_count, len(results) - ok_count)

    generate_reports(results, tasks, Path(args.out))


# --- CLI и точка входа ---
def create_arg_parser() -> argparse.ArgumentParser:
    """Создает и настраивает парсер аргументов командной строки."""
    p = argparse.ArgumentParser(
        description="Высокопроизводительный скрипт для проверки сессий Telegram (tdata).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    # Источники сессий
    group = p.add_argument_group("Источники сессий (укажите хотя бы один)")
    group.add_argument("--sessions", help="Папка с файлами .session Telethon.")
    group.add_argument("--strings", help="CSV файл со StringSession (формат: id,session).")
    group.add_argument("--tdata", help="Папка tdata для конвертации (требует opentele).")

    # Настройки конвертации tdata
    conv_group = p.add_argument_group("Настройки конвертации tdata")
    conv_group.add_argument("--convert-out", default="./converted_sessions", help="Папка для сконвертированных сессий.")
    conv_group.add_argument("--convert-timeout", type=int, default=60, help="Таймаут конвертации одного аккаунта (сек).")
    conv_group.add_argument("--convert-parallel", type=int, default=2, help="Количество параллельных процессов конвертации.")
    conv_group.add_argument("--keep-temp", action="store_true", help="Не удалять временные копии tdata для отладки.")

    # Настройки сети и API
    net_group = p.add_argument_group("Настройки сети и API")
    net_group.add_argument("--apis", required=True, help='JSON файл со списком API-ключей.\nФормат: [{"api_id": 123, "api_hash": "abc", "label": "my-api"}]')
    net_group.add_argument("--proxies", help="Файл со списком прокси (один на строку).")
    net_group.add_argument("--proxy-strategy", choices=["round", "random", "sticky"], default="round", help="Стратегия выбора прокси.")
    
    # Настройки производительности
    perf_group = p.add_argument_group("Настройки производительности")
    perf_group.add_argument("-c", "--concurrency", type=int, default=20, help="Количество одновременных проверок (1-50).")
    perf_group.add_argument("--max-attempts", type=int, default=3, help="Макс. попыток на одну сессию.")
    perf_group.add_argument("--connect-timeout", type=int, default=15, help="Таймаут подключения (сек).")
    perf_group.add_argument("--backoff-base", type=float, default=1.7, help="База для экспоненциальной задержки при ошибках.")
    perf_group.add_argument("--rate-delay", type=float, default=0.3, help="Задержка перед каждым запросом к API (сек).")

    # Вывод и разное
    out_group = p.add_argument_group("Вывод и разное")
    out_group.add_argument("--out", default="report.csv", help="Путь для основного CSV отчета.")
    out_group.add_argument("--dry-run", action="store_true", help="Симуляция без реальных сетевых запросов.")
    
    return p

async def main():
    """Главная функция: парсинг аргументов и запуск проверки."""
    parser = create_arg_parser()
    args = parser.parse_args()
    
    # Валидация аргументов
    if not any([args.sessions, args.strings, args.tdata]):
        parser.error("Необходимо указать хотя бы один источник сессий (--sessions, --strings или --tdata).")
    
    args.concurrency = max(1, min(50, args.concurrency))

    tasks, conv_results = await prepare_tasks(args)
    if not tasks:
        logging.error("Не найдено ни одной сессии для проверки.")
        return 1

    logging.info("Найдено %d уникальных сессий для проверки.", len(tasks))
    if args.dry_run:
        logging.info("Режим DRY-RUN: сетевые запросы выполняться не будут.")
    
    if tasks:
        await run_checker(args, tasks)
    
    if conv_results:
        try:
            conv_report_path = Path(args.convert_out) / "convert_report.json"
            conv_report_path.parent.mkdir(exist_ok=True, parents=True)
            with conv_report_path.open("w", encoding="utf-8") as f:
                json.dump(conv_results, f, ensure_ascii=False, indent=2)
            logging.info("Отчет о конвертации сохранен: %s", conv_report_path)
        except (IOError, TypeError) as e:
            logging.error("Не удалось сохранить отчет о конвертации: %s", e)

    return 0

if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        logging.info("Процесс прерван пользователем.")
        sys.exit(130)
    except Exception as e:
        logging.critical("Произошла неперехваченная ошибка: %s", e, exc_info=True)
        sys.exit(1)
