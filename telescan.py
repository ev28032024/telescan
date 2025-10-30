#!/usr/bin/env python3
"""telescan.py
================

Утилита для работы с Telegram аккаунтами через Telethon и opentele.

Функциональность:
* Конвертация папок tdata в Telethon `.session` файлы.
* Конвертация `.session` файлов обратно в tdata.
* Асинхронная проверка валидности сессий (включая StringSession).
* Живой прогресс с расчётом ETA/CPM, подробная отчётность в CSV и JSON.
* Поддержка ротации API ключей и прокси (round/random/sticky стратегии).
* Гибкие лимиты параллелизма, backoff, таймауты и rate limiting.

Важно: используйте программу исключительно для аккаунтов, на которые у вас есть права.
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
from enum import Enum
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

try:
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError, RPCError, SessionPasswordNeededError
    from telethon.sessions import StringSession
except ImportError as exc:  # pragma: no cover - критическая ошибка конфигурации окружения
    raise SystemExit(
        "[ERROR] Требуется установить telethon: pip install telethon"
    ) from exc

try:  # pragma: no cover - опциональная зависимость
    from tqdm.asyncio import tqdm
except ImportError as exc:
    raise SystemExit("[ERROR] Требуется установить tqdm: pip install tqdm") from exc

try:  # pragma: no cover - опциональная зависимость
    from opentele.api import UseCurrentSession
    from opentele.exception import OpenTeleException, TFileNotFound
    from opentele.td import TDesktop

    OPENTELE_AVAILABLE = True
except ImportError:
    OpenTeleException = TFileNotFound = None  # type: ignore
    OPENTELE_AVAILABLE = False


if OPENTELE_AVAILABLE:
    OpenTeleBaseException = OpenTeleException  # type: ignore[assignment]
else:
    class OpenTeleBaseException(Exception):
        """Заглушка для ситуаций без установленного opentele."""

        pass

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


DEFAULT_CONFIG_PATH = Path("telescan.toml")


def _read_toml_file(path: Path) -> Dict[str, Any]:
    """Читает TOML файл, подсказывая об отсутствующих зависимостях."""

    try:  # Python 3.11+
        import tomllib  # type: ignore[attr-defined]
    except ModuleNotFoundError:  # pragma: no cover - fallback для Python < 3.11
        try:
            import tomli as tomllib  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover - информативная ошибка
            raise SystemExit(
                "[ERROR] Для чтения конфигурации нужен пакет tomli: pip install tomli"
            ) from exc

    with path.open("rb") as handle:
        return tomllib.load(handle)


def resolve_config_path(argv: Sequence[str]) -> Optional[Path]:
    """Определяет, какой файл конфигурации следует использовать."""

    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument("--config")
    known_args, _ = config_parser.parse_known_args(argv)

    if known_args.config:
        explicit = Path(known_args.config).expanduser()
        if not explicit.exists():
            raise SystemExit(f"[ERROR] Конфигурационный файл не найден: {explicit}")
        return explicit

    if DEFAULT_CONFIG_PATH.exists():
        return DEFAULT_CONFIG_PATH

    return None


def load_config(path: Optional[Path]) -> Dict[str, Any]:
    """Загружает конфигурацию из TOML файла (или возвращает пустую)."""

    if path is None:
        return {}

    try:
        data = _read_toml_file(path)
    except FileNotFoundError as exc:
        raise SystemExit(f"[ERROR] Конфигурационный файл не найден: {path}") from exc
    except OSError as exc:
        raise SystemExit(f"[ERROR] Не удалось прочитать конфигурацию: {exc}") from exc
    except Exception as exc:  # pragma: no cover - защита от неожиданных ошибок парсинга
        raise SystemExit(f"[ERROR] Ошибка при разборе конфигурации: {exc}") from exc

    if not isinstance(data, dict):
        raise SystemExit(
            "[ERROR] Конфигурационный файл должен содержать TOML таблицу верхнего уровня"
        )

    return data


def _config_section(config: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Извлекает раздел конфигурации, гарантируя корректный тип."""

    value = config.get(name, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        logging.warning("Раздел [%s] в конфигурации должен быть таблицей", name)
        return {}
    return dict(value)


def _config_str(
    section_name: str,
    section: Dict[str, Any],
    key: str,
    default: Optional[str],
) -> Optional[str]:
    if key not in section:
        return default
    value = section[key]
    if value is None:
        return None
    if isinstance(value, (str, Path)):
        return str(value)
    logging.warning(
        "Значение %s.%s должно быть строкой, найдено %r. Используется %r",
        section_name,
        key,
        value,
        default,
    )
    return default


def _config_int(section_name: str, section: Dict[str, Any], key: str, default: int) -> int:
    if key not in section:
        return default
    value = section[key]
    try:
        return int(value)
    except (TypeError, ValueError):
        logging.warning(
            "Значение %s.%s должно быть целым числом, найдено %r. Используется %r",
            section_name,
            key,
            value,
            default,
        )
        return default


def _config_float(section_name: str, section: Dict[str, Any], key: str, default: float) -> float:
    if key not in section:
        return default
    value = section[key]
    try:
        return float(value)
    except (TypeError, ValueError):
        logging.warning(
            "Значение %s.%s должно быть числом, найдено %r. Используется %r",
            section_name,
            key,
            value,
            default,
        )
        return default


def _config_bool(section_name: str, section: Dict[str, Any], key: str, default: bool) -> bool:
    if key not in section:
        return default
    value = section[key]
    if isinstance(value, bool):
        return value
    logging.warning(
        "Значение %s.%s должно быть логическим, найдено %r. Используется %r",
        section_name,
        key,
        value,
        default,
    )
    return default


def apply_logging_config(config: Dict[str, Any]) -> None:
    """Применяет настройки логирования из конфигурации."""

    logging_cfg = _config_section(config, "logging")
    level = _config_str("logging", logging_cfg, "level", None)
    if not level:
        return
    level_name = str(level).upper()
    level_value = logging._nameToLevel.get(level_name)
    if level_value is None:
        logging.warning("Неизвестный уровень логирования в конфиге: %s", level)
        return
    logging.getLogger().setLevel(level_value)


try:  # pragma: no cover - опциональная зависимость
    import socks
except ImportError:  # pragma: no cover
    socks = None
    logging.warning(
        "[WARNING] PySocks не установлен. Поддержка прокси будет недоступна."
    )


# ---------------------------------------------------------------------------
# Data classes & enums
# ---------------------------------------------------------------------------


class SourceType(Enum):
    """Возможные типы входных источников."""

    TELETHON_SESSION_FILE = "session_file"
    TELETHON_SESSION_DIR = "session_dir"
    STRING_SESSION_CSV = "string_csv"
    TDATA_DIRECTORY = "tdata"


@dataclass
class SourceItem:
    """Описание одного входного источника."""

    path: Path
    type: SourceType
    label: str


@dataclass
class SourceError:
    """Описание ошибки при поиске входных источников."""

    path: Path
    error: str


@dataclass
class ApiPair:
    api_id: int
    api_hash: str
    label: str


@dataclass
class ProxySpec:
    raw: str
    scheme: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class CheckResult:
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


@dataclass
class ConversionResult:
    source: str
    status: str
    output: Optional[str]
    duration_s: float
    error: Optional[str] = None
    api_label: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers: API, proxies, filesystem
# ---------------------------------------------------------------------------


def load_api_pairs(path: str) -> List[ApiPair]:
    """Загружает пары API ID/Hash из JSON файла."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise ValueError(f"Не удалось прочитать файл с API ключами: {exc}") from exc

    if not isinstance(data, Sequence):
        raise ValueError("JSON должен содержать массив объектов API ключей")

    result: List[ApiPair] = []
    for idx, item in enumerate(data):
        try:
            api_id = int(item["api_id"])
            api_hash = str(item["api_hash"])
            label = str(item.get("label", f"api_{idx}"))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Неверный формат API ключа #{idx}: {item}") from exc
        result.append(ApiPair(api_id, api_hash, label))

    if not result:
        raise ValueError("Список API ключей пуст")

    return result


def parse_proxy_line(line: str) -> ProxySpec:
    """Парсит строку с описанием прокси."""

    line = line.strip()
    if not line:
        raise ValueError("Пустая строка прокси")

    if "://" not in line:
        # По умолчанию считаем, что это socks5
        line = f"socks5://{line}"

    parsed = urlparse(line)
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"Не удалось разобрать строку прокси: {line}")

    return ProxySpec(
        raw=line,
        scheme=(parsed.scheme or "socks5").lower(),
        host=parsed.hostname,
        port=parsed.port,
        username=parsed.username,
        password=parsed.password,
    )


def load_proxies(path: str) -> List[ProxySpec]:
    proxies: List[ProxySpec] = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw or raw.startswith("#"):
                    continue
                try:
                    proxies.append(parse_proxy_line(raw))
                except ValueError as exc:
                    logging.warning("Пропущен некорректный прокси '%s': %s", raw, exc)
    except FileNotFoundError:
        logging.error("Файл с прокси не найден: %s", path)
    return proxies


def get_telethon_proxy(proxy: ProxySpec) -> tuple:
    if not socks:
        raise RuntimeError("PySocks не установлен, использование прокси невозможно")

    proxy_map = {
        "socks5": socks.SOCKS5,
        "socks5h": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
        "https": socks.HTTP,
    }
    proxy_type = proxy_map.get(proxy.scheme, socks.SOCKS5)
    return (proxy_type, proxy.host, proxy.port, True, proxy.username, proxy.password)


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------


def looks_like_tdata(path: Path) -> bool:
    """Эвристическое определение директории tdata."""

    if not path.is_dir():
        return False

    indicator_files = ["map0", "map1", "map2", "user_data", "key_datas"]
    if any((path / name).exists() for name in indicator_files):
        return True

    hashed_dirs = [p for p in path.iterdir() if p.is_dir() and len(p.name) >= 16]
    return len(hashed_dirs) > 0


def detect_source(path: Path) -> SourceItem:
    """Определяет тип входного источника."""

    if not path.exists():
        raise FileNotFoundError(f"Источник '{path}' не найден")

    if path.is_file():
        suffix = path.suffix.lower()
        if suffix == ".session":
            return SourceItem(path, SourceType.TELETHON_SESSION_FILE, path.stem)
        if suffix in {".csv", ".txt"}:
            # CSV со строковыми сессиями
            return SourceItem(path, SourceType.STRING_SESSION_CSV, path.stem)
        raise ValueError(f"Неизвестный формат файла: {path}")

    session_files = list(path.glob("*.session"))
    if session_files:
        return SourceItem(path, SourceType.TELETHON_SESSION_DIR, path.name)

    # директория
    if looks_like_tdata(path):
        return SourceItem(path, SourceType.TDATA_DIRECTORY, path.name)

    csv_files = list(path.glob("*.csv"))
    if csv_files:
        return SourceItem(path, SourceType.STRING_SESSION_CSV, path.name)

    raise ValueError(f"Не удалось определить формат источника: {path}")


# ---------------------------------------------------------------------------
# Recursive source discovery
# ---------------------------------------------------------------------------


def discover_sources(root: Path, errors: Optional[List[SourceError]] = None) -> List[SourceItem]:
    """Рекурсивно находит поддерживаемые источники внутри каталога."""

    path = root.expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Источник '{path}' не найден")

    sources: List[SourceItem] = []
    seen: Set[Path] = set()

    def add(item: SourceItem) -> None:
        try:
            resolved = item.path.resolve()
        except OSError:
            resolved = item.path
        if resolved in seen:
            return
        seen.add(resolved)
        sources.append(item)

    def register_error(target: Path, message: str) -> None:
        logging.warning("Источник %s пропущен: %s", target, message)
        if errors is not None:
            errors.append(SourceError(target, message))

    def walk(current: Path) -> None:
        if current.is_symlink():
            register_error(current, "символическая ссылка")
            return

        if current.is_file():
            suffix = current.suffix.lower()
            if suffix == ".session":
                add(SourceItem(current, SourceType.TELETHON_SESSION_FILE, current.stem))
            elif suffix in {".csv", ".txt"}:
                add(SourceItem(current, SourceType.STRING_SESSION_CSV, current.stem))
            return

        try:
            session_candidates = list(current.glob("*.session"))
        except OSError as exc:
            register_error(current, str(exc))
            return
        if session_candidates:
            add(SourceItem(current, SourceType.TELETHON_SESSION_DIR, current.name))
            return

        if looks_like_tdata(current):
            add(SourceItem(current, SourceType.TDATA_DIRECTORY, current.name))
            return

        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name.lower())
        except OSError as exc:
            register_error(current, str(exc))
            return

        for entry in entries:
            try:
                walk(entry)
            except Exception as exc:
                if errors is None:
                    raise
                register_error(entry, str(exc))

    if path.is_file():
        try:
            add(detect_source(path))
        except Exception as exc:
            if errors is None:
                raise
            register_error(path, str(exc))
    else:
        walk(path)

    return sources


# ---------------------------------------------------------------------------
# tdata -> Telethon session
# ---------------------------------------------------------------------------


def detect_account_dirs(tdata_path: Path) -> List[str]:
    shared = {
        "countries",
        "emoji",
        "prefix",
        "key_datas",
        "settingss",
        "user_data",
        "usertag",
        "shortcuts-default.json",
        "shortcuts-custom.json",
        "dumps",
        "temp",
        "map",
        "logs",
    }

    candidates: List[str] = []
    fallback: List[str] = []
    for entry in sorted(tdata_path.iterdir()):
        if not entry.is_dir() or entry.name in shared:
            continue
        if len(entry.name) >= 16:
            candidates.append(entry.name)
        else:
            fallback.append(entry.name)

    if candidates:
        for name in fallback:
            if name not in candidates:
                candidates.append(name)
        return candidates

    return fallback


def make_isolated_tdata_copy(src: Path, account_dirname: str) -> Path:
    tmp_root = Path(tempfile.mkdtemp(prefix=f"tdata_iso_{account_dirname}_"))
    dst = tmp_root / "tdata"
    dst.mkdir()

    account_src = src / account_dirname

    def copy_into_dst(root: Path) -> None:
        for entry in root.iterdir():
            try:
                if entry.is_dir():
                    shutil.copytree(entry, dst / entry.name, dirs_exist_ok=True)
                else:
                    shutil.copy2(entry, dst / entry.name)
            except (OSError, shutil.Error) as exc:
                logging.warning("Не удалось скопировать '%s': %s", entry.name, exc)

    # Если сама директория аккаунта уже является корнем tdata (например, tdata лежит в подкаталоге),
    # копируем её содержимое напрямую.
    if account_src.is_dir() and looks_like_tdata(account_src):
        copy_into_dst(account_src)
        return dst

    nested_tdata = account_src / "tdata"
    if nested_tdata.is_dir() and looks_like_tdata(nested_tdata):
        copy_into_dst(nested_tdata)
        for extra in account_src.iterdir():
            if extra == nested_tdata:
                continue
            try:
                if extra.is_dir():
                    continue
                shutil.copy2(extra, dst / extra.name)
            except (OSError, shutil.Error) as exc:
                logging.warning("Не удалось скопировать '%s': %s", extra.name, exc)
        return dst

    shutil.copytree(account_src, dst / account_dirname)
    for entry in src.iterdir():
        if entry.name == account_dirname:
            continue
        try:
            if entry.is_dir():
                if entry.name not in {"dumps", "temp", "log", "logs"}:
                    shutil.copytree(entry, dst / entry.name, dirs_exist_ok=True)
            else:
                shutil.copy2(entry, dst / entry.name)
        except (OSError, shutil.Error) as exc:
            logging.warning("Не удалось скопировать '%s': %s", entry.name, exc)
    return dst


def format_opentele_error(exc: BaseException) -> str:
    """Возвращает человеко-понятное описание исключения opentele."""

    if OPENTELE_AVAILABLE and TFileNotFound is not None and isinstance(exc, TFileNotFound):
        detail = getattr(exc, "message", None)
        suffix = f": {detail}" if detail else ""
        return "Не найден ключ key_data (папка tdata неполная или повреждена)" + suffix
    return str(exc)


def discover_keyfile_candidates(tdata_path: Path) -> List[str]:
    """Возвращает возможные значения keyFile для TDesktop."""

    candidates: List[str] = []
    # Основные файлы вида key_*
    for entry in sorted(tdata_path.glob("key_*")):
        if entry.is_file():
            suffix = entry.name[4:]
            if suffix and suffix not in candidates:
                candidates.append(suffix)

    # Некоторые версии Telegram Desktop хранят ключи в key_datas/
    nested_dir = tdata_path / "key_datas"
    if nested_dir.is_dir():
        for entry in sorted(nested_dir.glob("key_*")):
            if entry.is_file():
                suffix = entry.name[4:]
                if suffix and suffix not in candidates:
                    candidates.append(suffix)
                nested_key = f"datas/{entry.name}"
                if nested_key not in candidates:
                    candidates.append(nested_key)

    return candidates


def load_tdesktop_with_fallbacks(tdata_path: Path) -> TDesktop:
    """Пытается загрузить tdata, перебирая доступные варианты keyFile."""

    last_error: Optional[BaseException] = None
    tried: List[Optional[str]] = []

    keyfile_candidates = [None] + discover_keyfile_candidates(tdata_path)
    for keyfile in keyfile_candidates:
        if keyfile in tried:
            continue
        tried.append(keyfile)
        try:
            kwargs = {"basePath": str(tdata_path)}
            if keyfile:
                kwargs["keyFile"] = keyfile
            tdesk = TDesktop(**kwargs)  # type: ignore[arg-type]
            if tdesk.isLoaded():
                return tdesk
            last_error = RuntimeError("TDesktop не загрузил данные tdata")
        except OpenTeleBaseException as exc:  # pragma: no cover - защитный блок
            if OPENTELE_AVAILABLE and TFileNotFound is not None and isinstance(exc, TFileNotFound):
                last_error = exc
                continue
            last_error = exc
            break
        except Exception as exc:  # pragma: no cover - защитный блок
            last_error = exc
            break

    if last_error is None:
        raise RuntimeError("Не удалось загрузить tdata: не найдены ключи шифрования")
    message = (
        format_opentele_error(last_error)
        if isinstance(last_error, OpenTeleBaseException)
        else str(last_error)
    )
    raise RuntimeError(f"Не удалось загрузить tdata: {message}") from last_error


async def convert_account_async(tdata_copy: Path, out_session: Path) -> None:
    if not OPENTELE_AVAILABLE:
        raise RuntimeError("Требуется библиотека opentele для конвертации tdata")

    tdesk = load_tdesktop_with_fallbacks(tdata_copy)

    client = await tdesk.ToTelethon(session=str(out_session), flag=UseCurrentSession)
    try:
        await client.connect()
    finally:
        if client.is_connected():
            await client.disconnect()


async def _convert_tdata_worker(
    account: str,
    src_tdata: Path,
    output_dir: Path,
    timeout: int,
    keep_temp: bool,
    semaphore: asyncio.Semaphore,
) -> ConversionResult:
    async with semaphore:
        started = time.monotonic()
        tmp_copy: Optional[Path] = None
        session_path = output_dir / f"{account}.session"
        try:
            tmp_copy = await asyncio.to_thread(make_isolated_tdata_copy, src_tdata, account)
            await asyncio.wait_for(
                convert_account_async(tmp_copy, session_path), timeout=timeout
            )
            status = "ok"
            error = None
        except asyncio.TimeoutError:
            status = "timeout"
            error = f"Конвертация превысила таймаут {timeout} сек"
        except OpenTeleBaseException as exc:
            status = "error"
            error = format_opentele_error(exc)
            logging.error("Ошибка конвертации tdata (%s): %s", account, error)
        except Exception as exc:
            status = "error"
            error = str(exc)
            logging.error("Ошибка конвертации tdata (%s): %s", account, exc)
        finally:
            duration = time.monotonic() - started
            if tmp_copy and not keep_temp:
                try:
                    await asyncio.to_thread(shutil.rmtree, tmp_copy.parent)
                except Exception as exc:  # pragma: no cover - best effort cleanup
                    logging.warning("Не удалось удалить временную директорию %s: %s", tmp_copy.parent, exc)

        return ConversionResult(
            source=account,
            status=status,
            output=str(session_path) if status == "ok" else None,
            duration_s=duration,
            error=error,
        )


async def convert_tdata_directory(
    src_tdata: Path,
    output_dir: Path,
    timeout: int,
    keep_temp: bool,
    parallel: int,
) -> List[ConversionResult]:
    if not src_tdata.is_dir():
        raise FileNotFoundError(f"Директория tdata не найдена: {src_tdata}")

    accounts = detect_account_dirs(src_tdata)
    if not accounts:
        logging.warning("В папке %s не найдено аккаунтов", src_tdata)
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(max(1, parallel))
    tasks = [
        _convert_tdata_worker(account, src_tdata, output_dir, timeout, keep_temp, semaphore)
        for account in accounts
    ]

    results: List[ConversionResult] = []
    start_ts = time.monotonic()
    with tqdm(total=len(tasks), desc="Конвертация tdata", unit="акк") as progress:
        for future in asyncio.as_completed(tasks):
            res = await future
            results.append(res)
            progress.update(1)
            elapsed = time.monotonic() - start_ts
            cpm = (len(results) / elapsed * 60) if elapsed > 0 else 0.0
            progress.set_postfix(cpm=f"{cpm:.1f}")
            if res.status != "ok" and res.error:
                progress.set_postfix_str(f"Ошибка {res.source}: {res.error[:40]}")
    return results


# ---------------------------------------------------------------------------
# Telethon session -> tdata
# ---------------------------------------------------------------------------


async def convert_session_to_tdata(
    session_file: Path,
    out_dir: Path,
    api_pair: ApiPair,
    timeout: int,
) -> ConversionResult:
    if not OPENTELE_AVAILABLE:
        raise RuntimeError("opentele обязателен для экспорта в tdata")

    started = time.monotonic()
    out_account_dir = out_dir / session_file.stem
    out_account_dir.mkdir(parents=True, exist_ok=True)

    client = TelegramClient(str(session_file), api_pair.api_id, api_pair.api_hash)
    try:
        await asyncio.wait_for(client.connect(), timeout=timeout)
        if not await client.is_user_authorized():
            raise RuntimeError("Сессия не авторизована")

        me = await client.get_me()
        logging.debug(
            "Экспортируем %s (user_id=%s)", session_file.name, getattr(me, "id", None)
        )

        desktop = await asyncio.wait_for(
            client.ToTDesktop(flag=UseCurrentSession), timeout=timeout
        )
        await asyncio.to_thread(desktop.SaveTData, str(out_account_dir))
        status = "ok"
        error = None
    except Exception as exc:
        status = "error"
        error = str(exc)
        logging.error("Не удалось конвертировать %s: %s", session_file.name, exc)
    finally:
        try:
            await client.disconnect()
        except Exception:  # pragma: no cover - best effort cleanup
            pass

    return ConversionResult(
        source=str(session_file),
        status=status,
        output=str(out_account_dir) if status == "ok" else None,
        duration_s=time.monotonic() - started,
        error=error,
        api_label=api_pair.label,
    )


async def convert_sessions_directory(
    sessions_dir: Path,
    out_dir: Path,
    api_pairs: Sequence[ApiPair],
    concurrency: int,
    timeout: int,
) -> List[ConversionResult]:
    session_files = list_session_files(sessions_dir)
    if not session_files:
        logging.warning("В директории %s не найдено файлов .session", sessions_dir)
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(max(1, concurrency))
    api_rotator = ApiRotator(list(api_pairs))

    async def worker(index: int, path: Path) -> ConversionResult:
        async with semaphore:
            api = api_rotator.pick(index)
            return await convert_session_to_tdata(path, out_dir, api, timeout)

    tasks = [worker(idx, path) for idx, path in enumerate(session_files)]
    results: List[ConversionResult] = []
    start_ts = time.monotonic()

    with tqdm(total=len(tasks), desc="Сессии → tdata", unit="файл") as progress:
        for fut in asyncio.as_completed(tasks):
            res = await fut
            results.append(res)
            progress.update(1)
            elapsed = time.monotonic() - start_ts
            cpm = (len(results) / elapsed * 60) if elapsed > 0 else 0.0
            progress.set_postfix(cpm=f"{cpm:.1f}")
            if res.status != "ok" and res.error:
                progress.set_postfix_str(f"Ошибка: {res.error[:40]}")

    return results


# ---------------------------------------------------------------------------
# Session checking core
# ---------------------------------------------------------------------------


class ApiRotator:
    def __init__(self, apis: Sequence[ApiPair]):
        apis = list(apis)
        if not apis:
            raise ValueError("Список API ключей пуст")
        self._apis = apis
        self._count = len(apis)

    def pick(self, index: int) -> ApiPair:
        return self._apis[index % self._count]


class ProxyRotator:
    def __init__(self, proxies: Sequence[ProxySpec], strategy: str):
        self._proxies = list(proxies)
        self._strategy = strategy
        self._sticky: Dict[str, ProxySpec] = {}
        self._counter = 0

    def choose(self, key: str) -> Optional[ProxySpec]:
        if not self._proxies:
            return None
        if self._strategy == "random":
            return random.choice(self._proxies)
        if self._strategy == "sticky":
            if key not in self._sticky:
                self._sticky[key] = random.choice(self._proxies)
            return self._sticky[key]
        # round robin
        proxy = self._proxies[self._counter % len(self._proxies)]
        self._counter += 1
        return proxy


@asynccontextmanager
async def telegram_client_manager(
    session: str | StringSession,
    api_id: int,
    api_hash: str,
    proxy: Optional[tuple],
    timeout: int,
) -> AsyncGenerator[TelegramClient, None]:
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
    start_time = time.monotonic()
    last_error: Optional[str] = "no_session"

    session_arg: Optional[str | StringSession]
    if session_value:
        session_arg = StringSession(session_value)
    elif session_file:
        session_arg = str(session_file)
    else:
        session_arg = None

    if not session_arg:
        return CheckResult(
            key,
            False,
            last_error,
            None,
            None,
            None,
            None,
            datetime.utcnow().isoformat(),
            api_pair.label,
            proxy_spec.raw if proxy_spec else None,
            None,
            0,
            0.0,
        )

    proxy_tuple = None
    if proxy_spec:
        try:
            proxy_tuple = get_telethon_proxy(proxy_spec)
        except RuntimeError as exc:
            return CheckResult(
                key,
                False,
                f"proxy_error:{exc}",
                None,
                None,
                None,
                None,
                datetime.utcnow().isoformat(),
                api_pair.label,
                proxy_spec.raw,
                None,
                0,
                0.0,
            )

    for attempt in range(1, max_attempts + 1):
        started_attempt = time.monotonic()
        try:
            async with telegram_client_manager(
                session_arg, api_pair.api_id, api_pair.api_hash, proxy_tuple, connect_timeout
            ) as client:
                if rate_delay > 0:
                    await asyncio.sleep(rate_delay)

                if not await client.is_user_authorized():
                    latency = int((time.monotonic() - started_attempt) * 1000)
                    return CheckResult(
                        key,
                        False,
                        "unauthorized",
                        None,
                        None,
                        None,
                        None,
                        datetime.utcnow().isoformat(),
                        api_pair.label,
                        proxy_spec.raw if proxy_spec else None,
                        latency,
                        attempt,
                        time.monotonic() - start_time,
                    )

                me = await client.get_me()
                latency = int((time.monotonic() - started_attempt) * 1000)
                first_name = getattr(me, "first_name", None)
                last_name = getattr(me, "last_name", None)
                if first_name and last_name:
                    display_name = f"{first_name} {last_name}".strip()
                else:
                    display_name = first_name or last_name

                return CheckResult(
                    key,
                    True,
                    None,
                    getattr(me, "id", None),
                    getattr(me, "username", None),
                    display_name,
                    getattr(me, "phone", None),
                    datetime.utcnow().isoformat(),
                    api_pair.label,
                    proxy_spec.raw if proxy_spec else None,
                    latency,
                    attempt,
                    time.monotonic() - start_time,
                )
        except FloodWaitError as exc:
            last_error = f"FloodWait:{exc.seconds}s"
            wait_time = min(300, backoff_base**attempt + exc.seconds)
            logging.warning("FloodWait для %s. Пауза %.1f сек", key, wait_time)
            await asyncio.sleep(wait_time)
        except SessionPasswordNeededError:
            last_error = "2FA_enabled"
            break
        except RPCError as exc:
            last_error = f"RPCError:{type(exc).__name__}"
            await asyncio.sleep(min(60, backoff_base**attempt))
        except (asyncio.TimeoutError, OSError):
            last_error = "timeout"
            await asyncio.sleep(min(60, backoff_base**attempt))
        except Exception as exc:  # pragma: no cover - неизвестные ошибки
            last_error = f"UnknownError:{type(exc).__name__}"
            logging.error("Неизвестная ошибка при проверке %s: %s", key, exc)
            await asyncio.sleep(min(30, backoff_base**attempt))

    return CheckResult(
        key,
        False,
        last_error,
        None,
        None,
        None,
        None,
        datetime.utcnow().isoformat(),
        api_pair.label,
        proxy_spec.raw if proxy_spec else None,
        None,
        max_attempts,
        time.monotonic() - start_time,
    )


# ---------------------------------------------------------------------------
# Reporting & task preparation
# ---------------------------------------------------------------------------


def list_session_files(folder: Path) -> List[Path]:
    if not folder.is_dir():
        raise FileNotFoundError(f"Директория {folder} не найдена")
    session_files: List[Path] = []
    for candidate in folder.rglob("*"):
        if candidate.is_file() and candidate.suffix.lower() == ".session":
            session_files.append(candidate)
    session_files.sort()
    return session_files


def load_string_sessions_csv(path: Path) -> List[Tuple[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(f"CSV файл не найден: {path}")

    sessions: List[Tuple[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for idx, row in enumerate(reader, start=1):
            if not row:
                continue
            if len(row) == 1:
                key, session_str = f"row_{idx}", row[0].strip()
            else:
                key, session_str = row[0].strip(), row[1].strip()
            if key and session_str:
                sessions.append((key, session_str))
            else:
                logging.warning("Пропущена пустая строка CSV #%d", idx)
    return sessions


async def prepare_tasks(
    args: argparse.Namespace,
    sources: Sequence[SourceItem],
) -> Tuple[List[Tuple[str, Optional[str], Optional[Path]]], List[ConversionResult]]:
    tasks: List[Tuple[str, Optional[str], Optional[Path]]] = []
    conversions: List[ConversionResult] = []

    for item in sources:
        logging.info("Источник '%s' определён как %s", item.path, item.type.value)
        if item.type == SourceType.TELETHON_SESSION_FILE:
            tasks.append((item.label, None, item.path))
        elif item.type == SourceType.TELETHON_SESSION_DIR:
            for file_path in list_session_files(item.path):
                tasks.append((file_path.stem, None, file_path))
        elif item.type == SourceType.STRING_SESSION_CSV:
            for key, session_str in load_string_sessions_csv(item.path):
                tasks.append((key, session_str, None))
        elif item.type == SourceType.TDATA_DIRECTORY:
            conversion_results = await convert_tdata_directory(
                item.path,
                Path(args.convert_out),
                args.convert_timeout,
                args.keep_temp,
                max(1, min(args.convert_parallel, 8)),
            )
            conversions.extend(conversion_results)
            for res in conversion_results:
                if res.status == "ok" and res.output:
                    tasks.append((f"tdata_{item.label}_{res.source}", None, Path(res.output)))
        else:  # pragma: no cover - безопасность от будущих расширений
            raise RuntimeError(f"Необработанный тип источника: {item.type}")

    unique: Dict[str, Tuple[str, Optional[str], Optional[Path]]] = {}
    for task in tasks:
        if task[0] not in unique:
            unique[task[0]] = task
    if len(unique) != len(tasks):
        logging.info("Удалено %d дубликатов", len(tasks) - len(unique))

    return list(unique.values()), conversions


def generate_reports(
    results: Sequence[CheckResult],
    tasks: Sequence[Tuple[str, Optional[str], Optional[Path]]],
    out_path: Path,
) -> None:
    fieldnames = list(CheckResult.__annotations__.keys())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    valid_path = out_path.with_name("valid.csv")
    invalid_path = out_path.with_name("invalid.csv")

    valid_dir = out_path.with_name("valid_sessions")
    invalid_dir = out_path.with_name("invalid_sessions")
    valid_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)

    results_map = {item.key: item for item in results}

    try:
        with out_path.open("w", newline="", encoding="utf-8") as main_file, \
            valid_path.open("w", newline="", encoding="utf-8") as valid_file, \
            invalid_path.open("w", newline="", encoding="utf-8") as invalid_file:

            main_writer = csv.DictWriter(main_file, fieldnames=fieldnames)
            valid_writer = csv.DictWriter(valid_file, fieldnames=fieldnames)
            invalid_writer = csv.DictWriter(invalid_file, fieldnames=fieldnames)
            main_writer.writeheader()
            valid_writer.writeheader()
            invalid_writer.writeheader()

            for result in results:
                row = asdict(result)
                main_writer.writerow(row)
                if result.ok:
                    valid_writer.writerow(row)
                else:
                    invalid_writer.writerow(row)

        for key, _, session_path in tasks:
            if session_path and session_path.exists():
                result = results_map.get(key)
                if not result:
                    continue
                target_dir = valid_dir if result.ok else invalid_dir
                try:
                    shutil.copy2(session_path, target_dir / session_path.name)
                except (OSError, IOError) as exc:
                    logging.warning("Не удалось скопировать %s: %s", session_path, exc)

        logging.info(
            "Отчёты сохранены: %s, %s, %s", out_path.name, valid_path.name, invalid_path.name
        )
    except IOError as exc:
        logging.error("Ошибка записи отчёта: %s", exc)


def write_convert_report(
    conversions: Sequence[ConversionResult], output_dir: Path
) -> Optional[Path]:
    if not conversions:
        return None

    report_path = output_dir / "convert_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(
                [asdict(item) for item in conversions],
                handle,
                ensure_ascii=False,
                indent=2,
            )
    except IOError as exc:
        logging.error("Ошибка записи отчёта о конвертации: %s", exc)
        return None

    logging.info("Отчёт по конвертации сохранён: %s", report_path)
    return report_path


# ---------------------------------------------------------------------------
# Checker runner
# ---------------------------------------------------------------------------


async def run_checker(
    args: argparse.Namespace,
    tasks: Sequence[Tuple[str, Optional[str], Optional[Path]]],
) -> List[CheckResult]:
    total = len(tasks)
    apis = load_api_pairs(args.apis)
    proxies = load_proxies(args.proxies) if args.proxies else []

    api_rotator = ApiRotator(apis)
    proxy_rotator = ProxyRotator(proxies, args.proxy_strategy)
    semaphore = asyncio.Semaphore(args.concurrency)

    async def worker(idx: int, task: Tuple[str, Optional[str], Optional[Path]]) -> CheckResult:
        key, session_str, session_file = task
        async with semaphore:
            api = api_rotator.pick(idx)
            proxy = proxy_rotator.choose(key)

            if args.dry_run:
                await asyncio.sleep(0.01)
                return CheckResult(
                    key,
                    False,
                    "dry_run",
                    None,
                    None,
                    None,
                    None,
                    datetime.utcnow().isoformat(),
                    api.label,
                    proxy.raw if proxy else None,
                    None,
                    0,
                    0.0,
                )

            return await check_session_async(
                key,
                session_str,
                session_file,
                api,
                proxy,
                args.rate_delay,
                args.connect_timeout,
                args.max_attempts,
                args.backoff_base,
            )

    coroutines = [worker(idx, task) for idx, task in enumerate(tasks)]
    results: List[CheckResult] = []
    start_ts = time.monotonic()

    progress = tqdm(total=total, desc="Проверка сессий", unit="акк")
    try:
        for future in asyncio.as_completed(coroutines):
            result = await future
            results.append(result)
            progress.update(1)
            elapsed = time.monotonic() - start_ts
            cpm = (len(results) / elapsed * 60) if elapsed > 0 else 0.0
            progress.set_postfix(cpm=f"{cpm:.1f}")
    finally:
        progress.close()

    ok_count = sum(1 for item in results if item.ok)
    logging.info("Проверка завершена. Валидных: %d / %d", ok_count, len(results))
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(
    config: Optional[Dict[str, Any]] = None,
    config_path: Optional[Path] = None,
) -> argparse.ArgumentParser:
    cfg = config or {}
    check_cfg = _config_section(cfg, "check")
    convert_cfg = _config_section(cfg, "convert")

    parser = argparse.ArgumentParser(
        prog="telescan",
        description="Универсальный инструмент работы с Telegram сессиями",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default=str(config_path) if config_path else None,
        help="Путь к конфигурационному файлу TOML",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- CHECK ---
    check_parser = subparsers.add_parser(
        "check",
        help="Проверка валидности сессий",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    check_parser.add_argument(
        "inputs",
        nargs="+",
        help="Пути к источникам (.session, директориям, CSV, tdata)",
    )

    check_apis_default = _config_str("check", check_cfg, "apis", None)
    check_parser.add_argument(
        "--apis",
        required=check_apis_default is None,
        default=check_apis_default,
        help="JSON файл с API ключами",
    )

    check_proxies_default = _config_str("check", check_cfg, "proxies", None)
    check_parser.add_argument(
        "--proxies",
        default=check_proxies_default,
        help="Файл со списком прокси",
    )

    check_proxy_strategy_default = _config_str("check", check_cfg, "proxy_strategy", "round")
    check_parser.add_argument(
        "--proxy-strategy",
        choices=["round", "random", "sticky"],
        default=check_proxy_strategy_default,
        help="Стратегия выбора прокси",
    )

    concurrency_default = _config_int("check", check_cfg, "concurrency", 20)
    check_parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=concurrency_default,
        help="Количество одновременных проверок (1-50)",
    )

    check_parser.add_argument(
        "--max-attempts",
        type=int,
        default=_config_int("check", check_cfg, "max_attempts", 3),
        help="Повторы при ошибках",
    )
    check_parser.add_argument(
        "--connect-timeout",
        type=int,
        default=_config_int("check", check_cfg, "connect_timeout", 20),
        help="Таймаут подключения",
    )
    check_parser.add_argument(
        "--backoff-base",
        type=float,
        default=_config_float("check", check_cfg, "backoff_base", 1.7),
        help="Основание backoff",
    )
    check_parser.add_argument(
        "--rate-delay",
        type=float,
        default=_config_float("check", check_cfg, "rate_delay", 0.3),
        help="Задержка перед запросами",
    )
    check_parser.add_argument(
        "--out",
        default=_config_str("check", check_cfg, "out", "./reports/report.csv"),
        help="Путь к CSV отчёту",
    )
    check_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=_config_bool("check", check_cfg, "dry_run", False),
        help="Без реальных запросов",
    )

    check_parser.add_argument(
        "--convert-out",
        default=_config_str("check", check_cfg, "convert_out", "./converted_sessions"),
        help="Папка для сохранения конвертированных из tdata сессий",
    )
    check_parser.add_argument(
        "--tdata-root",
        default=_config_str("check", check_cfg, "tdata_root", None),
        help="Каталог с множеством поддиректорий tdata",
    )
    check_parser.add_argument(
        "--session-dir",
        default=_config_str("check", check_cfg, "session_dir", None),
        help="Каталог, где лежат .session файлы",
    )
    check_parser.add_argument(
        "--convert-timeout",
        type=int,
        default=_config_int("check", check_cfg, "convert_timeout", 60),
        help="Таймаут конвертации tdata",
    )
    check_parser.add_argument(
        "--convert-parallel",
        type=int,
        default=_config_int("check", check_cfg, "convert_parallel", 2),
        help="Параллелизм конвертации tdata",
    )
    check_parser.add_argument(
        "--keep-temp",
        action="store_true",
        default=_config_bool("check", check_cfg, "keep_temp", False),
        help="Не удалять временные копии tdata",
    )

    # --- CONVERT ---
    convert_parser = subparsers.add_parser(
        "convert",
        help="Конвертация tdata <-> Telethon session",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    convert_parser.add_argument(
        "mode",
        choices=["tdata-to-session", "session-to-tdata"],
        help="Направление конвертации",
    )
    convert_parser.add_argument("input", help="Путь к исходным данным")

    convert_output_default = _config_str("convert", convert_cfg, "output", None)
    convert_parser.add_argument(
        "--output",
        required=convert_output_default is None,
        default=convert_output_default,
        help="Директория для результата",
    )
    convert_parser.add_argument(
        "--timeout",
        type=int,
        default=_config_int("convert", convert_cfg, "timeout", 60),
        help="Таймаут на единицу работы",
    )
    convert_parser.add_argument(
        "--parallel",
        type=int,
        default=_config_int("convert", convert_cfg, "parallel", 4),
        help="Параллелизм (1-8)",
    )
    convert_parser.add_argument(
        "--keep-temp",
        action="store_true",
        default=_config_bool("convert", convert_cfg, "keep_temp", False),
        help="Не удалять временные tdata (только tdata→session)",
    )
    convert_parser.add_argument(
        "--apis",
        default=_config_str("convert", convert_cfg, "apis", None),
        help="JSON с API ключами (нужно для session→tdata)",
    )

    return parser


async def handle_check(args: argparse.Namespace) -> int:
    args.concurrency = max(1, min(50, args.concurrency))
    sources: List[SourceItem] = []
    seen_paths: Set[Path] = set()
    discovery_errors: List[SourceError] = []

    def add_source(item: SourceItem) -> None:
        try:
            resolved = item.path.resolve()
        except OSError:
            resolved = item.path
        if resolved in seen_paths:
            return
        seen_paths.add(resolved)
        sources.append(item)

    def extend_with_path(raw: str, only_tdata: bool = False) -> None:
        path = Path(raw).expanduser()
        if only_tdata and not path.is_dir():
            message = "Каталог с tdata не найден"
            logging.error("%s: %s", message, path)
            discovery_errors.append(SourceError(path, message))
            return
        try:
            discovered = discover_sources(path, discovery_errors)
        except FileNotFoundError:
            message = "Источник не найден"
            logging.error("%s: %s", message, path)
            discovery_errors.append(SourceError(path, message))
            return
        except Exception as exc:
            message = f"Не удалось обработать источник: {exc}"
            logging.error("%s (%s)", message, raw)
            discovery_errors.append(SourceError(path, message))
            return

        added = 0
        for item in discovered:
            if only_tdata and item.type != SourceType.TDATA_DIRECTORY:
                continue
            add_source(item)
            added += 1

        if added == 0:
            if only_tdata:
                message = (
                    "В каталоге не найдено поддиректорий, похожих на tdata"
                )
                logging.warning("%s: %s", message, path)
                discovery_errors.append(SourceError(path, message))
            else:
                message = "Источник не содержит поддерживаемых файлов"
                logging.warning("%s: %s", message, path)
                discovery_errors.append(SourceError(path, message))

    all_inputs = list(args.inputs)
    if args.session_dir:
        all_inputs.append(args.session_dir)

    for raw in all_inputs:
        extend_with_path(raw)

    if args.tdata_root:
        extend_with_path(args.tdata_root, only_tdata=True)

    if not sources and not discovery_errors:
        logging.error("Не найдено корректных источников для проверки")
        return 1

    tasks, conversions = await prepare_tasks(args, sources)
    write_convert_report(conversions, Path(args.convert_out))

    error_tasks: List[Tuple[str, Optional[str], Optional[Path]]] = []
    error_results: List[CheckResult] = []
    seen_error_keys: Set[str] = set()
    for issue in discovery_errors:
        key = f"source_error:{issue.path}"
        if key in seen_error_keys:
            continue
        seen_error_keys.add(key)
        error_tasks.append((key, None, None))
        error_results.append(
            CheckResult(
                key,
                False,
                f"source_error:{issue.error}",
                None,
                None,
                None,
                None,
                datetime.utcnow().isoformat(),
                None,
                None,
                None,
                0,
                0.0,
            )
        )

    if not tasks and not error_results:
        logging.error("Нет сессий для проверки")
        return 1

    if args.dry_run and tasks:
        logging.info("Режим dry-run: сетевые запросы выполняться не будут")

    results: List[CheckResult] = []
    if tasks:
        results = await run_checker(args, tasks)

    results.extend(error_results)
    all_tasks = list(tasks) + error_tasks
    generate_reports(results, all_tasks, Path(args.out))
    return 0


async def handle_convert(args: argparse.Namespace) -> int:
    mode = args.mode
    input_path = Path(args.input).expanduser()
    output_dir = Path(args.output).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    if mode == "tdata-to-session":
        if not input_path.exists():
            logging.error("Каталог %s не найден", input_path)
            return 1
        if input_path.is_file():
            logging.error("Для конвертации tdata требуется указать директорию: %s", input_path)
            return 1

        input_is_tdata = looks_like_tdata(input_path)
        if input_is_tdata:
            tdata_dirs = [input_path]
        else:
            try:
                discovered = discover_sources(input_path)
            except Exception as exc:
                logging.error("Не удалось разобрать каталог %s: %s", input_path, exc)
                return 1
            tdata_dirs = [item.path for item in discovered if item.type == SourceType.TDATA_DIRECTORY]

        if not tdata_dirs:
            logging.error("В %s не найдено поддиректорий, похожих на tdata", input_path)
            return 1

        parallel = max(1, min(args.parallel, 8))
        multi_output = len(tdata_dirs) > 1 or not input_is_tdata
        results: List[ConversionResult] = []
        for tdata_dir in tdata_dirs:
            target_output = output_dir if not multi_output else output_dir / tdata_dir.name
            sub_results = await convert_tdata_directory(
                tdata_dir,
                target_output,
                args.timeout,
                args.keep_temp,
                parallel,
            )
            results.extend(sub_results)
    else:
        if not args.apis:
            logging.error("Для конвертации session→tdata требуется указать --apis")
            return 1
        api_pairs = load_api_pairs(args.apis)
        if not input_path.exists():
            logging.error("Источник %s не найден", input_path)
            return 1
        if input_path.is_file() and input_path.suffix == ".session":
            session_dir = Path(tempfile.mkdtemp(prefix="session_single_"))
            shutil.copy2(input_path, session_dir / input_path.name)
            cleanup_dir = session_dir
        else:
            session_dir = input_path
            cleanup_dir = None

        try:
            results = await convert_sessions_directory(
                session_dir,
                output_dir,
                api_pairs,
                max(1, min(args.parallel, 16)),
                args.timeout,
            )
        finally:
            if cleanup_dir:
                shutil.rmtree(cleanup_dir, ignore_errors=True)

    summary_path = write_convert_report(results, output_dir)
    ok_count = sum(1 for item in results if item.status == "ok")
    logging.info("Готово: %d/%d успешно", ok_count, len(results))
    if summary_path:
        logging.info("Отчёт: %s", summary_path)
    return 0 if ok_count == len(results) else 2


async def main(argv: Optional[Sequence[str]] = None) -> int:
    argv_list = list(argv) if argv is not None else sys.argv[1:]

    config_path = resolve_config_path(argv_list)
    config_data = load_config(config_path)

    parser = build_parser(config_data, config_path)
    args = parser.parse_args(argv_list)

    final_config_path: Optional[Path]
    if args.config:
        final_config_path = Path(args.config).expanduser()
    else:
        final_config_path = config_path

    if final_config_path and (config_path is None or final_config_path != config_path):
        config_data = load_config(final_config_path)

    apply_logging_config(config_data)

    if args.command == "check":
        return await handle_check(args)
    if args.command == "convert":
        return await handle_convert(args)
    parser.error("Неизвестная команда")  # pragma: no cover
    return 1


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        logging.info("Операция прервана пользователем")
        sys.exit(130)
    except Exception as exc:  # pragma: no cover - safety net
        logging.critical("Необработанная ошибка: %s", exc, exc_info=True)
        sys.exit(1)
