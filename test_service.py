from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import requests

BASE_URL = os.environ.get("REC_SERVICE_BASE_URL", "http://127.0.0.1:8000")
REQUEST_TIMEOUT = float(os.environ.get("REC_SERVICE_TIMEOUT", "120"))

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR / "data"
PREPARED_DIR = DATA_ROOT / "prepared"
RECS_DIR = DATA_ROOT / "recommendations"

EVENTS_PATH = PREPARED_DIR / "events.parquet"
ALS_RECS_PATH = RECS_DIR / "als_recommendations.parquet"

DEFAULT_K = 10


def detect_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(f"Expected one of columns {candidates}, got {list(df.columns)}")


def get_demo_user_id() -> int:
    """Один user_id: есть в ALS и есть строки в events — для трёх стратегий через флаги API."""
    if not ALS_RECS_PATH.exists():
        raise FileNotFoundError(
            f"Нет файла {ALS_RECS_PATH}. Сервис грузит только его — положите parquet или запустите сервис для bootstrap."
        )
    if not EVENTS_PATH.exists():
        raise FileNotFoundError(f"Нет файла {EVENTS_PATH}.")

    events = pd.read_parquet(EVENTS_PATH)
    personal = pd.read_parquet(ALS_RECS_PATH)

    events_user_col = detect_column(events, ["user_id", "userid"])
    personal_user_col = detect_column(personal, ["user_id", "userid"])

    events_users = set(map(int, events[events_user_col].dropna().unique()))
    personal_users = set(map(int, personal[personal_user_col].dropna().unique()))

    intersection = sorted(personal_users & events_users)
    if not intersection:
        raise RuntimeError(
            "Нет пересечения user_id между events.parquet и als_recommendations.parquet. "
            "Нужен хотя бы один пользователь с персональными рекомендациями и историей в events."
        )
    return intersection[0]


def post_recommendations(
    user_id: int,
    *,
    ignore_offline: bool = False,
    ignore_history: bool = False,
    k: int = DEFAULT_K,
) -> requests.Response:
    payload = {
        "user_id": user_id,
        "k": k,
        "ignore_offline": ignore_offline,
        "ignore_history": ignore_history,
    }
    return requests.post(
        f"{BASE_URL}/recommendations",
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )


def print_response(title: str, response: requests.Response) -> None:
    print("=" * 70)
    print(title)
    print("status:", response.status_code)

    try:
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except Exception:
        print(response.text)


def test_health() -> None:
    r = requests.get(f"{BASE_URL}/health", timeout=REQUEST_TIMEOUT)
    print_response("HEALTHCHECK", r)


def test_strategy_popular_fallback(user_id: int) -> None:
    """Без персональных: ignore_offline — как будто пользователя нет в ALS."""
    r = post_recommendations(user_id, ignore_offline=True, ignore_history=False)
    print_response(
        "1) Без персональных (ignore_offline=true → popular_fallback, история учитывается)",
        r,
    )


def test_strategy_offline_only(user_id: int) -> None:
    """Только персональные: ignore_history — как будто нет онлайн-истории."""
    r = post_recommendations(user_id, ignore_offline=False, ignore_history=True)
    print_response(
        "2) Персональные без учёта истории (ignore_history=true → offline_only)",
        r,
    )


def test_strategy_hybrid_interleaved(user_id: int) -> None:
    """Персональные + история: полная логика с чередованием."""
    r = post_recommendations(user_id, ignore_offline=False, ignore_history=False)
    print_response(
        "3) Персональные + онлайн-история (hybrid_interleaved)",
        r,
    )


if __name__ == "__main__":
    uid = get_demo_user_id()
    print("Три стратегии для одного user_id:", uid)
    print(
        "(флаги ignore_offline / ignore_history только для демонстрации веток сервиса; "
        "в бою обычно не передаются.)\n"
    )

    test_health()
    test_strategy_popular_fallback(uid)
    test_strategy_offline_only(uid)
    test_strategy_hybrid_interleaved(uid)
