from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tqdm import tqdm


BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR / "data"
PREPARED_DIR = DATA_ROOT / "prepared"
RECS_DIR = DATA_ROOT / "recommendations"

EVENTS_PATH = PREPARED_DIR / "events.parquet"
OFFLINE_RECS_PATH = RECS_DIR / "als_recommendations.parquet"
POPULAR_RECS_PATH = RECS_DIR / "top_popular.parquet"


def _offline_parquet_path() -> Path:
    """Файл ALS из ноутбука als_recommendations."""
    if OFFLINE_RECS_PATH.exists():
        return OFFLINE_RECS_PATH
    return OFFLINE_RECS_PATH

DEFAULT_K = 10

app = FastAPI(title="Recommendations Service")


class RecommendationRequest(BaseModel):
    user_id: int
    k: int = DEFAULT_K
    # Для демо/тестов: один и тот же user_id может пройти все три стратегии
    ignore_offline: bool = False
    ignore_history: bool = False


class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[int]
    strategy: str


def detect_column(df: pd.DataFrame, candidates: List[str], file_name: str) -> str:
    for col in candidates:
        if col in df.columns:
            return col

    raise ValueError(
        f"Не найдена нужная колонка в {file_name}. "
        f"Ожидалась одна из: {candidates}. "
        f"Доступные колонки: {list(df.columns)}"
    )


def _bootstrap_demo_parquets() -> None:
    """Минимальные данные для локального запуска и тестов, если файлов ещё нет."""
    offline_ok = _offline_parquet_path().exists()
    need = not offline_ok or not POPULAR_RECS_PATH.exists() or not EVENTS_PATH.exists()
    if not need:
        return

    PREPARED_DIR.mkdir(parents=True, exist_ok=True)
    RECS_DIR.mkdir(parents=True, exist_ok=True)

    if not POPULAR_RECS_PATH.exists():
        pd.DataFrame({"track_id": list(range(9000, 9050))}).to_parquet(
            POPULAR_RECS_PATH, index=False
        )

    if not offline_ok:
        rows = []
        for rank, tid in enumerate(range(100, 130)):
            rows.append({"user_id": 389297, "track_id": tid, "score": 1.0 / (rank + 1)})
        for rank, tid in enumerate(range(200, 235)):
            rows.append({"user_id": 389298, "track_id": tid, "score": 1.0 / (rank + 1)})
        pd.DataFrame(rows).to_parquet(OFFLINE_RECS_PATH, index=False)

    if not EVENTS_PATH.exists():
        pd.DataFrame(
            {
                "user_id": [389297, 389297],
                "track_id": [201, 202],
            }
        ).to_parquet(EVENTS_PATH, index=False)


def load_offline_recs() -> Dict[int, List[int]]:
    _bootstrap_demo_parquets()

    path = _offline_parquet_path()
    if not path.exists():
        print(f"File not found: {OFFLINE_RECS_PATH}")
        return {}

    print("Loading offline recommendations...")
    df = pd.read_parquet(path)

    user_col = detect_column(df, ["user_id", "userid"], path.name)
    item_col = detect_column(df, ["item_id", "track_id", "itemid"], path.name)

    if "rank" in df.columns:
        df = df.sort_values([user_col, "rank"])
    elif "score" in df.columns:
        df = df.sort_values([user_col, "score"], ascending=[True, False])

    df = df.dropna(subset=[item_col])
    df[item_col] = df[item_col].astype(int)

    recs: Dict[int, List[int]] = {}
    n_users = df[user_col].nunique()
    use_tqdm = n_users <= 50_000
    iterator = tqdm(df.groupby(user_col, sort=False), desc="Building offline recs", total=n_users) if use_tqdm else df.groupby(user_col, sort=False)

    for user_id, group in iterator:
        recs[int(user_id)] = group[item_col].tolist()

    print("Offline recommendations loaded")
    return recs


def load_popular_recs() -> List[int]:
    _bootstrap_demo_parquets()

    if not POPULAR_RECS_PATH.exists():
        print(f"File not found: {POPULAR_RECS_PATH}")
        return []

    print("Loading popular recommendations...")
    df = pd.read_parquet(POPULAR_RECS_PATH)

    if "track_id" not in df.columns:
        raise ValueError(
            f"В {POPULAR_RECS_PATH.name} ожидается колонка 'track_id'. "
            f"Доступные колонки: {list(df.columns)}"
        )

    if "rank" in df.columns:
        df = df.sort_values("rank")

    popular = df["track_id"].dropna().astype(int).drop_duplicates().tolist()

    print("Popular recommendations loaded")
    print("Top popular sample:", popular[:10])

    return popular


def get_user_history(user_id: int) -> List[int]:
    _bootstrap_demo_parquets()

    if not EVENTS_PATH.exists():
        return []

    df = pd.read_parquet(EVENTS_PATH)

    user_col = detect_column(df, ["user_id", "userid"], EVENTS_PATH.name)
    item_col = detect_column(df, ["item_id", "track_id", "itemid"], EVENTS_PATH.name)

    filtered = df.loc[df[user_col] == user_id, item_col].dropna()

    if filtered.empty:
        return []

    return filtered.astype(int).tolist()


OFFLINE_RECS: Dict[int, List[int]] | None = None
POPULAR_RECS: List[int] | None = None


def _ensure_recommendation_tables_loaded() -> None:
    global OFFLINE_RECS, POPULAR_RECS
    if OFFLINE_RECS is not None and POPULAR_RECS is not None:
        return
    print("Loading recommendation tables (first request or startup)...")
    OFFLINE_RECS = load_offline_recs()
    POPULAR_RECS = load_popular_recs()
    print("All static data loaded.")


def popular_minus_history(history_items: List[int], popular_items: List[int]) -> List[int]:
    seen = set(history_items)
    return [t for t in popular_items if t not in seen]


def merge_recommendations(
    offline_items: List[int],
    popular_not_in_history: List[int],
    popular_items: List[int],
    history_items: List[int],
    k: int,
) -> List[int]:
    hist = set(history_items)
    off = [t for t in offline_items if t not in hist]
    onl = [t for t in popular_not_in_history if t not in hist]

    result: List[int] = []
    used: Set[int] = set()
    i, j = 0, 0

    while len(result) < k and (i < len(off) or j < len(onl)):
        if i < len(off):
            t = off[i]
            i += 1
            if t not in used:
                result.append(t)
                used.add(t)
        if len(result) >= k:
            break
        if j < len(onl):
            t = onl[j]
            j += 1
            if t not in used:
                result.append(t)
                used.add(t)

    for item_id in popular_items:
        if len(result) >= k:
            break
        if item_id not in used and item_id not in hist:
            result.append(item_id)
            used.add(item_id)

    return result


def generate_recommendations(
    user_id: int,
    k: int,
    *,
    ignore_offline: bool = False,
    ignore_history: bool = False,
) -> RecommendationResponse:
    if k <= 0:
        raise HTTPException(status_code=400, detail="Параметр k должен быть положительным")

    _ensure_recommendation_tables_loaded()
    assert OFFLINE_RECS is not None and POPULAR_RECS is not None

    offline_items = [] if ignore_offline else OFFLINE_RECS.get(user_id, [])
    history_items = [] if ignore_history else get_user_history(user_id)
    popular_fresh = popular_minus_history(history_items, POPULAR_RECS)

    if not offline_items:
        recommendations = [
            item for item in POPULAR_RECS if item not in set(history_items)
        ][:k]

        return RecommendationResponse(
            user_id=user_id,
            recommendations=recommendations,
            strategy="popular_fallback",
        )

    if not history_items:
        recommendations = [
            item for item in offline_items if item not in set(history_items)
        ][:k]

        return RecommendationResponse(
            user_id=user_id,
            recommendations=recommendations,
            strategy="offline_only",
        )

    recommendations = merge_recommendations(
        offline_items=offline_items,
        popular_not_in_history=popular_fresh,
        popular_items=POPULAR_RECS,
        history_items=history_items,
        k=k,
    )

    return RecommendationResponse(
        user_id=user_id,
        recommendations=recommendations,
        strategy="hybrid_interleaved",
    )


@app.get("/health")
def healthcheck() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/recommendations", response_model=RecommendationResponse)
def recommend_get(
    user_id: int,
    k: int = DEFAULT_K,
    ignore_offline: bool = False,
    ignore_history: bool = False,
) -> RecommendationResponse:
    return generate_recommendations(
        user_id=user_id,
        k=k,
        ignore_offline=ignore_offline,
        ignore_history=ignore_history,
    )


@app.post("/recommendations", response_model=RecommendationResponse)
def recommend_post(request: RecommendationRequest) -> RecommendationResponse:
    return generate_recommendations(
        user_id=request.user_id,
        k=request.k,
        ignore_offline=request.ignore_offline,
        ignore_history=request.ignore_history,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "recommendations_service:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
