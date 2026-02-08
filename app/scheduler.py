from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from fsrs import Card, Rating, Scheduler, State


@dataclass
class ReviewResult:
    state: str
    step: int
    due_at: datetime
    stability: float
    difficulty: float
    reps: int
    lapses: int
    elapsed_days: float


def _parse_ts(ts: str) -> datetime:
    if "T" in ts:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _state_from_str(value: str) -> State:
    v = (value or "").strip().lower()
    if v == "review":
        return State.Review
    if v == "relearning":
        return State.Relearning
    return State.Learning


def _state_to_str(state: State) -> str:
    if state == State.Review:
        return "review"
    if state == State.Relearning:
        return "relearning"
    return "learning"


def _rating_from_int(value: int) -> Rating:
    if value == 1:
        return Rating.Again
    if value == 2:
        return Rating.Hard
    if value == 3:
        return Rating.Good
    if value == 4:
        return Rating.Easy
    raise ValueError("rating must be in [1, 4]")


def next_review(
    *,
    state: str,
    step: int,
    stability: float,
    difficulty: float,
    reps: int,
    lapses: int,
    last_review_at: str | None,
    due_at: str | None,
    rating: int,
    desired_retention: float = 0.9,
) -> ReviewResult:
    now = datetime.now(timezone.utc)
    prev = _parse_ts(last_review_at) if last_review_at else None
    due = _parse_ts(due_at) if due_at else now

    scheduler = Scheduler(desired_retention=desired_retention)

    card = Card(
        state=_state_from_str(state),
        step=max(int(step or 0), 0),
        stability=max(float(stability), 0.01) if stability is not None else None,
        difficulty=float(difficulty) if difficulty is not None else None,
        due=due,
        last_review=prev,
    )

    updated, _ = scheduler.review_card(card, _rating_from_int(rating), review_datetime=now)

    elapsed_days = max(((now - prev).total_seconds() / 86400.0), 0.0) if prev else 0.0

    next_lapses = lapses + (1 if rating == 1 else 0)

    return ReviewResult(
        state=_state_to_str(updated.state),
        step=int(updated.step or 0),
        due_at=updated.due,
        stability=float(updated.stability or 0.0),
        difficulty=float(updated.difficulty or 0.0),
        reps=reps + 1,
        lapses=next_lapses,
        elapsed_days=elapsed_days,
    )
