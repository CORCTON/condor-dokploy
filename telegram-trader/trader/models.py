from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


def decimal(value) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


class Signal(BaseModel):
    action: Literal["open", "close", "tp1", "tp2", "sl", "targets", "ignore", "review"]
    symbol: str | None = None
    side: Literal["long", "short"] | None = None
    price: str | None = None
    reason: str = ""
    levels: dict[str, str] = {}


class Plan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sl: Decimal
    tp1: Decimal
    tp2: Decimal
    tp1_fraction: Decimal
    rationale: str

    @field_validator("sl", "tp1", "tp2", "tp1_fraction", mode="before")
    @classmethod
    def finite(cls, value):
        return decimal(value)

    @model_validator(mode="after")
    def validate_values(self):
        if min(self.sl, self.tp1, self.tp2) <= 0:
            raise ValueError("prices must be positive")
        if not 0 < self.tp1_fraction < 1:
            raise ValueError("tp1_fraction must be between zero and one")
        if not self.rationale.strip():
            raise ValueError("rationale required")
        return self

    def validate_side(self, side: str, entry: Decimal):
        if side == "long" and not self.sl < entry < self.tp1 < self.tp2:
            raise ValueError("invalid long exit ordering")
        if side == "short" and not self.sl > entry > self.tp1 > self.tp2:
            raise ValueError("invalid short exit ordering")
