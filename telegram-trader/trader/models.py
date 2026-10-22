from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def decimal(value) -> Decimal:
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


class Signal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: Literal["open", "close", "cancel", "tp1", "tp2", "sl", "targets", "ignore", "review"]
    symbol: str | None = Field(default=None, pattern=r"^[A-Z0-9]{2,20}$")
    side: Literal["long", "short"] | None = None
    price: str | None = None
    target_trade_id: str | None = None
    reason: str = ""
    levels: dict[Literal["sl", "tp1", "tp2"], str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_signal(self):
        for value in [self.price, *self.levels.values()]:
            if value is not None and decimal(value) <= 0:
                raise ValueError("prices must be finite and positive")
        if self.action == "open" and (not self.symbol or not self.side or self.target_trade_id):
            raise ValueError("entry requires symbol and side, not an existing trade")
        if self.action == "sl" and self.price is None:
            raise ValueError("stop price required")
        if self.action == "targets" and not self.levels:
            raise ValueError("target levels required")
        return self


class Decision(Signal):
    event_id: str
    reason: str = Field(min_length=1)
    evidence_message_ids: list[int] = Field(min_length=1)


class Decisions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decisions: list[Decision] = Field(min_length=1, max_length=50)


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
