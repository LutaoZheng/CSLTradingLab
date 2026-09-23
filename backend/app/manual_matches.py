import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

class MarketRule(BaseModel):
    model_config=ConfigDict(extra="forbid")
    ticker:str
    title:str
    group:str
    direction:dict[str,str]=Field(min_length=1)
    settlement_rule:str=Field(min_length=1)

class EventButton(BaseModel):
    model_config=ConfigDict(extra="forbid")
    event_type:str
    label:str
    teams:list[str]=Field(default_factory=list)

    @field_validator("event_type")
    @classmethod
    def allowed_type(cls,value):
        if value not in {"BALL_IN_NET","PENALTY_EVENT","RED_CARD_EVENT","VAR_CHECK","EVENT_VOIDED"}: raise ValueError("unsupported event type")
        return value

class MatchDefinition(BaseModel):
    model_config=ConfigDict(extra="forbid")
    event_ticker:str
    home_team:str
    away_team:str
    start_time:str
    timezone:str
    markets:list[MarketRule]=Field(min_length=1)
    allowed_event_buttons:list[EventButton]=Field(min_length=1)

    @field_validator("start_time")
    @classmethod
    def valid_start(cls,value):
        parsed=datetime.fromisoformat(value.replace("Z","+00:00"))
        if parsed.tzinfo is None: raise ValueError("start_time must include a UTC offset")
        return value

    @model_validator(mode="after")
    def validate_identity(self):
        try: ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc: raise ValueError("timezone must be an IANA timezone") from exc
        tickers=[x.ticker for x in self.markets]
        if len(tickers)!=len(set(tickers)): raise ValueError("market tickers must be unique")
        return self

class MatchConfigFile(BaseModel):
    model_config=ConfigDict(extra="forbid")
    schema_version:int
    matches:list[MatchDefinition]

    @field_validator("schema_version")
    @classmethod
    def version_one(cls,value):
        if value!=1: raise ValueError("unsupported match config schema")
        return value

    @model_validator(mode="after")
    def unique_events(self):
        tickers=[x.event_ticker for x in self.matches]
        if len(tickers)!=len(set(tickers)): raise ValueError("event tickers must be unique")
        return self

def load_match_config(path:Path)->MatchConfigFile:
    if not path.exists(): return MatchConfigFile(schema_version=1,matches=[])
    return MatchConfigFile.model_validate(json.loads(path.read_text(encoding="utf-8")))

def configured_match(path:Path,event_ticker:str)->MatchDefinition|None:
    return next((item for item in load_match_config(path).matches if item.event_ticker==event_ticker),None)
