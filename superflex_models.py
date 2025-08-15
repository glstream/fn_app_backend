from pydantic import BaseModel
from typing import Optional, List


class UserDataModel(BaseModel):
    user_name: str
    league_year: str
    guid: str
    platform: Optional[str] = "sleeper"  # Default to sleeper for backward compatibility
    timestamp: Optional[str] = None  # For cache busting
    league_ids: Optional[List[str]] = None  # For Fleaflicker league IDs


class LeagueDataModel(BaseModel):
    league_id: str


class RosterDataModel(BaseModel):
    league_id: str
    user_id: str
    guid: str
    league_year: str
    platform: Optional[str] = None  # Will be detected from database if not provided
    timestamp: Optional[str] = None  # For cache busting


class RanksDataModel(BaseModel):
    user_id: str
    display_name: str
    league_id: str
    rank_source: str
    power_rank: int
    starters_rank: int
    bench_rank: int
    picks_rank: int

