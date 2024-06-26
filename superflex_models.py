from pydantic import BaseModel


class UserDataModel(BaseModel):
    user_name: str
    league_year: str
    guid: str


class LeagueDataModel(BaseModel):
    league_id: str


class RosterDataModel(BaseModel):
    league_id: str
    user_id: str
    guid: str
    league_year: str


class RanksDataModel(BaseModel):
    user_id: str
    display_name: str
    league_id: str
    rank_source: str
    power_rank: int
    starters_rank: int
    bench_rank: int
    picks_rank: int

