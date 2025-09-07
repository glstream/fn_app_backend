"""
MyFantasyLeague (MFL) API Client for NFL Fantasy Football data ingestion.

This module provides a comprehensive client for interacting with the MyFantasyLeague API,
following the existing patterns and conventions used in fleaflicker_client.py.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Union
import aiohttp
from utils import get_http_session, CACHE_EXPIRATION, LEAGUE_CACHE_EXPIRATION, SHORT_CACHE_EXPIRATION


class MFLClient:
    """
    MyFantasyLeague API client for NFL fantasy football data.
    
    Provides methods to fetch leagues, rosters, standings, 
    transactions, and player data from MyFantasyLeague platform.
    """
    
    BASE_URL = "https://api.myfantasyleague.com"
    DEFAULT_YEAR = str(datetime.now().year)
    
    def __init__(self):
        self.session = None
        self.cookies = None  # Store authentication cookies
    
    async def _make_api_call(
        self, 
        endpoint: str,
        params: Dict[str, Union[str, int]] = None,
        year: str = None,
        timeout: int = 10,
        max_retries: int = 5
    ) -> Dict:
        """
        Make API call to MFL endpoint with retry logic.
        
        Args:
            endpoint: API endpoint type (e.g., 'league', 'rosters', 'players')
            params: Query parameters including league ID
            year: Year for the API call (defaults to current year)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            
        Returns:
            API response data as dictionary
        """
        if params is None:
            params = {}
        
        # Determine the year
        if year is None:
            year = self.DEFAULT_YEAR
        
        # Build the URL with year
        url = f"{self.BASE_URL}/{year}/export"
        
        # Always set JSON response format
        params["JSON"] = "1"
        
        # Set the export type
        params["TYPE"] = endpoint
        
        session = await get_http_session()
        
        for retry in range(max_retries):
            try:
                async with session.get(
                    url, 
                    params=params,
                    cookies=self.cookies,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    response.raise_for_status()
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                if retry < max_retries - 1:
                    sleep_time = 2 ** retry
                    print(f"MFL API error: {e}. Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"MFL API error: {e}. Max retries reached.")
                    raise
    
    async def authenticate(self, username: str, password: str, year: str = None) -> bool:
        """
        Authenticate with MFL API to get session cookies.
        
        Args:
            username: MFL username
            password: MFL password
            year: Year for authentication
            
        Returns:
            True if authentication successful
        """
        if year is None:
            year = self.DEFAULT_YEAR
            
        params = {
            "USERNAME": username,
            "PASSWORD": password,
            "XML": "1"
        }
        
        try:
            session = await get_http_session()
            url = f"{self.BASE_URL}/{year}/login"
            
            async with session.post(url, data=params) as response:
                if response.status == 200:
                    self.cookies = response.cookies
                    return True
                return False
        except Exception as e:
            print(f"Authentication failed: {e}")
            return False
    
    async def get_league_info(self, league_id: str, year: str = None) -> Dict:
        """
        Fetch comprehensive league information including settings and teams.
        
        Args:
            league_id: MFL league ID
            year: Season year
            
        Returns:
            League information dictionary
        """
        params = {"L": league_id}
        response = await self._make_api_call("league", params, year)
        return response.get("league", {})
    
    async def get_rosters(self, league_id: str, year: str = None, week: str = None) -> Dict:
        """
        Fetch all team rosters in a league.
        
        Args:
            league_id: MFL league ID
            year: Season year
            week: Week number (optional, defaults to current)
            
        Returns:
            League rosters dictionary
        """
        params = {"L": league_id}
        if week:
            params["W"] = week
            
        response = await self._make_api_call("rosters", params, year)
        return response.get("rosters", {})
    
    async def get_standings(self, league_id: str, year: str = None) -> Dict:
        """
        Fetch league standings.
        
        Args:
            league_id: MFL league ID
            year: Season year
            
        Returns:
            League standings dictionary
        """
        params = {"L": league_id}
        response = await self._make_api_call("leagueStandings", params, year)
        return response.get("leagueStandings", {})
    
    async def get_players(self, year: str = None, players: str = None) -> Dict:
        """
        Fetch player information.
        
        Args:
            year: Season year
            players: Comma-separated player IDs (optional)
            
        Returns:
            Player information dictionary
        """
        params = {}
        if players:
            params["PLAYERS"] = players
            
        response = await self._make_api_call("players", params, year)
        return response.get("players", {})
    
    async def get_transactions(
        self, 
        league_id: str, 
        year: str = None,
        trans_type: str = None,
        days: int = None
    ) -> Dict:
        """
        Fetch league transaction history.
        
        Args:
            league_id: MFL league ID
            year: Season year
            trans_type: Transaction type filter (TRADE, WAIVER, etc.)
            days: Number of days to look back
            
        Returns:
            Transaction history dictionary
        """
        params = {"L": league_id}
        if trans_type:
            params["TRANS_TYPE"] = trans_type
        if days:
            params["DAYS"] = str(days)
            
        response = await self._make_api_call("transactions", params, year)
        return response.get("transactions", {})
    
    async def get_draft_results(self, league_id: str, year: str = None) -> Dict:
        """
        Fetch draft results for a league.
        
        Args:
            league_id: MFL league ID
            year: Season year
            
        Returns:
            Draft results dictionary
        """
        params = {"L": league_id}
        response = await self._make_api_call("draftResults", params, year)
        return response.get("draftResults", {})
    
    async def get_future_draft_picks(self, league_id: str, year: str = None) -> Dict:
        """
        Fetch future draft picks for a league.
        
        Args:
            league_id: MFL league ID
            year: Season year
            
        Returns:
            Future draft picks dictionary
        """
        params = {"L": league_id}
        response = await self._make_api_call("futureDraftPicks", params, year)
        return response.get("futureDraftPicks", {})
    
    async def get_scoring(self, league_id: str, year: str = None, week: str = None) -> Dict:
        """
        Fetch scoring data for a specific week.
        
        Args:
            league_id: MFL league ID
            year: Season year
            week: Week number
            
        Returns:
            Scoring data dictionary
        """
        params = {"L": league_id}
        if week:
            params["W"] = week
            
        response = await self._make_api_call("weeklyResults", params, year)
        return response.get("weeklyResults", {})
    
    async def get_matchup(self, league_id: str, year: str = None, week: str = None) -> Dict:
        """
        Fetch matchup/schedule data.
        
        Args:
            league_id: MFL league ID
            year: Season year
            week: Week number (optional)
            
        Returns:
            Matchup data dictionary
        """
        params = {"L": league_id}
        if week:
            params["W"] = week
            
        response = await self._make_api_call("schedule", params, year)
        return response.get("schedule", {})
    
    async def get_league_by_id(self, league_id: str, year: str = None) -> Dict:
        """
        Direct league lookup by ID without authentication.
        This is the main entry point when user provides league ID.
        
        Args:
            league_id: MFL league ID
            year: Season year
            
        Returns:
            Complete league data including rosters
        """
        # Get league info and rosters in parallel
        league_task = self.get_league_info(league_id, year)
        rosters_task = self.get_rosters(league_id, year)
        standings_task = self.get_standings(league_id, year)
        
        league_info, rosters, standings = await asyncio.gather(
            league_task, rosters_task, standings_task
        )
        
        return {
            "league": league_info,
            "rosters": rosters,
            "standings": standings
        }


# Global client instance
mfl_client = MFLClient()


# Utility functions following existing patterns

def normalize_mfl_league_data(league_data: Dict, year: str = None) -> Dict:
    """
    Normalize MFL league data to match internal format.
    
    Args:
        league_data: Raw league data from MFL
        year: League year
        
    Returns:
        Normalized league data dictionary
    """
    league = league_data.get("league", {})
    
    # Parse roster positions
    starters = league.get("starters", {})
    
    # Count position requirements
    qbs = int(starters.get("count", 0)) if starters.get("position") == "QB" else 0
    rbs = int(starters.get("count", 0)) if starters.get("position") == "RB" else 0
    wrs = int(starters.get("count", 0)) if starters.get("position") == "WR" else 0
    tes = int(starters.get("count", 0)) if starters.get("position") == "TE" else 0
    
    # Look for flex positions in starters
    flexes = 0
    super_flexes = 0
    
    # Parse starters which can be a dict or list
    if isinstance(starters, list):
        for starter in starters:
            pos = starter.get("position", "")
            count = int(starter.get("count", 0))
            if pos == "QB":
                qbs = count
            elif pos == "RB":
                rbs = count
            elif pos == "WR":
                wrs = count
            elif pos == "TE":
                tes = count
            elif "FLEX" in pos and "SUPER" not in pos:
                flexes = count
            elif "SUPER" in pos or "OP" in pos:  # OP = Offensive Player (superflex)
                super_flexes = count
    
    total_starters = qbs + rbs + wrs + tes + flexes + super_flexes
    
    # Determine league category (1=standard, 2=superflex, 3=TE premium)
    league_cat = 2 if super_flexes > 0 else 1
    
    franchises = league.get("franchises", {}).get("franchise", [])
    if not isinstance(franchises, list):
        franchises = [franchises]
    
    return {
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "total_rosters": len(franchises),
        "qb_cnt": qbs,
        "rb_cnt": rbs,
        "wr_cnt": wrs,
        "te_cnt": tes,
        "flex_cnt": flexes,
        "sf_cnt": super_flexes,
        "starter_cnt": total_starters,
        "total_roster_cnt": int(league.get("rosterSize", 0)),
        "sport": "nfl",
        "league_year": year or str(datetime.now().year),
        "platform": "mfl",
        "league_cat": league_cat,
        "rf_cnt": 0  # rec_flex not typically in MFL
    }


def normalize_mfl_roster_data(roster_data: Dict, league_id: str) -> List[Dict]:
    """
    Normalize MFL roster data to match internal format.
    
    Args:
        roster_data: Raw roster data from MFL
        league_id: League ID
        
    Returns:
        Normalized roster data list
    """
    normalized_rosters = []
    
    franchises = roster_data.get("franchise", [])
    if not isinstance(franchises, list):
        franchises = [franchises]
    
    for franchise in franchises:
        franchise_id = franchise.get("id")
        
        # Get players - can be a dict with 'player' key or direct list
        players = franchise.get("player", [])
        if not isinstance(players, list):
            players = [players] if players else []
        
        for player in players:
            player_id = player.get("id")
            if player_id:
                normalized_rosters.append({
                    "player_id": player_id,
                    "team_id": franchise_id,
                    "franchise_id": franchise_id,
                    "league_id": league_id,
                    "status": player.get("status", ""),
                    "salary": player.get("salary", 0)
                })
    
    return normalized_rosters


def extract_mfl_managers(league_data: Dict) -> List[Dict]:
    """
    Extract manager information from MFL league data.
    
    Args:
        league_data: League data containing franchise information
        
    Returns:
        List of manager dictionaries
    """
    managers = []
    
    franchises = league_data.get("franchises", {}).get("franchise", [])
    if not isinstance(franchises, list):
        franchises = [franchises]
    
    for franchise in franchises:
        owner = franchise.get("owner", {})
        
        # Handle case where owner might be a string (just name) or dict
        if isinstance(owner, str):
            owner_name = owner
            owner_email = ""
        else:
            owner_name = owner.get("name", "Unknown")
            owner_email = owner.get("email", "")
        
        managers.append({
            "franchise_id": franchise.get("id"),
            "team_name": franchise.get("name"),
            "owner_name": owner_name,
            "owner_email": owner_email,
            "division": franchise.get("division", ""),
            "league_id": league_data.get("id")
        })
    
    return managers


async def get_mfl_league_with_rosters(league_id: str, year: str = None) -> Dict:
    """
    Get complete MFL league data including all rosters and standings.
    
    Args:
        league_id: MFL league ID
        year: Season year
        
    Returns:
        Complete league data with rosters and standings
    """
    data = await mfl_client.get_league_by_id(league_id, year)
    
    # Normalize the data
    normalized_league = normalize_mfl_league_data(data, year)
    normalized_rosters = normalize_mfl_roster_data(data["rosters"], league_id)
    managers = extract_mfl_managers(data["league"])
    
    return {
        "league": normalized_league,
        "rosters": normalized_rosters,
        "managers": managers,
        "raw_data": data  # Keep raw data for debugging
    }