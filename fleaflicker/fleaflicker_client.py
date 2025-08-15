"""
Fleaflicker API Client for NFL Fantasy Football data ingestion.

This module provides a comprehensive client for interacting with the Fleaflicker API,
following the existing patterns and conventions used in utils.py for Sleeper API integration.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Union
import aiohttp
# from fastapi_cache.decorator import cache  # Disabled temporarily
from utils import get_http_session, CACHE_EXPIRATION, LEAGUE_CACHE_EXPIRATION, SHORT_CACHE_EXPIRATION


class FleaflickerClient:
    """
    Fleaflicker API client for NFL fantasy football data.
    
    Provides methods to fetch leagues, rosters, scoreboards, standings, 
    transactions, and player data from Fleaflicker platform.
    """
    
    BASE_URL = "https://www.fleaflicker.com/api"
    SPORT = "NFL"
    
    def __init__(self):
        self.session = None
    
    async def _make_api_call(
        self, 
        endpoint: str, 
        params: Dict[str, Union[str, int]] = None,
        timeout: int = 10,
        max_retries: int = 5
    ) -> Dict:
        """
        Make API call to Fleaflicker endpoint with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            
        Returns:
            API response data as dictionary
        """
        if params is None:
            params = {}
        
        # Always include sport parameter for NFL
        params["sport"] = self.SPORT
        
        url = f"{self.BASE_URL}/{endpoint}"
        session = await get_http_session()
        
        for retry in range(max_retries):
            try:
                async with session.get(
                    url, 
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    response.raise_for_status()
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                if retry < max_retries - 1:
                    sleep_time = 2 ** retry
                    print(f"Fleaflicker API error: {e}. Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    print(f"Fleaflicker API error: {e}. Max retries reached.")
                    raise
    
    # @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily
    async def get_user_leagues(self, user_id: str, season: str = None, email: str = None) -> List[Dict]:
        """
        Fetch user's leagues for a given season using FetchUserLeagues endpoint.
        
        Args:
            user_id: Fleaflicker user ID
            season: Season year (defaults to current)
            email: User's email address (optional)
            
        Returns:
            List of league dictionaries
        """
        params = {}
        if user_id:
            params["user_id"] = user_id
        if season:
            params["season"] = season
        if email:
            params["email"] = email
            
        # print removed for production - DEBUG: Fleaflicker API call params: {params}")
            
        try:
            response = await self._make_api_call("FetchUserLeagues", params)
            # print removed for production - DEBUG: Fleaflicker API raw response: {response}")
            return response.get("leagues", [])
        except Exception as e:
            print(f"Error fetching user leagues for user {user_id}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    # @cache(expire=CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_rules(self, league_id: str) -> Dict:
        """
        Fetch league scoring rules and roster configuration.
        
        Args:
            league_id: Fleaflicker league ID
            
        Returns:
            League rules dictionary
        """
        params = {"league_id": league_id}
        return await self._make_api_call("FetchLeagueRules", params)
    
    # @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_standings(self, league_id: str, season: str = None) -> Dict:
        """
        Fetch league standings and team records.
        
        Args:
            league_id: Fleaflicker league ID
            season: Season year
            
        Returns:
            League standings dictionary
        """
        params = {"league_id": league_id}
        if season:
            params["season"] = season
            
        return await self._make_api_call("FetchLeagueStandings", params)
    
    # @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_rosters(self, league_id: str, season: str = None, scoring_period: str = None) -> Dict:
        """
        Fetch all team rosters in a league.
        
        Args:
            league_id: Fleaflicker league ID
            season: Season year
            scoring_period: Week number
            
        Returns:
            League rosters dictionary
        """
        params = {"league_id": league_id}
        if season:
            params["season"] = season
        if scoring_period:
            params["scoring_period"] = scoring_period
            
        return await self._make_api_call("FetchLeagueRosters", params)
    
    async def fetch_roster(self, league_id: str, team_id: str, season: str = None, scoring_period: str = None) -> Dict:
        """
        Fetch detailed roster for a specific team.
        
        Args:
            league_id: Fleaflicker league ID
            team_id: Team ID
            season: Season year
            scoring_period: Week number
            
        Returns:
            Team roster dictionary
        """
        params = {
            "league_id": league_id,
            "team_id": team_id
        }
        if season:
            params["season"] = season
        if scoring_period:
            params["scoring_period"] = scoring_period
            
        return await self._make_api_call("FetchRoster", params)
    
    # @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_scoreboard(self, league_id: str, season: str = None, scoring_period: str = None) -> Dict:
        """
        Fetch league scoreboard for a specific week.
        
        Args:
            league_id: Fleaflicker league ID
            season: Season year
            scoring_period: Week number
            
        Returns:
            League scoreboard dictionary
        """
        params = {"league_id": league_id}
        if season:
            params["season"] = season
        if scoring_period:
            params["scoring_period"] = scoring_period
            
        return await self._make_api_call("FetchLeagueScoreboard", params)
    
    # @cache(expire=SHORT_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_boxscore(self, league_id: str, fantasy_game_id: str, scoring_period: str = None) -> Dict:
        """
        Fetch detailed boxscore for a specific fantasy game.
        
        Args:
            league_id: Fleaflicker league ID
            fantasy_game_id: Specific fantasy game ID
            scoring_period: Week number
            
        Returns:
            Boxscore dictionary with player-level scoring
        """
        params = {
            "league_id": league_id,
            "fantasy_game_id": fantasy_game_id
        }
        if scoring_period:
            params["scoring_period"] = scoring_period
            
        return await self._make_api_call("FetchLeagueBoxscore", params)
    
    # @cache(expire=SHORT_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_transactions(self, league_id: str, team_id: str = None, result_offset: int = 0) -> Dict:
        """
        Fetch league transaction history (trades, waivers, etc.).
        
        Args:
            league_id: Fleaflicker league ID
            team_id: Optional team ID to filter transactions
            result_offset: Pagination offset
            
        Returns:
            Transaction history dictionary
        """
        params = {
            "league_id": league_id,
            "result_offset": result_offset
        }
        if team_id:
            params["team_id"] = team_id
            
        return await self._make_api_call("FetchLeagueTransactions", params)
    
    # @cache(expire=SHORT_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_activity(self, league_id: str, result_offset: int = 0) -> Dict:
        """
        Fetch recent league activity feed.
        
        Args:
            league_id: Fleaflicker league ID
            result_offset: Pagination offset
            
        Returns:
            Activity feed dictionary
        """
        params = {
            "league_id": league_id,
            "result_offset": result_offset
        }
        
        return await self._make_api_call("FetchLeagueActivity", params)
    
    # @cache(expire=LEAGUE_CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_league_draft_board(self, league_id: str, season: str = None, draft_number: int = 1) -> Dict:
        """
        Fetch league draft board and results.
        
        Args:
            league_id: Fleaflicker league ID
            season: Season year
            draft_number: Draft number (for leagues with multiple drafts)
            
        Returns:
            Draft board dictionary
        """
        params = {
            "league_id": league_id,
            "draft_number": draft_number
        }
        if season:
            params["season"] = season
            
        return await self._make_api_call("FetchLeagueDraftBoard", params)
    
    # @cache(expire=CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_team_picks(self, league_id: str, team_id: str) -> Dict:
        """
        Fetch future draft picks for a specific team.
        
        Args:
            league_id: Fleaflicker league ID
            team_id: Team ID
            
        Returns:
            Team picks dictionary containing future draft picks
        """
        params = {
            "sport": "NFL",
            "league_id": league_id,
            "team_id": team_id
        }
        
        return await self._make_api_call("FetchTeamPicks", params)
    
    # @cache(expire=CACHE_EXPIRATION)  # Disabled temporarily
    async def fetch_player_listing(
        self, 
        filter_position: str = None,
        filter_team: str = None,
        filter_owned: bool = None,
        result_offset: int = 0
    ) -> Dict:
        """
        Fetch player listing with optional filters.
        
        Args:
            filter_position: Position filter (QB, RB, WR, TE, etc.)
            filter_team: Team filter (e.g., "SF", "DAL")
            filter_owned: Filter by ownership status
            result_offset: Pagination offset
            
        Returns:
            Player listing dictionary
        """
        params = {"result_offset": result_offset}
        
        if filter_position:
            params["filter.position"] = filter_position
        if filter_team:
            params["filter.team"] = filter_team
        if filter_owned is not None:
            params["filter.owned"] = str(filter_owned).lower()
            
        return await self._make_api_call("FetchPlayerListing", params)


# Global client instance
fleaflicker_client = FleaflickerClient()


# Utility functions following existing patterns in utils.py

async def get_fleaflicker_league_info(league_id: str) -> Dict:
    """
    Get comprehensive league information including rules and standings.
    
    Args:
        league_id: Fleaflicker league ID
        
    Returns:
        Combined league information dictionary
    """
    rules_task = fleaflicker_client.fetch_league_rules(league_id)
    standings_task = fleaflicker_client.fetch_league_standings(league_id)
    
    rules, standings = await asyncio.gather(rules_task, standings_task)
    
    return {
        "rules": rules,
        "standings": standings
    }


async def get_fleaflicker_league_rosters(league_id: str, season: str = None) -> List[Dict]:
    """
    Get all rosters in a league with team and player details.
    
    Args:
        league_id: Fleaflicker league ID
        season: Season year
        
    Returns:
        List of roster dictionaries
    """
    rosters_data = await fleaflicker_client.fetch_league_rosters(league_id, season)
    return rosters_data.get("teams", [])


async def get_fleaflicker_league_transactions(league_id: str, max_pages: int = 5) -> List[Dict]:
    """
    Get all recent transactions for a league with pagination.
    
    Args:
        league_id: Fleaflicker league ID
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of transaction dictionaries
    """
    all_transactions = []
    offset = 0
    
    for _ in range(max_pages):
        transactions_data = await fleaflicker_client.fetch_league_transactions(league_id, result_offset=offset)
        transactions = transactions_data.get("items", [])
        
        if not transactions:
            break
            
        all_transactions.extend(transactions)
        
        # Check if there are more pages
        if len(transactions) < 25:  # Assuming 25 is the default page size
            break
            
        offset += 25
    
    return all_transactions


async def get_fleaflicker_scoreboard_for_week(league_id: str, season: str, week: int) -> Dict:
    """
    Get scoreboard data for a specific week.
    
    Args:
        league_id: Fleaflicker league ID
        season: Season year
        week: Week number
        
    Returns:
        Scoreboard data dictionary
    """
    return await fleaflicker_client.fetch_league_scoreboard(
        league_id=league_id,
        season=season,
        scoring_period=str(week)
    )


def normalize_fleaflicker_league_data(league_data: Dict, user_id: str = None) -> Dict:
    """
    Normalize Fleaflicker league data to match internal format.
    
    Args:
        league_data: Raw league data from Fleaflicker
        user_id: Optional user ID
        
    Returns:
        Normalized league data dictionary
    """
    # This function will need to be implemented based on actual Fleaflicker response format
    # Placeholder implementation
    return {
        "league_id": league_data.get("id"),
        "league_name": league_data.get("name"),
        "total_rosters": league_data.get("size"),
        "sport": "nfl",
        "league_year": league_data.get("season"),
        "platform": "fleaflicker"
    }


def normalize_fleaflicker_roster_data(roster_data: Dict) -> List[Dict]:
    """
    Normalize Fleaflicker roster data to match internal format.
    
    Args:
        roster_data: Raw roster data from Fleaflicker
        
    Returns:
        Normalized roster data list
    """
    # This function will need to be implemented based on actual Fleaflicker response format
    # Placeholder implementation
    normalized_rosters = []
    
    for team in roster_data.get("teams", []):
        for player in team.get("players", []):
            normalized_rosters.append({
                "player_id": player.get("proPlayer", {}).get("id"),
                "team_id": team.get("id"),
                "owner_id": team.get("owners", [{}])[0].get("id") if team.get("owners") else None,
                "league_id": roster_data.get("league_id")
            })
    
    return normalized_rosters