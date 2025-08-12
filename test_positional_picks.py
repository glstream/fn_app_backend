#!/usr/bin/env python3
"""
Test the new positional draft pick names
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from fleaflicker_utils import fetch_fleaflicker_future_draft_picks

load_dotenv()

async def test_positional_picks():
    # Use environment variables
    host = os.getenv("host")
    dbname = os.getenv("dbname") 
    user = os.getenv("user")
    password = os.getenv("password")
    sslmode = os.getenv("sslmode")
    
    conn = await asyncpg.connect(
        host=host,
        database=dbname,
        user=user,
        password=password,
        ssl=sslmode
    )
    
    try:
        print("=== TESTING POSITIONAL DRAFT PICKS ===")
        
        # Clear existing picks for this league
        await conn.execute("""
            DELETE FROM dynastr.draft_picks 
            WHERE league_id = '349505' 
            AND session_id LIKE 'f2a27713%'
        """)
        print("Cleared existing draft picks")
        
        # Fetch new picks with positional names
        print("\nFetching draft picks with new positional naming...")
        session_id = "f2a27713-e95e-46e5-a27c-02bbcb0a2d09"
        league_id = "349505"
        
        await fetch_fleaflicker_future_draft_picks(conn, session_id, league_id)
        
        # Check what picks were created for SveetVilliam (team 1798697)
        picks_query = """
        SELECT 
            dp.year,
            dp.round,
            dp.round_name,
            dp.owner_id,
            dp.roster_id
        FROM dynastr.draft_picks dp
        WHERE dp.league_id = '349505'
        AND dp.owner_id = '1798697'
        AND dp.year = '2026'
        AND dp.round = '2'
        ORDER BY dp.round_name
        """
        
        picks = await conn.fetch(picks_query)
        print(f"\nSveetVilliam's 2026 2nd round picks: {len(picks)}")
        for pick in picks:
            print(f"  {pick['year']} {pick['round_name']} - Owner: {pick['owner_id']}, Original: {pick['roster_id']}")
            
        # Check if these new names have values in sf_player_ranks
        if picks:
            for pick in picks:
                pick_name = f"{pick['year']} {pick['round_name']}"
                value_query = """
                SELECT player_full_name, superflex_one_qb_value
                FROM dynastr.sf_player_ranks 
                WHERE player_full_name = $1
                AND rank_type = 'dynasty'
                LIMIT 1
                """
                value_result = await conn.fetchrow(value_query, pick_name)
                if value_result:
                    print(f"    Value: {value_result['superflex_one_qb_value']}")
                else:
                    print(f"    ❌ No value found for '{pick_name}'")
                    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_positional_picks())