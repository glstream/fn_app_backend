#!/usr/bin/env python3
"""
Debug script to check team name vs display name mapping
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def debug_team_mapping():
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
        print("=== TEAM MAPPING DEBUG ===")
        
        # Get all managers in the league
        query = """
        SELECT 
            m.user_id,
            m.display_name as db_display_name,
            m.league_id,
            m.session_id
        FROM dynastr.managers m
        WHERE m.league_id = '349505'
        AND m.session_id LIKE 'f2a27713%'
        ORDER BY m.user_id
        """
        
        managers = await conn.fetch(query)
        print(f"\n1. MANAGERS IN DATABASE ({len(managers)} total):")
        
        sveet_user_id = None
        for manager in managers:
            print(f"   UserID: {manager[0]}, Display: '{manager[1]}'")
            if 'sveet' in manager[1].lower():
                sveet_user_id = manager[0]
                print(f"   ^ FOUND SVEETVILLIAM: user_id = {sveet_user_id}")
        
        if sveet_user_id:
            # Check draft picks for SveetVilliam
            picks_query = """
            SELECT 
                dp.year,
                dp.round,
                COUNT(*) as count
            FROM dynastr.draft_picks dp
            WHERE dp.league_id = '349505'
            AND dp.owner_id = $1
            GROUP BY dp.year, dp.round
            ORDER BY dp.year, dp.round
            """
            
            picks = await conn.fetch(picks_query, sveet_user_id)
            print(f"\n2. SVEETVILLIAM'S PICKS (user_id: {sveet_user_id}):")
            total_picks = 0
            sveet_2026_2nds = 0
            for pick in picks:
                count = pick[2]
                total_picks += count
                if pick[0] == '2026' and pick[1] == '2':
                    sveet_2026_2nds = count
                print(f"   {pick[0]} Round {pick[1]}: {count} picks")
            
            print(f"\n   SUMMARY:")
            print(f"   - Total picks: {total_picks}")
            print(f"   - 2026 2nd round picks: {sveet_2026_2nds}")
            
            # Check if there are any roster_id mappings
            roster_query = """
            SELECT DISTINCT 
                dp.owner_id,
                dp.roster_id
            FROM dynastr.draft_picks dp
            WHERE dp.league_id = '349505'
            AND dp.owner_id = $1
            """
            rosters = await conn.fetch(roster_query, sveet_user_id)
            print(f"\n3. ROSTER MAPPINGS for SveetVilliam:")
            for roster in rosters:
                print(f"   owner_id: {roster[0]} -> roster_id: {roster[1]}")
                
        else:
            print("\n❌ SveetVilliam not found in database!")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_team_mapping())