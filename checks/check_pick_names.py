#!/usr/bin/env python3
"""
Check what draft pick names exist in sf_player_ranks vs what we're generating
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def check_pick_names():
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
        print("=== CHECKING DRAFT PICK NAME MISMATCH ===")
        
        # 1. Check what pick names exist in sf_player_ranks
        print("\n1. Draft pick names in sf_player_ranks (2026 2nd round):")
        sf_picks = await conn.fetch("""
        SELECT DISTINCT player_full_name, superflex_one_qb_value
        FROM dynastr.sf_player_ranks 
        WHERE player_full_name LIKE '%2026%'
        AND player_full_name LIKE '%2nd%'
        AND rank_type = 'dynasty'
        ORDER BY player_full_name
        """)
        
        if sf_picks:
            for pick in sf_picks:
                print(f"   '{pick['player_full_name']}' -> {pick['superflex_one_qb_value']}")
        else:
            print("   ❌ No 2026 2nd round picks found in sf_player_ranks!")
            
        # 2. Check what we generated in draft_picks table  
        print("\n2. Draft pick names we generated (2026 2nd round):")
        dp_picks = await conn.fetch("""
        SELECT DISTINCT 
            dp.year || ' ' || dp.round_name as generated_name,
            dp.owner_id,
            dp.roster_id
        FROM dynastr.draft_picks dp
        WHERE dp.league_id = '349505'
        AND dp.year = '2026'
        AND dp.round = '2'
        ORDER BY generated_name
        """)
        
        if dp_picks:
            for pick in dp_picks:
                print(f"   '{pick['generated_name']}' - Owner: {pick['owner_id']}, Original: {pick['roster_id']}")
        else:
            print("   ❌ No 2026 2nd round picks found in draft_picks!")
            
        # 3. Check which ones match
        print("\n3. Checking for matches:")
        sf_names = {pick['player_full_name'] for pick in sf_picks}
        dp_names = {pick['generated_name'] for pick in dp_picks}
        
        matches = sf_names.intersection(dp_names)
        missing = dp_names - sf_names
        
        print(f"   ✅ Matches found: {len(matches)}")
        for name in matches:
            print(f"      '{name}'")
            
        print(f"   ❌ Missing from sf_player_ranks: {len(missing)}")
        for name in missing:
            print(f"      '{name}'")
            
        # 4. Check what generic names exist
        print("\n4. Generic 2026 pick names in sf_player_ranks:")
        generic_picks = await conn.fetch("""
        SELECT DISTINCT player_full_name, superflex_one_qb_value
        FROM dynastr.sf_player_ranks 
        WHERE player_full_name LIKE '%2026%2nd%'
        AND player_full_name NOT LIKE '%Early%'
        AND player_full_name NOT LIKE '%Mid%' 
        AND player_full_name NOT LIKE '%Late%'
        AND rank_type = 'dynasty'
        ORDER BY superflex_one_qb_value DESC
        """)
        
        if generic_picks:
            for pick in generic_picks:
                print(f"   '{pick['player_full_name']}' -> {pick['superflex_one_qb_value']}")
        else:
            print("   No generic 2026 2nd picks found")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_pick_names())