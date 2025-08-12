#!/usr/bin/env python3
"""
Regenerate draft picks with new positional naming
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from fleaflicker_utils import extract_all_fleaflicker_draft_picks, insert_fleaflicker_draft_picks_data

load_dotenv()

async def regenerate_picks():
    host = os.getenv('host')
    dbname = os.getenv('dbname') 
    user = os.getenv('user')
    password = os.getenv('password')
    sslmode = os.getenv('sslmode')
    
    conn = await asyncpg.connect(
        host=host, database=dbname, user=user, password=password, ssl=sslmode
    )
    
    try:
        print('=== REGENERATING DRAFT PICKS WITH POSITIONAL NAMING ===')
        
        session_id = "f2a27713-e95e-46e5-a27c-02bbcb0a2d09"
        league_id = "349505"
        
        # 1. Clear existing picks for this league
        print('\n1. Clearing existing draft picks...')
        await conn.execute('''
            DELETE FROM dynastr.draft_picks 
            WHERE league_id = $1 AND session_id = $2
        ''', league_id, session_id)
        print('   Cleared existing picks')
        
        # 2. Fetch picks with new positional naming
        print('\n2. Fetching picks with new positional naming...')
        draft_picks_data = await extract_all_fleaflicker_draft_picks(league_id)
        print(f'   Extracted {len(draft_picks_data)} draft picks')
        
        # 3. Insert the new picks
        print('\n3. Inserting new draft picks...')
        await insert_fleaflicker_draft_picks_data(conn, session_id, league_id, draft_picks_data)
        
        # 4. Verify SveetVilliam's picks
        print('\n4. Checking SveetVilliam\'s 2026 2nd picks...')
        picks_query = '''
        SELECT 
            dp.year,
            dp.round_name,
            dp.owner_id,
            dp.roster_id,
            sfr.superflex_sf_value as value_superflex,
            sfr.superflex_one_qb_value as value_one_qb
        FROM dynastr.draft_picks dp
        LEFT JOIN dynastr.sf_player_ranks sfr ON (dp.year || ' ' || dp.round_name) = sfr.player_full_name
        WHERE dp.league_id = $1 
        AND dp.year = '2026' 
        AND dp.round = '2'
        AND dp.owner_id = '1798697'
        AND sfr.rank_type = 'dynasty'
        ORDER BY dp.round_name
        '''
        
        picks = await conn.fetch(picks_query, league_id)
        print(f'   SveetVilliam has {len(picks)} 2026 2nd round picks:')
        
        for pick in picks:
            print(f'     {pick["year"]} {pick["round_name"]}')
            print(f'       SuperFlex Value: {pick["value_superflex"]}')
            print(f'       Owner: {pick["owner_id"]}, Original: {pick["roster_id"]}')
            print()
            
        # 5. Final verification
        if len(picks) == 2:
            values = [p["value_superflex"] or p["value_one_qb"] for p in picks]
            if len(set(values)) == 2:  # Check if values are different
                print('   ✅ SUCCESS: SveetVilliam has 2 different valued picks!')
                print(f'      Values: {values}')
            else:
                print('   ❌ ERROR: Both picks have the same value')
                print(f'      Values: {values}')
        else:
            print(f'   ❌ ERROR: Found {len(picks)} picks instead of 2')
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(regenerate_picks())