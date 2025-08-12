#!/usr/bin/env python3
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def verify_picks():
    host = os.getenv('host')
    dbname = os.getenv('dbname') 
    user = os.getenv('user')
    password = os.getenv('password')
    sslmode = os.getenv('sslmode')
    
    conn = await asyncpg.connect(
        host=host, database=dbname, user=user, password=password, ssl=sslmode
    )
    
    try:
        print('=== FINAL VERIFICATION ===')
        
        # Get SveetVilliam's 2026 2nd round picks with values
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
        WHERE dp.league_id = '349505' 
        AND dp.year = '2026' 
        AND dp.round = '2'
        AND dp.owner_id = '1798697'
        AND sfr.rank_type = 'dynasty'
        ORDER BY dp.round_name
        '''
        
        picks = await conn.fetch(picks_query)
        print(f'SveetVilliam has {len(picks)} 2026 2nd round picks:')
        for pick in picks:
            print(f'  {pick["year"]} {pick["round_name"]}')
            print(f'    SuperFlex Value: {pick["value_superflex"]}')
            print(f'    One QB Value: {pick["value_one_qb"]}')
            print(f'    Owner: {pick["owner_id"]}, Original Team: {pick["roster_id"]}')
            print()
            
        # Verify these are different values
        if len(picks) == 2:
            val1 = picks[0]['value_superflex'] or picks[0]['value_one_qb'] 
            val2 = picks[1]['value_superflex'] or picks[1]['value_one_qb']
            if val1 != val2:
                print('✅ SUCCESS: Different values confirmed!')
                print(f'   Pick 1: {val1}')
                print(f'   Pick 2: {val2}')
            else:
                print('❌ ERROR: Both picks have the same value')
        else:
            print(f'❌ ERROR: Found {len(picks)} picks instead of 2')
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(verify_picks())