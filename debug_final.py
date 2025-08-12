#!/usr/bin/env python3
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def debug_picks():
    host = os.getenv('host')
    dbname = os.getenv('dbname') 
    user = os.getenv('user')
    password = os.getenv('password')
    sslmode = os.getenv('sslmode')
    
    conn = await asyncpg.connect(
        host=host, database=dbname, user=user, password=password, ssl=sslmode
    )
    
    try:
        print('=== DEBUG FINAL STATE ===')
        
        # First, check draft_picks table
        draft_query = '''
        SELECT 
            dp.year,
            dp.round_name,
            dp.owner_id,
            dp.roster_id,
            (dp.year || ' ' || dp.round_name) as full_name
        FROM dynastr.draft_picks dp
        WHERE dp.league_id = '349505' 
        AND dp.year = '2026' 
        AND dp.round = '2'
        AND dp.owner_id = '1798697'
        ORDER BY dp.round_name
        '''
        
        draft_picks = await conn.fetch(draft_query)
        print(f'Draft picks in database: {len(draft_picks)}')
        for pick in draft_picks:
            print(f'  {pick["full_name"]} - Owner: {pick["owner_id"]}, Original: {pick["roster_id"]}')
        
        # Then check sf_player_ranks for matching names
        if draft_picks:
            print('\nChecking sf_player_ranks for these names:')
            for pick in draft_picks:
                rank_query = '''
                SELECT 
                    player_full_name,
                    superflex_sf_value,
                    superflex_one_qb_value,
                    rank_type
                FROM dynastr.sf_player_ranks 
                WHERE player_full_name = $1
                AND rank_type = 'dynasty'
                '''
                ranks = await conn.fetch(rank_query, pick['full_name'])
                print(f'  {pick["full_name"]}:')
                if ranks:
                    for rank in ranks:
                        print(f'    SF: {rank["superflex_sf_value"]}, OneQB: {rank["superflex_one_qb_value"]}')
                else:
                    print('    ❌ No matching rank found')
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_picks())