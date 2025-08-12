#!/usr/bin/env python3
"""
Test the fixed traded picks extraction
"""

import asyncio
import asyncpg
from fleaflicker_utils import extract_all_fleaflicker_draft_picks, insert_fleaflicker_draft_picks_data

async def test_fixed_extraction():
    league_id = '349505'
    session_id = 'test-traded-picks'
    
    # Extract picks with the fixed logic
    print('=== EXTRACTING DRAFT PICKS WITH FIXED LOGIC ===')
    all_picks = await extract_all_fleaflicker_draft_picks(league_id)
    
    print(f'\nTotal picks extracted: {len(all_picks)}')
    
    # Check for SveetVilliam's 2026 2nd round picks
    sveet_picks = [p for p in all_picks if p['owner_id'] == '1798697' and p['year'] == '2026' and p['round'] == '2']
    print(f'\nSveetVilliam 2026 2nd round picks: {len(sveet_picks)}')
    for pick in sveet_picks:
        print(f'  Owner: {pick["owner_id"]}, Original: {pick["roster_id"]}')
    
    # Check for KNUTE-ROCKNE-ND's missing pick
    knute_picks = [p for p in all_picks if p['owner_id'] == '1798858' and p['year'] == '2026' and p['round'] == '2']
    print(f'\nKNUTE-ROCKNE-ND 2026 2nd round picks: {len(knute_picks)} (should be 0)')
    
    # Insert into database
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    # First delete existing test picks
    await conn.execute('''
        DELETE FROM dynastr.draft_picks 
        WHERE league_id = $1 AND session_id = $2
    ''', league_id, session_id)
    
    # Insert new picks
    await insert_fleaflicker_draft_picks_data(conn, session_id, league_id, all_picks)
    
    # Verify in database
    print('\n=== VERIFYING IN DATABASE ===')
    result = await conn.fetch('''
        SELECT owner_id, roster_id, year, round_name, COUNT(*) as count
        FROM dynastr.draft_picks
        WHERE league_id = $1 AND session_id = $2 AND year = '2026' AND round = '2'
        GROUP BY owner_id, roster_id, year, round_name
        ORDER BY owner_id
    ''', league_id, session_id)
    
    for row in result:
        print(f'Owner {row["owner_id"]} has {row["count"]} x {row["year"]} {row["round_name"]} (originally from {row["roster_id"]})')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_fixed_extraction())