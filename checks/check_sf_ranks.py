#!/usr/bin/env python3
"""
Check for duplicate sf_player_ranks causing pick multiplication
"""

import asyncio
import asyncpg

async def check_sf_ranks():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== CHECKING SF_PLAYER_RANKS FOR PICKS ===')
    result = await conn.fetch('''
        SELECT player_full_name, rank_type, COUNT(*) as count
        FROM dynastr.sf_player_ranks 
        WHERE player_full_name LIKE '%Mid 1st%'
        GROUP BY player_full_name, rank_type
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    for row in result:
        print(f'{row["player_full_name"]} ({row["rank_type"]}): {row["count"]} records')
    
    print('\n=== SAMPLE SF_PLAYER_RANKS FOR 2026 Mid 1st ===')
    result = await conn.fetch('''
        SELECT * FROM dynastr.sf_player_ranks 
        WHERE player_full_name = '2026 Mid 1st'
        LIMIT 5
    ''')
    
    for row in result:
        print(dict(row))
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_sf_ranks())