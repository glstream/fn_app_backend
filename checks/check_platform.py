#!/usr/bin/env python3
"""
Check platform assignment for league 349505
"""

import asyncio
import asyncpg

async def check_platform():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== CURRENT_LEAGUES PLATFORM FOR 349505 ===')
    result = await conn.fetch('''
        SELECT session_id, league_id, platform, user_name, league_name
        FROM dynastr.current_leagues 
        WHERE league_id = '349505'
        ORDER BY session_id
    ''')
    
    for row in result:
        print(f'Session: {row["session_id"]}, Platform: {row["platform"]}, League: {row["league_name"]}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_platform())