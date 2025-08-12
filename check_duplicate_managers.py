#!/usr/bin/env python3
"""
Check for duplicate manager records that might cause pick duplication
"""

import asyncio
import asyncpg

async def check_managers():
    conn = await asyncpg.connect(
        host='dynasty.postgres.database.azure.com',
        database='postgres',
        user='dynasty1',
        password=r'jeKlim@361!9',
        port=5432,
        ssl='require'
    )
    
    print('=== CHECKING FOR DUPLICATE MANAGERS ===')
    result = await conn.fetch('''
        SELECT user_id, COUNT(*) as count
        FROM dynastr.managers 
        WHERE league_id = '349505'
        GROUP BY user_id
        ORDER BY count DESC, user_id
    ''')
    
    for row in result:
        print(f'User {row["user_id"]}: {row["count"]} manager records')
    
    print('\n=== SAMPLE MANAGER RECORDS ===')
    result = await conn.fetch('''
        SELECT * FROM dynastr.managers 
        WHERE league_id = '349505' AND user_id = '2239577'
    ''')
    
    for row in result:
        print(dict(row))
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_managers())