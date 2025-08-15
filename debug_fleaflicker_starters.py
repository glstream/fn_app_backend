#!/usr/bin/env python3
"""
Debug script to check Fleaflicker league starter counts in current_leagues table.
"""
import asyncio
import asyncpg
import os
from pathlib import Path

async def debug_fleaflicker_leagues():
    """Check Fleaflicker league configurations in current_leagues table."""
    
    # Database connection
    try:
        db_url = os.getenv('DATABASE_URL') or os.getenv('POSTGRES_CONNECTION_STRING')
        if not db_url:
            print("ERROR: No database URL found. Please set DATABASE_URL or POSTGRES_CONNECTION_STRING environment variable.")
            return
            
        conn = await asyncpg.connect(db_url)
        
        # Query current_leagues for Fleaflicker leagues
        query = """
            SELECT 
                session_id,
                league_id,
                league_name,
                platform,
                qb_cnt,
                rb_cnt, 
                wr_cnt,
                te_cnt,
                flex_cnt,
                sf_cnt,
                rf_cnt,
                starter_cnt,
                total_roster_cnt
            FROM dynastr.current_leagues 
            WHERE platform = 'fleaflicker'
            ORDER BY session_id, league_id
        """
        
        results = await conn.fetch(query)
        
        if not results:
            print("No Fleaflicker leagues found in current_leagues table.")
            return
            
        print(f"Found {len(results)} Fleaflicker leagues:")
        print("-" * 120)
        print(f"{'Session ID':<12} {'League ID':<10} {'League Name':<25} {'QB':<3} {'RB':<3} {'WR':<3} {'TE':<3} {'FLEX':<4} {'SF':<3} {'RF':<3} {'Starters':<8} {'Total':<5}")
        print("-" * 120)
        
        for row in results:
            print(f"{row['session_id'][:12]:<12} {row['league_id']:<10} {row['league_name'][:25]:<25} "
                  f"{row['qb_cnt']:<3} {row['rb_cnt']:<3} {row['wr_cnt']:<3} {row['te_cnt']:<3} "
                  f"{row['flex_cnt']:<4} {row['sf_cnt']:<3} {row['rf_cnt']:<3} {row['starter_cnt']:<8} {row['total_roster_cnt']:<5}")
        
        # Check if there are any league_players for these leagues
        print("\n" + "=" * 120)
        print("Checking league_players data for these leagues:")
        
        for row in results:
            session_id = row['session_id']
            league_id = row['league_id']
            
            player_count_query = """
                SELECT COUNT(*) as player_count, COUNT(DISTINCT user_id) as unique_users
                FROM dynastr.league_players 
                WHERE session_id = $1 AND league_id = $2
            """
            
            player_results = await conn.fetchrow(player_count_query, session_id, league_id)
            print(f"League {league_id}: {player_results['player_count']} players, {player_results['unique_users']} unique users")
        
        await conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(debug_fleaflicker_leagues())