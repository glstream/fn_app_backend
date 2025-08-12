#!/usr/bin/env python3
"""
Test Fleaflicker API directly to see what picks it returns
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fleaflicker_client import fleaflicker_client

async def test_fleaflicker_picks():
    league_id = '349505'
    
    print('=== FETCHING TEAM PICKS FOR SVEETVILLIAM (Team 1798697) ===')
    picks_1798697 = await fleaflicker_client.fetch_team_picks(league_id, '1798697')
    
    if picks_1798697:
        future_picks = []
        for pick in picks_1798697:
            season = pick.get('season')
            slot = pick.get('slot', {})
            round_num = slot.get('round')
            owned_by = pick.get('ownedBy', {})
            owner_id = owned_by.get('id')
            original = pick.get('original', {})
            original_id = original.get('id') if original else None
            
            if season and season >= 2026 and round_num and round_num <= 4:
                future_picks.append({
                    'season': season,
                    'round': round_num,
                    'owner': owner_id,
                    'original': original_id,
                    'is_traded': owner_id != original_id if original_id else False
                })
        
        print(f'Found {len(future_picks)} future picks for SveetVilliam:')
        for pick in sorted(future_picks, key=lambda x: (x['season'], x['round'])):
            traded = " (TRADED FROM team " + str(pick['original']) + ")" if pick['is_traded'] else ""
            print(f"  {pick['season']} Round {pick['round']}{traded}")
    
    print('\n=== FETCHING TEAM PICKS FOR KNUTE-ROCKNE-ND (Team 1798858) ===')
    picks_1798858 = await fleaflicker_client.fetch_team_picks(league_id, '1798858')
    
    if picks_1798858:
        future_picks = []
        for pick in picks_1798858:
            season = pick.get('season')
            slot = pick.get('slot', {})
            round_num = slot.get('round')
            owned_by = pick.get('ownedBy', {})
            owner_id = owned_by.get('id')
            original = pick.get('original', {})
            original_id = original.get('id') if original else None
            
            if season and season >= 2026 and round_num and round_num <= 4:
                future_picks.append({
                    'season': season,
                    'round': round_num,
                    'owner': owner_id,
                    'original': original_id,
                    'is_traded': owner_id != original_id if original_id else False
                })
        
        print(f'Found {len(future_picks)} future picks for KNUTE-ROCKNE-ND:')
        for pick in sorted(future_picks, key=lambda x: (x['season'], x['round'])):
            traded = " (TRADED FROM team " + str(pick['original']) + ")" if pick['is_traded'] else ""
            print(f"  {pick['season']} Round {pick['round']}{traded}")

if __name__ == "__main__":
    asyncio.run(test_fleaflicker_picks())