#!/usr/bin/env python3
"""
Test Fleaflicker API directly with minimal imports
"""

import asyncio
import aiohttp
import json

async def test_picks():
    league_id = '349505'
    base_url = 'https://www.fleaflicker.com/api'
    
    async with aiohttp.ClientSession() as session:
        # Test SveetVilliam's picks
        print('=== FETCHING PICKS FOR SVEETVILLIAM (Team 1798697) ===')
        url = f'{base_url}/FetchTeamPicks?league_id={league_id}&team_id=1798697'
        
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                picks = data.get('picks', [])
                
                future_picks = []
                for pick in picks:
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
                            'is_traded': str(owner_id) != str(original_id) if original_id else False
                        })
                
                print(f'Found {len(future_picks)} future picks for SveetVilliam:')
                for pick in sorted(future_picks, key=lambda x: (x['season'], x['round'])):
                    traded = f" (TRADED FROM team {pick['original']})" if pick['is_traded'] else ""
                    print(f"  {pick['season']} Round {pick['round']}{traded}")
                
                # Check specifically for 2026 2nd round picks
                picks_2026_2nd = [p for p in future_picks if p['season'] == 2026 and p['round'] == 2]
                print(f'\nSpecifically for 2026 2nd round: {len(picks_2026_2nd)} picks')
                for pick in picks_2026_2nd:
                    print(f"  Owner: {pick['owner']}, Original: {pick['original']}")
        
        # Test KNUTE-ROCKNE-ND's picks
        print('\n=== FETCHING PICKS FOR KNUTE-ROCKNE-ND (Team 1798858) ===')
        url = f'{base_url}/FetchTeamPicks?league_id={league_id}&team_id=1798858'
        
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                picks = data.get('picks', [])
                
                future_picks = []
                for pick in picks:
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
                            'is_traded': str(owner_id) != str(original_id) if original_id else False
                        })
                
                print(f'Found {len(future_picks)} future picks for KNUTE-ROCKNE-ND:')
                for pick in sorted(future_picks, key=lambda x: (x['season'], x['round'])):
                    traded = f" (TRADED FROM team {pick['original']})" if pick['is_traded'] else ""
                    print(f"  {pick['season']} Round {pick['round']}{traded}")
                
                # Check specifically for 2026 2nd round picks
                picks_2026_2nd = [p for p in future_picks if p['season'] == 2026 and p['round'] == 2]
                print(f'\nSpecifically for 2026 2nd round: {len(picks_2026_2nd)} picks')
                for pick in picks_2026_2nd:
                    print(f"  Owner: {pick['owner']}, Original: {pick['original']}")

if __name__ == "__main__":
    asyncio.run(test_picks())