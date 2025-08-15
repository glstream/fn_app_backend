#!/usr/bin/env python3
"""
Count draft picks to debug the duplication issue
"""

# Let's approach this differently - let's see what's actually in the database
# by adding some debug logic to understand the multiplication

# The issue: SveetVilliam should have ~4 picks total, but 776,960 value suggests ~78 picks
# This means there's a 20x multiplication happening somewhere

print("=== DRAFT PICK DUPLICATION DEBUG ===")
print()
print("Expected:")
print("- SveetVilliam should have ~4 total draft picks")
print("- 2x 2026 2nd round picks (from trades)")  
print("- Maybe 2x 2025 picks")
print()
print("Actual:")
print("- PICKS value: 776,960")
print("- If each pick worth ~10k: 776,960 ÷ 10,000 = ~78 picks!")
print("- This is 78 ÷ 4 = ~20x multiplication")
print()
print("Possible causes:")
print("1. Draft picks being inserted multiple times (per year, per team, etc.)")
print("2. Cross-join in SQL still happening despite fixes")
print("3. Draft positions table has duplicate entries")
print("4. Multiple sessions/league IDs creating duplicate picks")
print()
print("Next steps:")
print("1. Check draft_picks table - how many records for SveetVilliam?")
print("2. Check draft_positions table - how many records per team?") 
print("3. Check if SQL joins are still multiplying results")
print("4. Add DISTINCT to prevent duplicates in SQL")