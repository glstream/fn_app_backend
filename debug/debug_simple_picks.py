#!/usr/bin/env python3
"""
Temporary script to create simple draft picks for SveetVilliam to test
"""

# The issue is clearly in the complex draft positions logic
# Let's create a simple test to bypass all the complex SQL and just insert 2 picks manually

# SveetVilliam should have:
# - 2x 2026 2nd round picks  
# - Each worth ~5,000-10,000 points
# - Total: ~10,000-20,000 points

print("Simple test approach:")
print("1. Bypass complex draft positions logic")  
print("2. Create exactly 2 draft picks for SveetVilliam")
print("3. Name them '2026 Mid 2nd' (not '2026 Mid 1st')")
print("4. Ensure they only appear once in the database")
print("5. Total should be ~20,000 instead of 776,960")

# The 392 duplicates suggest the draft positions table has 392 entries
# and each one is matching with the same 2 picks, creating 392 x 2 = 784 total entries
# But since they all have the same name "2026 Mid 1st", they get summed together

print("\nThe real issue:")
print("- Draft positions table: ~392 entries per team")
print("- Draft picks table: 2 entries for SveetVilliam") 
print("- SQL join: 2 picks × 392 positions = 784 duplicates")
print("- Since all named '2026 Mid 1st', they sum to 784 × 1,977 = massive value")