#!/usr/bin/env python3
"""
Debug level routing to understand why levels aren't working correctly
"""

import asyncio
import sys
import os
sys.path.append('/app/src')

# Add debug logging to understand the flow
async def debug_level_routing():
    print("🔍 DEBUGGING LEVEL ROUTING IN booking_hotels.py")
    
    # Read the actual source code to understand the issue
    with open('/app/src/tasks/booking_hotels.py', 'r') as f:
        content = f.read()
    
    print("📍 Checking method existence...")
    
    methods_to_check = [
        'scrape_hotels_quick',
        'scrape_hotels_level_2', 
        'scrape_hotels_level_3',
        'scrape_hotels_level_4',
        '_scrape_property_cards_level_4'
    ]
    
    for method in methods_to_check:
        if f'def {method}' in content:
            print(f"   ✅ {method} exists")
        else:
            print(f"   ❌ {method} MISSING")
    
    print("\n📍 Checking routing logic...")
    
    # Extract the routing section
    lines = content.split('\n')
    routing_section = []
    in_routing = False
    
    for i, line in enumerate(lines):
        if 'if scrape_level >= 4:' in line:
            in_routing = True
        if in_routing:
            routing_section.append(f"{i+1:4d}: {line}")
        if in_routing and 'extraction_method = "level_1' in line:
            break
    
    print("   ROUTING LOGIC:")
    for line in routing_section[:20]:  # Show first 20 lines
        print(f"   {line}")
    
    print("\n📍 Checking class structure...")
    
    # Look for ModernBookingScraper class
    class_start = content.find('class ModernBookingScraper')
    if class_start != -1:
        print("   ✅ ModernBookingScraper class found")
        
        # Get class methods
        class_section = content[class_start:class_start+2000]
        method_lines = [line.strip() for line in class_section.split('\n') if 'async def' in line or 'def' in line]
        
        print("   📋 Available methods in ModernBookingScraper:")
        for method in method_lines[:10]:
            print(f"      {method}")
    else:
        print("   ❌ ModernBookingScraper class NOT FOUND")
    
    return True

if __name__ == "__main__":
    asyncio.run(debug_level_routing())