#!/usr/bin/env python3
"""
Airbnb Phase 2 Testing Script
============================

Tests Phase 2.1: Property Page Navigation & Core Setup
- Fixed _apply_search_filters method
- Enhanced Level 2 property detail extraction
- Location data with coordinates
- Amenities and images extraction
"""

import asyncio
import json
import sys
import os
from datetime import datetime, timedelta

# Add the parent directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from browser.main import create_browser
from browser.src.tasks.airbnb import AirbnbTask

async def test_phase_2_single_property():
    """Test Phase 2.1: Single property Level 2 extraction."""
    print("🚀 AIRBNB PHASE 2 TESTING - Single Property Level 2")
    print("=" * 60)
    
    browser_context = await create_browser()
    
    try:
        # Test parameters - New York single property
        test_params = {
            "location": "New York", 
            "check_in": "2025-09-15",
            "check_out": "2025-09-18",
            "adults": 2,
            "max_results": 1,
            "level": 2,  # Level 2: Full data extraction
            "scrape_level": 2
        }
        
        print(f"📍 Location: {test_params['location']}")
        print(f"📅 Dates: {test_params['check_in']} to {test_params['check_out']}")
        print(f"🎯 Level: {test_params['level']} (Full data extraction)")
        print(f"🏠 Max results: {test_params['max_results']}")
        print()
        
        # Create mock logger
        import logging
        logger = logging.getLogger('test')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Execute Level 2 extraction
        print("🔄 Starting Level 2 extraction...")
        result = await AirbnbTask.run(test_params, logger, browser_context.browser)
        
        # Analyze results
        properties = result.get('properties', [])
        metadata = result.get('search_metadata', {})
        
        print()
        print("📊 RESULTS ANALYSIS")
        print("-" * 40)
        print(f"Properties found: {len(properties)}")
        print(f"Success rate: {metadata.get('success_rate', 0):.1%}")
        print(f"Average completeness: {metadata.get('average_completeness', 0):.1f}%")
        print(f"Extraction method: {metadata.get('extraction_method', 'unknown')}")
        
        if properties:
            prop = properties[0]
            print()
            print("🏠 PROPERTY DETAILS")
            print("-" * 40)
            print(f"Title: {prop.get('title', 'N/A')}")
            print(f"Price per night: ${prop.get('price_per_night', 0)}")
            print(f"Rating: {prop.get('rating', 'N/A')}")
            print(f"Property type: {prop.get('property_type', 'N/A')}")
            print(f"Extraction level: {prop.get('extraction_level', 'N/A')}")
            
            # Phase 2 enhanced fields
            print()
            print("🔧 PHASE 2 ENHANCEMENTS")
            print("-" * 40)
            print(f"Host name: {prop.get('host_name', 'N/A')}")
            print(f"Amenities count: {len(prop.get('amenities', []))}")
            print(f"Images count: {len(prop.get('images', []))}")
            print(f"Bedrooms: {prop.get('bedrooms', 'N/A')}")
            print(f"Bathrooms: {prop.get('bathrooms', 'N/A')}")
            print(f"Description length: {len(prop.get('description', '')) if prop.get('description') else 0} chars")
            
            # Location data
            print()
            print("📍 LOCATION DATA")
            print("-" * 40)
            print(f"Address: {prop.get('address', 'N/A')}")
            print(f"Coordinates: {prop.get('latitude', 'N/A')}, {prop.get('longitude', 'N/A')}")
            print(f"Google Maps URL: {prop.get('google_maps_url', 'N/A')}")
            print(f"Coordinates source: {prop.get('coordinates_source', 'N/A')}")
            
            # Data completeness analysis
            essential_fields = ['title', 'airbnb_url', 'price_per_night']
            phase2_fields = ['host_name', 'amenities', 'images', 'description']
            location_fields = ['latitude', 'longitude', 'address']
            
            essential_complete = sum(1 for field in essential_fields if prop.get(field))
            phase2_complete = sum(1 for field in phase2_fields if prop.get(field))
            location_complete = sum(1 for field in location_fields if prop.get(field))
            
            print()
            print("✅ COMPLETENESS BREAKDOWN")
            print("-" * 40)
            print(f"Essential fields: {essential_complete}/{len(essential_fields)} ({essential_complete/len(essential_fields)*100:.0f}%)")
            print(f"Phase 2 fields: {phase2_complete}/{len(phase2_fields)} ({phase2_complete/len(phase2_fields)*100:.0f}%)")
            print(f"Location fields: {location_complete}/{len(location_fields)} ({location_complete/len(location_fields)*100:.0f}%)")
            
            # Success criteria check
            print()
            print("🎯 SUCCESS CRITERIA CHECK")
            print("-" * 40)
            success_criteria = {
                "Property details extracted": prop.get('extraction_level', 0) >= 2,
                "Price found": prop.get('price_per_night', 0) > 0,
                "Host information": bool(prop.get('host_name')),
                "Amenities found": len(prop.get('amenities', [])) >= 3,
                "Images found": len(prop.get('images', [])) >= 2,
                "Location data": bool(prop.get('latitude') and prop.get('longitude')),
                "Description extracted": len(prop.get('description', '')) > 50
            }
            
            passed = 0
            for criterion, result in success_criteria.items():
                status = "✅ PASS" if result else "❌ FAIL"
                print(f"{criterion}: {status}")
                if result:
                    passed += 1
            
            print()
            print(f"📈 OVERALL SCORE: {passed}/{len(success_criteria)} ({passed/len(success_criteria)*100:.0f}%)")
            
            # Sample data
            if prop.get('amenities'):
                print()
                print(f"🏠 SAMPLE AMENITIES ({len(prop['amenities'])} total):")
                for i, amenity in enumerate(prop['amenities'][:5]):
                    print(f"  {i+1}. {amenity}")
            
            return passed / len(success_criteria) >= 0.7  # 70% pass rate
            
        else:
            print("❌ No properties found")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await browser_context.close()

async def main():
    """Run Phase 2 test."""
    print("🧪 AIRBNB PHASE 2.1 VALIDATION TEST")
    print("Testing Property Page Navigation & Core Setup")
    print("=" * 80)
    print()
    
    success = await test_phase_2_single_property()
    
    print()
    print("=" * 80)
    if success:
        print("🎉 PHASE 2.1 TEST: SUCCESS")
        print("✅ Property page navigation working")
        print("✅ Level 2 enhancement functional")
        print("✅ Ready to proceed to Phase 2.2")
    else:
        print("❌ PHASE 2.1 TEST: FAILED")
        print("⚠️ Issues found in Phase 2.1 implementation")
        print("🔧 Review and fix before proceeding")
    
    return success

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)