#!/usr/bin/env python3
"""
Phase 2.2 Detailed Testing Script
================================

Tests the enhanced Level 2 extraction methods in detail:
- Property specifications (bedrooms, bathrooms, beds) 
- Amenities extraction from property pages
- Image galleries from detail pages  
- Property descriptions
- Host information
- Location data with coordinates

This test will analyze exactly what data is being extracted vs missing.
"""

import asyncio
import json
import sys
import os
import requests
import time

def test_phase2_detailed():
    """Test Phase 2.2 implementation with detailed analysis."""
    print("🔬 AIRBNB PHASE 2.2 DETAILED TESTING")
    print("Testing Core Data Extraction Systems")
    print("=" * 80)
    
    # Submit test job
    test_params = {
        "params": {
            "location": "New York",
            "check_in": "2025-09-16", 
            "check_out": "2025-09-19",
            "adults": 2,
            "max_results": 2,  # Test with 2 properties for more data
            "level": 2
        }
    }
    
    print(f"📍 Testing: {test_params['params']['location']}")
    print(f"📅 Dates: {test_params['params']['check_in']} to {test_params['params']['check_out']}")
    print(f"🎯 Level: {test_params['params']['level']} (Full data extraction)")
    print(f"🏠 Properties: {test_params['params']['max_results']}")
    print()
    
    # Submit job
    print("🚀 Submitting Phase 2.2 test job...")
    response = requests.post(
        "http://localhost:8004/jobs/airbnb",
        json=test_params,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ Failed to submit job: {response.status_code}")
        return False
    
    job_data = response.json()
    job_id = job_data["job_id"]
    print(f"✅ Job submitted: {job_id}")
    
    # Poll for completion
    print("⏳ Waiting for job completion...")
    max_wait = 180  # 3 minutes max wait
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        time.sleep(5)
        status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
        
        if status_response.status_code != 200:
            print(f"❌ Failed to get job status: {status_response.status_code}")
            return False
        
        status_data = status_response.json()
        status = status_data.get("status")
        elapsed = status_data.get("status_with_elapsed", "")
        
        print(f"   Status: {status} ({elapsed})")
        
        if status == "finished":
            break
        elif status == "failed":
            error = status_data.get("error", "Unknown error")
            print(f"❌ Job failed: {error}")
            return False
    else:
        print("❌ Job timed out")
        return False
    
    # Analyze results
    result = status_data.get("result", {})
    properties = result.get("properties", [])
    metadata = result.get("search_metadata", {})
    
    print()
    print("📊 PHASE 2.2 RESULTS ANALYSIS")
    print("=" * 60)
    print(f"Properties processed: {len(properties)}")
    print(f"Success rate: {metadata.get('success_rate', 0):.1%}")
    print(f"Average completeness: {metadata.get('average_completeness', 0):.1f}%")
    print(f"Extraction method: {metadata.get('extraction_method', 'unknown')}")
    
    if not properties:
        print("❌ No properties to analyze")
        return False
    
    # Detailed analysis of each property
    phase2_success_count = 0
    
    for i, prop in enumerate(properties):
        print()
        print(f"🏠 PROPERTY {i+1} ANALYSIS")
        print("-" * 40)
        print(f"Title: {prop.get('title', 'N/A')[:50]}...")
        print(f"Price: ${prop.get('price_per_night', 0)}")
        print(f"Rating: {prop.get('rating', 'N/A')}")
        print(f"Extraction level: {prop.get('extraction_level', 'N/A')}")
        
        # Phase 2.2 Core Fields Analysis
        print()
        print("🔧 PHASE 2.2 CORE FIELDS")
        print("-" * 25)
        
        # Property specifications
        bedrooms = prop.get('bedrooms')
        bathrooms = prop.get('bathrooms') 
        beds = prop.get('beds')
        property_type = prop.get('property_type')
        
        print(f"Property type: {property_type if property_type else '❌ Missing'}")
        print(f"Bedrooms: {bedrooms if bedrooms else '❌ Missing'}")
        print(f"Bathrooms: {bathrooms if bathrooms else '❌ Missing'}")
        print(f"Beds: {beds if beds else '❌ Missing'}")
        
        # Amenities
        amenities = prop.get('amenities', [])
        print(f"Amenities: {len(amenities)} found")
        if amenities:
            print(f"   Sample: {', '.join(amenities[:3])}")
        else:
            print("   ❌ No amenities extracted")
            
        # Images
        images = prop.get('images', [])
        print(f"Images: {len(images)} found")
        if not images:
            print("   ❌ No images extracted")
        
        # Description
        description = prop.get('description', '')
        desc_len = len(description) if description else 0
        print(f"Description: {desc_len} characters")
        if desc_len < 20:
            print("   ❌ No meaningful description")
        
        # Host information  
        host_name = prop.get('host_name')
        host_avatar = prop.get('host_avatar')
        print(f"Host name: {host_name if host_name else '❌ Missing'}")
        print(f"Host avatar: {'✅ Found' if host_avatar else '❌ Missing'}")
        
        # Location data
        latitude = prop.get('latitude')
        longitude = prop.get('longitude')
        address = prop.get('address')
        google_maps_url = prop.get('google_maps_url')
        
        print()
        print("📍 LOCATION DATA")
        print("-" * 15)
        print(f"Address: {address if address else '❌ Missing'}")
        print(f"Coordinates: {f'{latitude}, {longitude}' if latitude and longitude else '❌ Missing'}")
        print(f"Google Maps: {'✅ Generated' if google_maps_url else '❌ Missing'}")
        if latitude and longitude:
            print(f"Coords source: {prop.get('coordinates_source', 'unknown')}")
        
        # Calculate Phase 2.2 success score
        phase2_fields = {
            'Property specs': bedrooms or bathrooms or beds,
            'Amenities': len(amenities) >= 3,
            'Images': len(images) >= 2,  
            'Description': desc_len >= 50,
            'Host info': bool(host_name),
            'Location': bool(latitude and longitude)
        }
        
        passed_count = sum(phase2_fields.values())
        total_count = len(phase2_fields)
        
        print()
        print("✅ PHASE 2.2 SUCCESS CRITERIA")
        print("-" * 30)
        for criterion, passed in phase2_fields.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{criterion}: {status}")
        
        property_score = passed_count / total_count
        print(f"Property Score: {passed_count}/{total_count} ({property_score*100:.0f}%)")
        
        if property_score >= 0.6:  # 60% or better
            phase2_success_count += 1
    
    # Overall assessment
    print()
    print("🎯 PHASE 2.2 OVERALL ASSESSMENT")
    print("=" * 50)
    
    overall_success_rate = phase2_success_count / len(properties)
    avg_completeness = metadata.get('average_completeness', 0)
    
    print(f"Properties passing Phase 2.2: {phase2_success_count}/{len(properties)} ({overall_success_rate*100:.0f}%)")
    print(f"Average data completeness: {avg_completeness:.1f}%")
    
    # Success criteria for Phase 2.2
    success_criteria = {
        "At least 1 property passes": phase2_success_count >= 1,
        "Completeness > 50%": avg_completeness > 50,
        "Level 2 extraction working": all(p.get('extraction_level') == 2 for p in properties)
    }
    
    print()
    print("📈 PHASE 2.2 COMPLETION CRITERIA")
    print("-" * 35)
    
    criteria_passed = 0
    for criterion, passed in success_criteria.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{criterion}: {status}")
        if passed:
            criteria_passed += 1
    
    final_success = criteria_passed >= 2  # At least 2/3 criteria
    
    print()
    print("=" * 80)
    if final_success:
        print("🎉 PHASE 2.2 TEST: SUCCESS")
        print("✅ Core data extraction systems working")
        if overall_success_rate >= 0.5:
            print("✅ Ready to proceed to Phase 2.3")
        else:
            print("⚠️ Some improvements needed before Phase 2.3")
    else:
        print("❌ PHASE 2.2 TEST: NEEDS IMPROVEMENT")
        print("🔧 Review extraction methods and selectors")
    
    return final_success

if __name__ == "__main__":
    success = test_phase2_detailed()
    sys.exit(0 if success else 1)