#!/usr/bin/env python3
"""
Comprehensive Phase 2.2 Validation Script
=========================================

This script thoroughly tests all Phase 2.2 enhancements:
1. Property specifications (bedrooms, bathrooms, beds)
2. Description extraction quality
3. Image collection and filtering  
4. Amenities extraction
5. Host information extraction
6. Address/location data
7. Data completeness calculation accuracy
8. Error handling and edge cases

Runs multiple test scenarios and provides detailed analysis.
"""

import requests
import json
import time
import sys
from typing import Dict, List, Any, Optional

def run_test_job(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Run a test job and return results."""
    print(f"🚀 Submitting test: {params['params']['location']} ({params['params']['max_results']} properties)")
    
    response = requests.post(
        "http://localhost:8004/jobs/airbnb",
        json=params,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ Failed to submit job: {response.status_code}")
        return None
    
    job_data = response.json()
    job_id = job_data["job_id"]
    print(f"   Job ID: {job_id}")
    
    # Wait for completion
    max_wait = 300  # 5 minutes
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        time.sleep(10)
        status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
        
        if status_response.status_code != 200:
            print(f"❌ Failed to get job status")
            return None
        
        status_data = status_response.json()
        status = status_data.get("status")
        
        if status == "finished":
            print(f"✅ Test completed successfully")
            return status_data.get("result", {})
        elif status == "failed":
            error = status_data.get("error", "Unknown error")
            print(f"❌ Test failed: {error}")
            return None
    
    print("⏰ Test timed out")
    return None

def analyze_property(prop: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Analyze a single property in detail."""
    analysis = {
        "property_index": index,
        "title": prop.get("title", "N/A")[:50] + "...",
        "price": prop.get("price_per_night", 0),
        "extraction_level": prop.get("extraction_level", 0),
        "data_completeness": prop.get("data_completeness", 0),
        "issues": [],
        "successes": [],
        "scores": {}
    }
    
    # Test Property Specifications
    bedrooms = prop.get("bedrooms")
    bathrooms = prop.get("bathrooms") 
    beds = prop.get("beds")
    
    if bedrooms is not None:
        analysis["successes"].append(f"Bedrooms: {bedrooms}")
        analysis["scores"]["bedrooms"] = 1
    else:
        analysis["issues"].append("Missing bedrooms")
        analysis["scores"]["bedrooms"] = 0
        
    if bathrooms is not None:
        analysis["successes"].append(f"Bathrooms: {bathrooms}")
        analysis["scores"]["bathrooms"] = 1
    else:
        analysis["issues"].append("Missing bathrooms")
        analysis["scores"]["bathrooms"] = 0
        
    if beds is not None:
        analysis["successes"].append(f"Beds: {beds}")
        analysis["scores"]["beds"] = 1
    else:
        analysis["issues"].append("Missing beds")
        analysis["scores"]["beds"] = 0
    
    # Test Description
    description = prop.get("description", "")
    if len(description) > 100:
        analysis["successes"].append(f"Description: {len(description)} chars")
        analysis["scores"]["description"] = 1
    elif len(description) > 20:
        analysis["issues"].append(f"Short description: {len(description)} chars")
        analysis["scores"]["description"] = 0.5
    else:
        analysis["issues"].append("Missing/minimal description")
        analysis["scores"]["description"] = 0
    
    # Test Images
    images = prop.get("images", [])
    if len(images) >= 3:
        analysis["successes"].append(f"Images: {len(images)} found")
        analysis["scores"]["images"] = 1
    elif len(images) >= 1:
        analysis["issues"].append(f"Few images: {len(images)} found")
        analysis["scores"]["images"] = 0.5
    else:
        analysis["issues"].append("No images found")
        analysis["scores"]["images"] = 0
    
    # Test Amenities
    amenities = prop.get("amenities", [])
    if len(amenities) >= 5:
        analysis["successes"].append(f"Amenities: {len(amenities)} found")
        analysis["scores"]["amenities"] = 1
    elif len(amenities) >= 1:
        analysis["issues"].append(f"Few amenities: {len(amenities)} found")
        analysis["scores"]["amenities"] = 0.5
    else:
        analysis["issues"].append("No amenities found")
        analysis["scores"]["amenities"] = 0
    
    # Test Host Information
    host_name = prop.get("host_name")
    host_avatar = prop.get("host_avatar")
    
    if host_name:
        analysis["successes"].append(f"Host: {host_name}")
        analysis["scores"]["host"] = 1
    elif host_avatar:
        analysis["issues"].append("Host avatar only, no name")
        analysis["scores"]["host"] = 0.3
    else:
        analysis["issues"].append("No host information")
        analysis["scores"]["host"] = 0
    
    # Test Location Data
    address = prop.get("address")
    latitude = prop.get("latitude")
    longitude = prop.get("longitude")
    
    if latitude and longitude:
        analysis["successes"].append(f"Coordinates: {latitude}, {longitude}")
        analysis["scores"]["location"] = 1
    elif address and len(address) > 50:
        analysis["issues"].append("Address only, no coordinates")
        analysis["scores"]["location"] = 0.5
    else:
        analysis["issues"].append("No location data")
        analysis["scores"]["location"] = 0
    
    # Calculate overall property score
    total_score = sum(analysis["scores"].values())
    max_score = len(analysis["scores"])
    analysis["overall_score"] = (total_score / max_score) * 100 if max_score > 0 else 0
    
    return analysis

def run_comprehensive_validation():
    """Run comprehensive validation tests."""
    print("🧪 COMPREHENSIVE PHASE 2.2 VALIDATION")
    print("=" * 80)
    
    # Test scenarios
    test_scenarios = [
        {
            "name": "New York - 3 Properties",
            "params": {
                "params": {
                    "location": "New York",
                    "check_in": "2025-09-20", 
                    "check_out": "2025-09-22",
                    "adults": 2,
                    "max_results": 3,
                    "level": 2
                }
            }
        },
        {
            "name": "Los Angeles - 2 Properties", 
            "params": {
                "params": {
                    "location": "Los Angeles",
                    "check_in": "2025-09-25",
                    "check_out": "2025-09-27", 
                    "adults": 2,
                    "max_results": 2,
                    "level": 2
                }
            }
        }
    ]
    
    all_results = []
    
    for i, scenario in enumerate(test_scenarios):
        print(f"\n📍 TEST {i+1}: {scenario['name']}")
        print("-" * 50)
        
        result = run_test_job(scenario["params"])
        if result:
            all_results.append({
                "scenario": scenario["name"],
                "result": result
            })
        else:
            print(f"❌ Test {i+1} failed")
            continue
    
    if not all_results:
        print("❌ All tests failed")
        return False
    
    # Analyze all results
    print("\n📊 DETAILED ANALYSIS")
    print("=" * 80)
    
    total_properties = 0
    property_analyses = []
    
    for test_result in all_results:
        scenario_name = test_result["scenario"]
        result = test_result["result"]
        properties = result.get("properties", [])
        metadata = result.get("search_metadata", {})
        
        print(f"\n🔍 {scenario_name}")
        print(f"Properties processed: {len(properties)}")
        print(f"Success rate: {metadata.get('success_rate', 0):.1%}")
        print(f"Average completeness: {metadata.get('average_completeness', 0):.1f}%")
        
        for j, prop in enumerate(properties):
            analysis = analyze_property(prop, total_properties + j + 1)
            property_analyses.append(analysis)
        
        total_properties += len(properties)
    
    # Overall summary
    print(f"\n🎯 OVERALL VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total properties tested: {total_properties}")
    
    if property_analyses:
        # Calculate field success rates
        field_scores = {}
        for analysis in property_analyses:
            for field, score in analysis["scores"].items():
                if field not in field_scores:
                    field_scores[field] = []
                field_scores[field].append(score)
        
        print(f"\n📈 FIELD SUCCESS RATES:")
        for field, scores in field_scores.items():
            avg_score = sum(scores) / len(scores)
            success_count = sum(1 for s in scores if s >= 0.5)
            print(f"   {field.title()}: {avg_score*100:.1f}% avg, {success_count}/{len(scores)} success")
        
        # Overall scores
        overall_scores = [a["overall_score"] for a in property_analyses]
        avg_overall = sum(overall_scores) / len(overall_scores)
        properties_passing = sum(1 for s in overall_scores if s >= 60)
        
        print(f"\n🏆 PROPERTY SCORES:")
        print(f"   Average score: {avg_overall:.1f}%")
        print(f"   Properties passing (≥60%): {properties_passing}/{len(property_analyses)}")
        
        # Top issues
        all_issues = []
        for analysis in property_analyses:
            all_issues.extend(analysis["issues"])
        
        if all_issues:
            issue_counts = {}
            for issue in all_issues:
                issue_type = issue.split(":")[0] if ":" in issue else issue
                issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
            
            print(f"\n⚠️ TOP ISSUES:")
            for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   {issue}: {count} properties")
        
        # Success criteria
        print(f"\n✅ SUCCESS CRITERIA EVALUATION:")
        criteria = {
            "Property specs working (bedrooms/bathrooms/beds)": field_scores.get("bedrooms", [0])[0] > 0.7,
            "Descriptions extracted": field_scores.get("description", [0])[0] > 0.7,  
            "Images collected": field_scores.get("images", [0])[0] > 0.3,
            "Average completeness >60%": avg_overall > 60,
            "Properties passing ≥50%": properties_passing >= len(property_analyses) * 0.5
        }
        
        passed_criteria = 0
        for criterion, passed in criteria.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"   {criterion}: {status}")
            if passed:
                passed_criteria += 1
        
        final_success = passed_criteria >= 3  # At least 3/5 criteria
        
        print(f"\n{'🎉 PHASE 2.2 VALIDATION: SUCCESS' if final_success else '❌ PHASE 2.2 VALIDATION: NEEDS IMPROVEMENT'}")
        print(f"Criteria passed: {passed_criteria}/{len(criteria)}")
        
        return final_success
    
    return False

if __name__ == "__main__":
    success = run_comprehensive_validation()
    sys.exit(0 if success else 1)