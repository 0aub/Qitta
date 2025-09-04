#!/usr/bin/env python3
"""
TEST PAGINATION FIXES - Validate enhanced JavaScript-aware pagination
"""

import os, time, requests, json
from datetime import datetime

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def test_enhanced_pagination(name, search_term):
    """Test enhanced pagination with specific hotel"""
    print(f"\n🧪 Testing Enhanced Pagination: {name}")
    print(f"🔍 Search: {search_term}")
    
    start_time = time.time()
    
    try:
        # Submit job with Level 4
        r = requests.post(f"{EP}/jobs/booking-hotels", json={
            "location": search_term,
            "scrape_level": 4,
            "max_results": 1
        })
        r.raise_for_status()
        job_id = r.json()["job_id"]
        
        print(f"🆔 Job ID: {job_id}")
        print(f"⏳ Waiting for enhanced pagination test...")
        
        # Wait for completion with timeout
        timeout = 120  # 2 minutes
        start_wait = time.time()
        
        while time.time() - start_wait < timeout:
            status_response = requests.get(f"{EP}/jobs/{job_id}")
            status_data = status_response.json()
            status = status_data["status"]
            
            if status not in {"finished", "error"}:
                elapsed = time.time() - start_time
                print(f"\r⏱️  {status} - {elapsed:.0f}s", end="")
            else:
                print(f"\n✅ {status.upper()}")
                break
            
            time.sleep(3)
        else:
            print(f"\n⏰ TIMEOUT after {timeout}s")
            return {"success": False, "error": "timeout"}
        
        elapsed = time.time() - start_time
        
        if status == "finished":
            result = status_data.get("result", {})
            hotels = result.get("hotels", [])
            
            if hotels:
                hotel = hotels[0]
                reviews = hotel.get('reviews', [])
                pages = hotel.get('pages_processed', 1)
                method = hotel.get('extraction_method', 'Unknown')
                
                print(f"   ✅ Hotel: {hotel.get('name', 'Unknown')}")
                print(f"   📝 Reviews: {len(reviews)}")
                print(f"   📄 Pages: {pages}")
                print(f"   🔧 Method: {method}")
                print(f"   ⏱️  Time: {elapsed:.1f}s")
                
                # Enhanced analysis
                success_indicators = []
                issues = []
                
                if pages > 1:
                    success_indicators.append(f"🎉 MULTI-PAGE SUCCESS! ({pages} pages)")
                    
                if len(reviews) >= 50:
                    success_indicators.append(f"🔥 HIGH REVIEW COUNT! ({len(reviews)} reviews)")
                elif len(reviews) >= 25:
                    success_indicators.append(f"📈 IMPROVED REVIEW COUNT ({len(reviews)} reviews)")
                elif len(reviews) <= 18:
                    issues.append(f"⚠️ Same as before ({len(reviews)} reviews)")
                
                if 'ENHANCED' in method or 'PAGINATION' in method:
                    success_indicators.append("✅ Enhanced pagination method detected")
                    
                # Check for improvements
                if len(reviews) > 18 or pages > 1:
                    success_indicators.append("🚀 IMPROVEMENT DETECTED!")
                
                if success_indicators:
                    print(f"   🎯 SUCCESS INDICATORS:")
                    for indicator in success_indicators:
                        print(f"      {indicator}")
                
                if issues:
                    print(f"   ⚠️ ISSUES:")
                    for issue in issues:
                        print(f"      {issue}")
                
                return {
                    "success": True,
                    "reviews_count": len(reviews),
                    "pages_processed": pages,
                    "method": method,
                    "success_indicators": success_indicators,
                    "issues": issues,
                    "elapsed": elapsed
                }
            else:
                print(f"   ❌ No hotels found")
                return {"success": False, "error": "no_hotels"}
        else:
            error = status_data.get("error", "Unknown error")
            print(f"   ❌ Job failed: {error}")
            return {"success": False, "error": error}
    
    except Exception as e:
        print(f"   ❌ Exception: {str(e)}")
        return {"success": False, "error": str(e)}

print("🚀 TESTING ENHANCED PAGINATION FIXES")
print("=" * 60)
print(f"🕐 Started: {datetime.now().strftime('%H:%M:%S')}")

# Test with hotels that should have high review counts
TEST_CASES = [
    ("Dubai Luxury Icon", "Burj Al Arab Dubai"),
    ("Dubai Resort Complex", "Atlantis The Palm Dubai"),
    ("Dubai Premium Hotel", "Ritz Carlton Dubai")
]

all_results = []
improvements_detected = 0
multi_page_success = 0

for i, (name, search) in enumerate(TEST_CASES, 1):
    print(f"\n{'🧪' * 30}")
    print(f"TEST {i}/{len(TEST_CASES)}: ENHANCED PAGINATION")
    print('🧪' * 30)
    
    result = test_enhanced_pagination(name, search)
    all_results.append((name, result))
    
    if result['success']:
        if result.get('pages_processed', 1) > 1:
            multi_page_success += 1
        
        if result.get('reviews_count', 0) > 18 or result.get('pages_processed', 1) > 1:
            improvements_detected += 1
    
    # Brief pause between tests
    time.sleep(3)

# Final analysis
print(f"\n{'🎯' * 30}")
print("ENHANCED PAGINATION TEST RESULTS")
print('🎯' * 30)

successful_tests = sum(1 for _, result in all_results if result['success'])

print(f"📊 Test Summary:")
print(f"   ✅ Successful tests: {successful_tests}/{len(TEST_CASES)}")
print(f"   🔄 Multi-page pagination: {multi_page_success}")
print(f"   🚀 Improvements detected: {improvements_detected}")

# Key findings
if multi_page_success > 0:
    print(f"\n🎉 BREAKTHROUGH: Enhanced pagination successfully working!")
    print(f"   • {multi_page_success} hotels processed across multiple pages")
elif improvements_detected > 0:
    print(f"\n📈 PARTIAL SUCCESS: Improvements detected in {improvements_detected} tests")
    print(f"   • Enhanced methods may be working even without multi-page")
elif successful_tests >= 2:
    print(f"\n⚠️ STABLE BUT NO IMPROVEMENTS: System running but fixes may need refinement")
else:
    print(f"\n❌ CRITICAL: System instability - need to investigate")

# Specific recommendations
print(f"\n💡 Next Steps:")

if multi_page_success > 0:
    print(f"   🎯 SUCCESS! Pagination fixes are working - proceed to full validation")
elif successful_tests == 0:
    print(f"   🔧 System issues - check container and service status")
else:
    print(f"   🔍 Investigate why pagination isn't triggering - may need specific hotels with known high review counts")

print(f"\n🕐 Completed: {datetime.now().strftime('%H:%M:%S')}")
print("=" * 60)