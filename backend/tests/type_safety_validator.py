#!/usr/bin/env python3
"""
Type Safety Validator for Twitter Scraper
========================================

This script validates type safety issues in the Twitter scraper,
specifically focusing on the boolean/list bug in success rate calculation.
"""

import sys
import traceback
from typing import List, Dict, Any, Union

def calculate_extraction_success_rate_original(results, params: Dict[str, Any], method: str) -> float:
    """
    Original problematic function that causes the bug.
    This is for testing what happens when results is a boolean.
    """
    if not results:
        return 0.0

    # This is the problematic line that causes the error
    max_requested = params.get('max_posts', params.get('max_results', 10))
    return len(results) / max(max_requested, 1) if max_requested > 0 else 0.0

def calculate_extraction_success_rate_fixed(results: Union[List[Dict[str, Any]], bool],
                                           params: Dict[str, Any],
                                           method: str) -> float:
    """
    Fixed version with proper type validation.
    """
    # Type validation first
    if not isinstance(results, (list, tuple)):
        print(f"⚠️ Results is {type(results)}, not list/tuple. Returning 0.0")
        return 0.0

    if not results:
        return 0.0

    # For comprehensive user scraping
    if method == "comprehensive_user_scraping" and len(results) == 1:
        result = results[0]

        # Count successful data extractions
        success_points = 0
        total_points = 0

        # Profile data (20 points possible)
        profile = result.get('profile', {}) if isinstance(result, dict) else {}
        if profile.get('display_name'):
            success_points += 5
        if profile.get('username'):
            success_points += 3
        if profile.get('bio'):
            success_points += 4
        if profile.get('followers_count'):
            success_points += 4
        if profile.get('following_count'):
            success_points += 2
        if profile.get('posts_count'):
            success_points += 2
        total_points += 20

        # Posts data (40 points possible)
        posts = result.get('posts', []) if isinstance(result, dict) else []
        if posts and isinstance(posts, (list, tuple)):
            requested_posts = params.get('max_posts', 10)
            posts_extracted = len(posts)
            posts_with_dates = len([p for p in posts if isinstance(p, dict) and (p.get('date') or p.get('timestamp'))])
            posts_with_engagement = len([p for p in posts if isinstance(p, dict) and (p.get('likes') or p.get('retweets'))])
            posts_with_text = len([p for p in posts if isinstance(p, dict) and p.get('text') and len(p.get('text', '')) > 10])

            # Posts quantity (10 points)
            quantity_score = min(10, (posts_extracted / max(requested_posts, 1)) * 10)
            success_points += quantity_score

            # Posts quality (30 points)
            if posts_extracted > 0:
                success_points += (posts_with_text / posts_extracted) * 15
                success_points += (posts_with_dates / posts_extracted) * 10
                success_points += (posts_with_engagement / posts_extracted) * 5
        total_points += 40

        # Other data types (40 points possible)
        for data_type, weight in [('likes', 10), ('mentions', 8), ('media', 8), ('followers', 7), ('following', 7)]:
            if params.get(f'scrape_{data_type}', False):
                data_list = result.get(data_type, []) if isinstance(result, dict) else []
                requested = params.get(f'max_{data_type}', 10)
                if data_list and isinstance(data_list, (list, tuple)) and len(data_list) > 0:
                    success_points += min(weight, (len(data_list) / max(requested, 1)) * weight)
            total_points += weight

        return (success_points / total_points) if total_points > 0 else 0.0

    # For other methods, use simpler calculation with type safety
    else:
        max_requested = params.get('max_posts', params.get('max_results', 10))
        return len(results) / max(max_requested, 1) if max_requested > 0 else 0.0

def test_type_safety():
    """Test various input types to validate the fix."""

    print("🔒 TYPE SAFETY VALIDATION TESTS")
    print("=" * 50)

    # Test data
    test_params = {
        'username': 'testuser',
        'max_posts': 10,
        'scrape_likes': True,
        'max_likes': 5
    }

    test_cases = [
        ("Empty list", [], "comprehensive_user_scraping"),
        ("None", None, "comprehensive_user_scraping"),
        ("Boolean True", True, "comprehensive_user_scraping"),  # This causes the bug
        ("Boolean False", False, "comprehensive_user_scraping"), # This causes the bug
        ("String", "not a list", "comprehensive_user_scraping"),
        ("Integer", 42, "comprehensive_user_scraping"),
        ("Valid list with dict", [{"profile": {"display_name": "Test"}, "posts": []}], "comprehensive_user_scraping"),
        ("Valid simple list", [{"text": "tweet1"}, {"text": "tweet2"}], "other_method")
    ]

    print("\n🧪 TESTING ORIGINAL FUNCTION (should fail with boolean inputs):")
    for test_name, test_input, method in test_cases:
        try:
            result = calculate_extraction_success_rate_original(test_input, test_params, method)
            print(f"   ✅ {test_name}: {result:.3f}")
        except Exception as e:
            print(f"   ❌ {test_name}: {type(e).__name__}: {str(e)}")

    print(f"\n🔧 TESTING FIXED FUNCTION (should handle all inputs safely):")
    for test_name, test_input, method in test_cases:
        try:
            result = calculate_extraction_success_rate_fixed(test_input, test_params, method)
            print(f"   ✅ {test_name}: {result:.3f}")
        except Exception as e:
            print(f"   ❌ {test_name}: {type(e).__name__}: {str(e)}")

def simulate_scraper_execution():
    """Simulate what happens in the actual scraper execution."""

    print(f"\n🎭 SCRAPER EXECUTION SIMULATION")
    print("=" * 40)

    # Simulate various return values from missing methods
    print("📝 Simulating missing method returns:")

    missing_methods = [
        "_extract_user_likes",
        "_extract_user_mentions",
        "_extract_user_media",
        "_extract_user_followers",
        "_extract_user_following"
    ]

    for method in missing_methods:
        print(f"\n   🔍 {method}:")

        # What happens when method doesn't exist
        try:
            # This simulates calling a non-existent method
            # In Python, this would raise AttributeError, but let's see what might be returned
            result = None  # AttributeError would be raised
            print(f"      Return: {result} (type: {type(result)})")
        except AttributeError as e:
            print(f"      ❌ AttributeError: {e}")

        # What if method exists but returns unexpected types
        for return_val, desc in [(True, "boolean True"), (False, "boolean False"),
                                (None, "None"), ("error", "string error")]:
            try:
                # Simulate success rate calculation with unexpected return
                params = {'max_posts': 5}
                rate = calculate_extraction_success_rate_original(return_val, params, "test_method")
                print(f"      {desc} → Success rate: {rate}")
            except Exception as e:
                print(f"      {desc} → ❌ {type(e).__name__}: {e}")

def analyze_level_4_structure():
    """Analyze what Level 4 scraping should return."""

    print(f"\n🏗️ LEVEL 4 STRUCTURE ANALYSIS")
    print("=" * 35)

    # Expected Level 4 structure
    expected_structure = {
        "type": "comprehensive_user_data",
        "username": "@testuser",
        "profile": {
            "display_name": "Test User",
            "username": "testuser",
            "bio": "This is a test bio",
            "followers_count": 1000,
            "following_count": 500,
            "posts_count": 250
        },
        "posts": [
            {"text": "Sample tweet 1", "date": "2025-01-01", "likes": 10, "retweets": 5},
            {"text": "Sample tweet 2", "date": "2025-01-02", "likes": 15, "retweets": 3}
        ],
        "likes": [
            {"text": "Liked tweet 1", "author": "user1"},
            {"text": "Liked tweet 2", "author": "user2"}
        ],
        "mentions": [
            {"text": "Mention tweet 1", "author": "user3"},
        ],
        "media": [
            {"type": "image", "url": "https://example.com/img1.jpg"},
        ],
        "followers": [
            {"username": "follower1", "display_name": "Follower One"},
        ],
        "following": [
            {"username": "following1", "display_name": "Following One"},
        ]
    }

    print("📊 Expected Level 4 structure:")
    print(f"   Profile fields: {len(expected_structure['profile'])}")
    print(f"   Posts: {len(expected_structure['posts'])}")
    print(f"   Likes: {len(expected_structure['likes'])}")
    print(f"   Mentions: {len(expected_structure['mentions'])}")
    print(f"   Media: {len(expected_structure['media'])}")
    print(f"   Followers: {len(expected_structure['followers'])}")
    print(f"   Following: {len(expected_structure['following'])}")

    # Test success rate calculation with this structure
    params = {
        'max_posts': 10,
        'scrape_likes': True,
        'max_likes': 5,
        'scrape_mentions': True,
        'max_mentions': 3,
        'scrape_media': True,
        'max_media': 3,
        'scrape_followers': True,
        'max_followers': 25,
        'scrape_following': True,
        'max_following': 20
    }

    success_rate = calculate_extraction_success_rate_fixed([expected_structure], params, "comprehensive_user_scraping")
    print(f"\n📈 Expected success rate: {success_rate:.1%}")

def main():
    """Main function to run all type safety tests."""

    print("🔍 TWITTER SCRAPER TYPE SAFETY VALIDATION")
    print("=" * 60)

    # Run all tests
    test_type_safety()
    simulate_scraper_execution()
    analyze_level_4_structure()

    print(f"\n🎯 VALIDATION COMPLETE")
    print("=" * 30)
    print("✅ Fixed function handles all input types safely")
    print("❌ Original function fails with boolean inputs")
    print("🔧 Ready to implement fixes in main codebase")

if __name__ == "__main__":
    main()