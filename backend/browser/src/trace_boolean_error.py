#!/usr/bin/env python3
"""
Trace Boolean Error - Find Where Boolean is Being Returned
==========================================================

This script helps diagnose where the boolean is coming from in the Twitter scraper.
"""

import re

def analyze_twitter_code():
    """Analyze the Twitter code to find potential boolean return sources."""

    print("🔍 TRACING BOOLEAN ERROR SOURCE")
    print("=" * 40)

    # Read the Twitter scraper code
    try:
        with open('/app/src/tasks/twitter.py', 'r') as f:
            code = f.read()

        print("✅ Loaded Twitter scraper code")

        # Find all scrape_level methods
        level_methods = re.findall(r'async def (scrape_level_\d+).*?(?=\n    async def|\n    def|\nclass|\Z)', code, re.DOTALL)

        print(f"\n📋 Found {len(level_methods)} scrape_level methods")

        # Check what each level method returns
        for i, method_body in enumerate(level_methods, 1):
            print(f"\n🔍 Analyzing scrape_level_{i}:")

            # Find return statements
            returns = re.findall(r'return\s+([^\n]+)', method_body)

            for ret in returns:
                ret = ret.strip()
                if ret in ['True', 'False']:
                    print(f"   ❌ BOOLEAN RETURN: {ret}")
                elif ret.startswith('[') and ret.endswith(']'):
                    print(f"   ✅ LIST RETURN: {ret[:50]}...")
                elif 'await' in ret:
                    print(f"   🔄 ASYNC RETURN: {ret[:50]}...")
                else:
                    print(f"   ⚠️ OTHER RETURN: {ret[:50]}...")

        # Look for exception handlers that might return booleans
        print(f"\n🚨 CHECKING EXCEPTION HANDLERS:")

        exception_handlers = re.findall(r'except.*?:\s*\n(.*?)(?=\n\s*(?:except|finally|def|class|$))', code, re.DOTALL)

        bool_returns_in_exceptions = 0
        for handler in exception_handlers:
            if 'return True' in handler or 'return False' in handler:
                bool_returns_in_exceptions += 1
                print(f"   ❌ Exception handler returns boolean")
                # Show a snippet
                lines = handler.strip().split('\n')[:3]
                for line in lines:
                    print(f"      {line.strip()}")
                print("      ...")

        print(f"\n📊 Found {bool_returns_in_exceptions} exception handlers returning booleans")

        # Check the main run method call chain
        print(f"\n🔗 CHECKING MAIN CALL CHAIN:")

        # Find the results assignment in the run method
        run_method_match = re.search(r'results = await scraper\.scrape_level_\d+\(.*?\)', code)
        if run_method_match:
            print(f"   ✅ Found main call: {run_method_match.group()}")
        else:
            print(f"   ❌ Main call not found")

        # Check if success rate calculation is called right after
        success_calc_pattern = r'success_rate = calculate_extraction_success_rate\(results,.*?\)'
        success_calc_match = re.search(success_calc_pattern, code)
        if success_calc_match:
            print(f"   ✅ Success rate calc: {success_calc_match.group()}")
        else:
            print(f"   ❌ Success rate calc not found")

        return True

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        return False

def check_specific_methods():
    """Check the specific new methods I added."""

    print(f"\n🔍 CHECKING NEWLY ADDED METHODS")
    print("=" * 35)

    new_methods = [
        '_extract_user_likes',
        '_extract_user_mentions',
        '_extract_user_media',
        '_extract_user_followers',
        '_extract_user_following'
    ]

    try:
        with open('/app/src/tasks/twitter.py', 'r') as f:
            code = f.read()

        for method_name in new_methods:
            # Find the method
            pattern = f'async def {method_name}.*?(?=\n    async def|\n    def|\nclass|\Z)'
            match = re.search(pattern, code, re.DOTALL)

            if match:
                method_body = match.group()

                # Check return statements
                returns = re.findall(r'return\s+([^\n]+)', method_body)

                print(f"📝 {method_name}:")
                for ret in returns:
                    ret = ret.strip()
                    if ret == '[]':
                        print(f"   ✅ Returns empty list: {ret}")
                    elif ret.startswith('['):
                        print(f"   ✅ Returns list: {ret[:30]}...")
                    elif ret in ['True', 'False']:
                        print(f"   ❌ BOOLEAN RETURN: {ret}")
                    else:
                        print(f"   ⚠️ Returns: {ret[:30]}...")
            else:
                print(f"❌ {method_name}: NOT FOUND")

    except Exception as e:
        print(f"❌ Method check failed: {e}")

def main():
    """Run the boolean error trace analysis."""

    analyze_twitter_code()
    check_specific_methods()

    print(f"\n🎯 DIAGNOSIS COMPLETE")
    print("=" * 25)
    print("If you see any 'BOOLEAN RETURN' entries above,")
    print("those are likely the source of the error.")

if __name__ == "__main__":
    main()