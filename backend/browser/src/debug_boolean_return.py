#!/usr/bin/env python3
"""
Debug Boolean Return Issue
==========================

This script helps identify exactly where the boolean is being returned
in the Twitter scraper pipeline.
"""

import re

def debug_scraper_call_chain():
    """Debug the scraper method call chain to find boolean returns."""

    print("🔍 DEBUGGING BOOLEAN RETURN ISSUE")
    print("=" * 45)

    try:
        with open('/app/src/tasks/twitter.py', 'r') as f:
            code = f.read()

        print("✅ Loaded Twitter scraper code")

        # Let's trace through the exact call chain that leads to boolean returns

        # Step 1: Find scrape_level_X method implementations
        level_methods = {
            1: re.search(r'async def scrape_level_1.*?(?=\n    async def|\n    def|\nclass|\Z)', code, re.DOTALL),
            2: re.search(r'async def scrape_level_2.*?(?=\n    async def|\n    def|\nclass|\Z)', code, re.DOTALL),
            3: re.search(r'async def scrape_level_3.*?(?=\n    async def|\n    def|\nclass|\Z)', code, re.DOTALL),
            4: re.search(r'async def scrape_level_4.*?(?=\n    async def|\n    def|\nclass|\Z)', code, re.DOTALL)
        }

        print(f"\n📋 SCRAPE LEVEL METHODS ANALYSIS:")

        for level, match in level_methods.items():
            if match:
                method_body = match.group()

                print(f"\n🔍 Level {level} Analysis:")

                # Find all async calls in this method
                async_calls = re.findall(r'await\s+([^(]+)', method_body)

                print(f"   📞 Async calls: {len(async_calls)}")
                for call in async_calls[:5]:  # Show first 5
                    if 'self.' in call:
                        print(f"      • {call.strip()}")

                # Check return statement
                returns = re.findall(r'return\s+([^\n]+)', method_body)
                print(f"   📤 Returns: {len(returns)}")
                for ret in returns:
                    ret_clean = ret.strip()
                    if ret_clean.startswith('[{'):
                        print(f"      ✅ List return: {ret_clean[:50]}...")
                    elif ret_clean in ['True', 'False']:
                        print(f"      ❌ BOOLEAN RETURN: {ret_clean}")
                    else:
                        print(f"      ⚠️ Return: {ret_clean[:30]}...")

                # Look for exception handlers that might return unexpected values
                try_blocks = re.findall(r'try:(.*?)except.*?:(.*?)(?=try:|def|class|$)', method_body, re.DOTALL)
                if try_blocks:
                    print(f"   🚨 Exception handlers: {len(try_blocks)}")
            else:
                print(f"\n❌ Level {level}: Method not found")

        # Step 2: Check the key helper methods that level methods call
        helper_methods = [
            '_extract_profile_info',
            '_extract_posts_basic',
            '_extract_posts_with_engagement',
            '_extract_posts_comprehensive',
            '_extract_posts_with_full_data'
        ]

        print(f"\n🔧 HELPER METHODS ANALYSIS:")

        for method_name in helper_methods:
            pattern = f'async def {method_name}.*?(?=\n    async def|\n    def|\nclass|\Z)'
            match = re.search(pattern, code, re.DOTALL)

            if match:
                method_body = match.group()

                # Check what this method returns
                returns = re.findall(r'return\s+([^\n]+)', method_body)

                print(f"\n📝 {method_name}:")
                if returns:
                    for ret in returns:
                        ret_clean = ret.strip()
                        if ret_clean == '[]':
                            print(f"   ✅ Empty list: {ret_clean}")
                        elif ret_clean.startswith('[') or 'tweets' in ret_clean or 'posts' in ret_clean:
                            print(f"   ✅ List/data: {ret_clean[:40]}...")
                        elif ret_clean in ['True', 'False']:
                            print(f"   ❌ BOOLEAN: {ret_clean}")
                        elif ret_clean == 'None':
                            print(f"   ⚠️ None: {ret_clean}")
                        else:
                            print(f"   ⚠️ Other: {ret_clean[:40]}...")
                else:
                    print(f"   ❓ No explicit returns found")
            else:
                print(f"\n❌ {method_name}: Method not found")

        # Step 3: Look for any method that might accidentally return a boolean
        print(f"\n🔍 SEARCHING FOR ACCIDENTAL BOOLEAN RETURNS:")

        # Find methods that should return List but might return bool
        list_return_pattern = r'async def [^(]*\([^)]*\) -> List\[.*?\]:(.*?)(?=\n    async def|\n    def|\nclass|\Z)'
        list_methods = re.findall(list_return_pattern, code, re.DOTALL)

        print(f"   📊 Methods that should return List: {len(list_methods)}")

        boolean_issues = 0
        for i, method_body in enumerate(list_methods):
            bool_returns = re.findall(r'return\s+(True|False)', method_body)
            if bool_returns:
                boolean_issues += 1
                print(f"   ❌ Method {i+1}: Returns boolean {bool_returns}")

        print(f"   🚨 Methods with boolean return issues: {boolean_issues}")

        return True

    except Exception as e:
        print(f"❌ Debug failed: {e}")
        return False

def check_specific_scraper_integration():
    """Check how the scraper integrates with the main run method."""

    print(f"\n🔗 SCRAPER INTEGRATION ANALYSIS:")
    print("=" * 35)

    try:
        with open('/app/src/tasks/twitter.py', 'r') as f:
            code = f.read()

        # Find the main run method where results are assigned
        run_pattern = r'async def run\(.*?\).*?(?=\n    async def|\n    def|\nclass|\Z)'
        run_match = re.search(run_pattern, code, re.DOTALL)

        if run_match:
            run_method = run_match.group()

            # Find scraper assignments
            scraper_assignments = re.findall(r'results = await scraper\.(scrape_level_\d+)\(.*?\)', run_method)

            print(f"📞 Scraper method calls found: {len(scraper_assignments)}")
            for call in scraper_assignments:
                print(f"   • {call}")

            # Find where results is used after assignment
            results_usage = re.findall(r'results.*?[^\w]', run_method)
            print(f"📊 Results variable usage: {len(results_usage)} times")

            # Look for type checking on results
            if 'isinstance(results,' in run_method:
                print("   ✅ Type checking found")
            else:
                print("   ❌ No type checking on results")

    except Exception as e:
        print(f"❌ Integration analysis failed: {e}")

def main():
    """Run the boolean return debugging."""

    debug_scraper_call_chain()
    check_specific_scraper_integration()

    print(f"\n🎯 DEBUGGING COMPLETE")
    print("=" * 25)
    print("Look for '❌ BOOLEAN RETURN' entries above to find the issue.")

if __name__ == "__main__":
    main()