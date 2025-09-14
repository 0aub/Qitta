#!/usr/bin/env python3
"""
Test the boolean return fix by checking method return types.
"""

import sys
import os

# Add the src path for imports
sys.path.append('/app/src')

def test_boolean_fix():
    """Test that our boolean fix worked."""

    print("🔍 TESTING BOOLEAN RETURN FIX")
    print("=" * 35)

    # Read the current twitter.py file
    twitter_file = '/home/aub/boo/Qitta/backend/browser/src/tasks/twitter.py'

    if not os.path.exists(twitter_file):
        print("❌ Twitter file not found at expected path")
        return

    with open(twitter_file, 'r') as f:
        content = f.read()

    # Check if the problematic "return True" line is fixed
    problematic_lines = []
    lines = content.split('\n')

    for i, line in enumerate(lines, 1):
        if 'return True' in line and 'scrape_user_comprehensive' in content[max(0, content.find(line) - 1000):content.find(line)]:
            # This is potentially in scrape_user_comprehensive method
            context_start = max(0, i-5)
            context_end = min(len(lines), i+5)
            problematic_lines.append({
                'line_no': i,
                'line': line.strip(),
                'context': lines[context_start:context_end]
            })

    if problematic_lines:
        print(f"⚠️ Found {len(problematic_lines)} potentially problematic 'return True' statements:")
        for issue in problematic_lines:
            print(f"  Line {issue['line_no']}: {issue['line']}")
    else:
        print("✅ No problematic 'return True' statements found in scrape_user_comprehensive")

    # Check for the fixed "break" statement
    if 'self.logger.info("🎉 CONTENT LOADED SUCCESSFULLY!")' in content:
        next_line_index = content.find('self.logger.info("🎉 CONTENT LOADED SUCCESSFULLY!")') + len('self.logger.info("🎉 CONTENT LOADED SUCCESSFULLY!")')
        next_few_chars = content[next_line_index:next_line_index+50]
        if 'break' in next_few_chars:
            print("✅ Found fixed 'break' statement after content loaded")
        else:
            print("❌ Break statement not found after content loaded message")

    # Check return statements in scrape_user_comprehensive
    method_start = content.find('async def scrape_user_comprehensive')
    if method_start == -1:
        print("❌ scrape_user_comprehensive method not found")
        return

    # Find the end of the method (next async def or end of class)
    method_end = content.find('\n    async def', method_start + 1)
    if method_end == -1:
        method_end = content.find('\n    def', method_start + 1)
    if method_end == -1:
        method_end = len(content)

    method_content = content[method_start:method_end]

    # Check final return statements
    return_statements = []
    for line in method_content.split('\n'):
        if line.strip().startswith('return '):
            return_statements.append(line.strip())

    print(f"\n📋 Return statements in scrape_user_comprehensive:")
    for ret in return_statements:
        print(f"  {ret}")
        if ret == 'return [results]':
            print("    ✅ Correct list return")
        elif ret == 'return True':
            print("    ❌ Problematic boolean return")
        elif 'results' in ret and '[' in ret:
            print("    ✅ Likely correct list return")
        else:
            print("    ⚠️ Unknown return type")

    print(f"\n🎯 BOOLEAN FIX TEST COMPLETE")

if __name__ == "__main__":
    test_boolean_fix()