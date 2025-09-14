#!/usr/bin/env python3
"""
Twitter Scraper Issue Investigation Script
==========================================

This script analyzes the Twitter scraper implementation to identify:
1. Missing method implementations
2. Type safety issues
3. Function call inconsistencies
4. Error patterns
"""

import os
import sys
import re
import ast
import inspect
from pathlib import Path
from typing import List, Dict, Set, Tuple

class TwitterScraperAnalyzer:
    def __init__(self, twitter_file_path: str):
        self.twitter_file_path = twitter_file_path
        self.source_code = ""
        self.missing_methods = []
        self.method_calls = set()
        self.method_definitions = set()
        self.type_issues = []

    def load_source(self):
        """Load the Twitter scraper source code."""
        try:
            with open(self.twitter_file_path, 'r', encoding='utf-8') as f:
                self.source_code = f.read()
            print(f"✅ Loaded source code from {self.twitter_file_path}")
            print(f"📊 File size: {len(self.source_code):,} characters")
        except Exception as e:
            print(f"❌ Error loading source: {e}")
            return False
        return True

    def find_method_calls(self) -> Set[str]:
        """Find all method calls in the source code."""
        # Pattern for method calls like self._extract_user_likes(
        call_pattern = r'await\s+self\.(_extract_[a-zA-Z_]+)\s*\('
        calls = set(re.findall(call_pattern, self.source_code))

        # Also check for direct calls
        direct_pattern = r'self\.(_extract_[a-zA-Z_]+)\s*\('
        direct_calls = set(re.findall(direct_pattern, self.source_code))

        all_calls = calls.union(direct_calls)
        self.method_calls = all_calls

        print(f"\n🔍 METHOD CALLS FOUND ({len(all_calls)}):")
        for call in sorted(all_calls):
            print(f"   📞 {call}")

        return all_calls

    def find_method_definitions(self) -> Set[str]:
        """Find all method definitions in the source code."""
        # Pattern for method definitions like def _extract_user_likes(
        def_pattern = r'async\s+def\s+(_extract_[a-zA-Z_]+)\s*\('
        defs = set(re.findall(def_pattern, self.source_code))

        # Also check for non-async methods
        sync_pattern = r'def\s+(_extract_[a-zA-Z_]+)\s*\('
        sync_defs = set(re.findall(sync_pattern, self.source_code))

        all_defs = defs.union(sync_defs)
        self.method_definitions = all_defs

        print(f"\n📋 METHOD DEFINITIONS FOUND ({len(all_defs)}):")
        for method in sorted(all_defs):
            print(f"   ✅ {method}")

        return all_defs

    def find_missing_methods(self) -> List[str]:
        """Identify methods that are called but not defined."""
        missing = list(self.method_calls - self.method_definitions)
        self.missing_methods = missing

        print(f"\n❌ MISSING METHODS ({len(missing)}):")
        if missing:
            for method in sorted(missing):
                print(f"   🔴 {method}")
                # Find where it's called
                self.find_method_usage(method)
        else:
            print("   🎉 No missing methods found!")

        return missing

    def find_method_usage(self, method_name: str):
        """Find where a specific method is used."""
        lines = self.source_code.split('\n')
        usage_lines = []

        for i, line in enumerate(lines, 1):
            if method_name in line and ('self.' + method_name in line):
                usage_lines.append((i, line.strip()))

        if usage_lines:
            print(f"      📍 Used at lines: {[line_num for line_num, _ in usage_lines]}")
            for line_num, line_content in usage_lines[:3]:  # Show first 3 usages
                print(f"         L{line_num}: {line_content}")

    def analyze_success_rate_bug(self):
        """Analyze the success rate calculation bug."""
        print(f"\n🐛 SUCCESS RATE CALCULATION ANALYSIS:")

        # Find the problematic function
        pattern = r'def calculate_extraction_success_rate.*?(?=\ndef|\nclass|\n\n\n|\Z)'
        matches = re.findall(pattern, self.source_code, re.DOTALL)

        if matches:
            func_code = matches[0]
            print("   📋 Function found - analyzing return types...")

            # Check for len(results) calls
            len_calls = re.findall(r'len\(results\)', func_code)
            print(f"   📏 len(results) calls found: {len(len_calls)}")

            # Check for type validation
            if 'isinstance' in func_code:
                print("   ✅ Type validation present")
            else:
                print("   ❌ No type validation found")

            # Check for boolean handling
            if 'bool' in func_code or 'True' in func_code or 'False' in func_code:
                print("   ⚠️ Boolean handling detected")
        else:
            print("   ❌ calculate_extraction_success_rate function not found")

    def analyze_level_4_method(self):
        """Analyze the scrape_level_4 method for issues."""
        print(f"\n🔍 SCRAPE_LEVEL_4 METHOD ANALYSIS:")

        pattern = r'async def scrape_level_4.*?(?=\n    async def|\n    def|\nclass|\Z)'
        matches = re.findall(pattern, self.source_code, re.DOTALL)

        if matches:
            method_code = matches[0]

            # Count method calls
            extract_calls = re.findall(r'await self\.(_extract_[a-zA-Z_]+)', method_code)
            print(f"   📞 Extraction method calls: {len(extract_calls)}")
            for call in extract_calls:
                status = "✅ DEFINED" if call in self.method_definitions else "❌ MISSING"
                print(f"      {call}: {status}")

            # Check return structure
            if 'return [{' in method_code:
                print("   📦 Returns dictionary structure")
            else:
                print("   ⚠️ Unclear return structure")
        else:
            print("   ❌ scrape_level_4 method not found")

    def find_type_safety_issues(self):
        """Identify potential type safety issues."""
        print(f"\n🔒 TYPE SAFETY ANALYSIS:")

        issues = []

        # Look for len() calls without type checking
        len_pattern = r'len\([^)]+\)'
        len_calls = re.findall(len_pattern, self.source_code)

        # Check if they're protected by isinstance
        lines = self.source_code.split('\n')
        for i, line in enumerate(lines):
            if 'len(' in line and 'isinstance' not in line:
                # Check surrounding lines for isinstance
                context_start = max(0, i-3)
                context_end = min(len(lines), i+4)
                context = '\n'.join(lines[context_start:context_end])

                if 'isinstance' not in context:
                    issues.append((i+1, line.strip()))

        print(f"   ⚠️ Potentially unsafe len() calls: {len(issues)}")
        for line_num, line_content in issues[:5]:  # Show first 5
            print(f"      L{line_num}: {line_content}")

    def generate_report(self) -> Dict:
        """Generate a comprehensive analysis report."""
        report = {
            "file_analyzed": self.twitter_file_path,
            "file_size_chars": len(self.source_code),
            "total_method_calls": len(self.method_calls),
            "total_method_definitions": len(self.method_definitions),
            "missing_methods": self.missing_methods,
            "missing_count": len(self.missing_methods),
            "method_calls": list(self.method_calls),
            "method_definitions": list(self.method_definitions)
        }

        print(f"\n📊 ANALYSIS SUMMARY:")
        print(f"   📁 File: {os.path.basename(self.twitter_file_path)}")
        print(f"   📏 Size: {len(self.source_code):,} chars")
        print(f"   📞 Method calls: {len(self.method_calls)}")
        print(f"   📋 Method definitions: {len(self.method_definitions)}")
        print(f"   ❌ Missing methods: {len(self.missing_methods)}")
        print(f"   🔒 Critical severity: {'HIGH' if len(self.missing_methods) > 3 else 'MEDIUM' if len(self.missing_methods) > 0 else 'LOW'}")

        return report

    def run_full_analysis(self):
        """Run complete analysis of the Twitter scraper."""
        print("🐦 TWITTER SCRAPER ANALYSIS STARTING")
        print("=" * 50)

        if not self.load_source():
            return None

        # Step 1: Find method calls and definitions
        self.find_method_calls()
        self.find_method_definitions()

        # Step 2: Identify missing methods
        self.find_missing_methods()

        # Step 3: Analyze specific bugs
        self.analyze_success_rate_bug()
        self.analyze_level_4_method()

        # Step 4: Type safety analysis
        self.find_type_safety_issues()

        # Step 5: Generate report
        report = self.generate_report()

        print("\n" + "=" * 50)
        print("🎯 ANALYSIS COMPLETE")

        return report

def main():
    """Main function to run the Twitter scraper analysis."""

    # Path to Twitter scraper file
    twitter_file = "/app/src/tasks/twitter.py"

    if not os.path.exists(twitter_file):
        print(f"❌ Twitter file not found: {twitter_file}")
        # Try alternative path
        alt_path = "../browser/src/tasks/twitter.py"
        if os.path.exists(alt_path):
            twitter_file = alt_path
            print(f"✅ Found at alternative path: {alt_path}")
        else:
            print("❌ Could not locate Twitter scraper file")
            return

    # Create analyzer and run analysis
    analyzer = TwitterScraperAnalyzer(twitter_file)
    report = analyzer.run_full_analysis()

    # Save report to file
    if report:
        import json
        report_file = "twitter_analysis_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n💾 Analysis report saved to: {report_file}")

if __name__ == "__main__":
    main()