"""
GithubTask - RAG-optimized GitHub repository analyzer.

Comprehensive repository knowledge extraction for RAG systems and vector stores.
Collects code, metadata, discussions, community insights, and repository health metrics.
"""

import asyncio
import logging
import json
import urllib.parse
import pathlib
import base64
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import httpx

from .base import _log


class GithubTask:
    """
    RAG-optimized GitHub repository analyzer with comprehensive data extraction.

    Features:
    - Enhanced file collection (up to 500 files with smart prioritization)
    - Per-file metadata (language, size, complexity)
    - Code structure analysis (language breakdown, architecture)
    - Pull request discussions and context
    - Contributor insights and community health
    - Dependency analysis
    - Repository health metrics
    - Structured RAG-ready output
    """

    # ═══════════════════════════════════════════════════════════════════════
    # URL PARSING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _parse_github_url(url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract owner and repo from GitHub URL."""
        try:
            parsed = urllib.parse.urlparse(url.lower())
            if 'github.com' not in parsed.netloc:
                return None, None

            path_parts = [p for p in parsed.path.split('/') if p]
            if len(path_parts) >= 2:
                return path_parts[0], path_parts[1]
            return None, None
        except Exception:
            return None, None

    # ═══════════════════════════════════════════════════════════════════════
    # GITHUB API REQUEST HANDLER
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _github_api_request(
        client: httpx.AsyncClient,
        endpoint: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[Any], Optional[str]]:
        """Make GitHub API request with error handling."""
        try:
            url = f"https://api.github.com/{endpoint.lstrip('/')}"
            response = await client.get(url, headers=headers, params=params or {})

            if response.status_code == 200:
                return True, response.json(), None
            elif response.status_code == 403:
                return False, None, "api_rate_limit"
            elif response.status_code == 404:
                return False, None, "not_found"
            else:
                return False, None, f"api_error_{response.status_code}"
        except Exception as e:
            return False, None, f"request_failed_{type(e).__name__}"

    # ═══════════════════════════════════════════════════════════════════════
    # REPOSITORY METADATA
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _get_repo_metadata(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Get comprehensive repository metadata."""
        success, data, error = await GithubTask._github_api_request(
            client, f"repos/{owner}/{repo}", headers
        )

        if not success:
            _log(logger, "error", f"Failed to fetch repo metadata: {error}")
            return {"error": error}

        return {
            "name": data.get("name"),
            "full_name": data.get("full_name"),
            "description": data.get("description"),
            "language": data.get("language"),
            "stars": data.get("stargazers_count", 0),
            "forks": data.get("forks_count", 0),
            "watchers": data.get("watchers_count", 0),
            "open_issues": data.get("open_issues_count", 0),
            "created_at": data.get("created_at"),
            "updated_at": data.get("updated_at"),
            "pushed_at": data.get("pushed_at"),
            "size_kb": data.get("size", 0),
            "default_branch": data.get("default_branch", "main"),
            "clone_url": data.get("clone_url"),
            "homepage": data.get("homepage"),
            "topics": data.get("topics", []),
            "license": data.get("license", {}).get("name") if data.get("license") else None,
            "has_issues": data.get("has_issues", False),
            "has_projects": data.get("has_projects", False),
            "has_wiki": data.get("has_wiki", False),
            "has_pages": data.get("has_pages", False),
            "archived": data.get("archived", False),
            "disabled": data.get("disabled", False)
        }

    # ═══════════════════════════════════════════════════════════════════════
    # ENHANCED FILE COLLECTION (RAG-OPTIMIZED)
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _get_repo_contents(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger,
        max_files: int = 500,
        output_dir: pathlib.Path = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Get repository file contents with smart prioritization.
        Returns: (files_metadata, code_structure_analysis)
        """
        # Get repository tree
        success, tree_data, error = await GithubTask._github_api_request(
            client, f"repos/{owner}/{repo}/git/trees/HEAD?recursive=1", headers
        )

        if not success:
            _log(logger, "warning", f"Could not fetch repo tree: {error}")
            return [], {}

        # Categorize files by priority
        files_by_priority = GithubTask._categorize_files(tree_data.get("tree", []))

        # Collect files based on priority
        files_collected = []
        files_metadata = []

        for priority in ["critical", "high", "medium", "low"]:
            if len(files_collected) >= max_files:
                break

            for file_info in files_by_priority.get(priority, []):
                if len(files_collected) >= max_files:
                    break

                path = file_info.get("path", "")

                # Download file content
                content = await GithubTask._get_file_content(
                    client, owner, repo, path, headers, logger
                )

                if content:
                    # Save file to disk if output_dir provided
                    if output_dir:
                        file_path = output_dir / "files" / path
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        try:
                            file_path.write_text(content, encoding='utf-8')
                        except Exception as e:
                            _log(logger, "debug", f"Could not save {path}: {e}")

                    # Extract file metadata
                    file_metadata = GithubTask._extract_file_metadata(path, content, priority)
                    files_metadata.append(file_metadata)

                    files_collected.append({
                        "path": path,
                        "filename": path.split("/")[-1],
                        "content": content[:50000],  # Limit content size
                        "priority": priority,
                        **file_metadata
                    })

        _log(logger, "info", f"Collected {len(files_collected)} repository files")

        # Analyze code structure
        code_structure = GithubTask._analyze_code_structure(files_collected, tree_data.get("tree", []))

        return files_collected, code_structure

    @staticmethod
    def _categorize_files(tree: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Categorize files by priority for smart collection."""
        categories = {
            "critical": [],  # Essential docs and config
            "high": [],      # Core source files
            "medium": [],    # Tests and examples
            "low": []        # Other files
        }

        for item in tree:
            if item.get("type") != "blob":
                continue

            path = item.get("path", "").lower()
            filename = path.split("/")[-1]

            # Critical: Documentation and configuration
            if any(doc in filename for doc in [
                "readme", "contributing", "license", "changelog", "security",
                "code_of_conduct", "package.json", "requirements.txt", "pyproject.toml",
                "cargo.toml", "go.mod", "pom.xml", "build.gradle", "dockerfile",
                ".gitignore", "makefile"
            ]):
                categories["critical"].append(item)

            # High: Core source code (in src/, lib/, app/ directories)
            elif any(dir in path for dir in ["src/", "lib/", "app/", "core/"]):
                if any(path.endswith(ext) for ext in [
                    ".py", ".js", ".ts", ".java", ".cpp", ".c", ".go", ".rs",
                    ".php", ".rb", ".swift", ".kt", ".scala", ".r", ".m"
                ]):
                    categories["high"].append(item)

            # Medium: Tests and examples
            elif any(dir in path for dir in ["test", "tests", "spec", "example", "demo"]):
                categories["medium"].append(item)

            # Low: Other source files
            elif any(path.endswith(ext) for ext in [
                ".py", ".js", ".ts", ".java", ".cpp", ".c", ".go", ".rs"
            ]):
                categories["low"].append(item)

        return categories

    @staticmethod
    def _extract_file_metadata(path: str, content: str, priority: str) -> Dict[str, Any]:
        """Extract metadata for a single file."""
        ext = pathlib.Path(path).suffix

        # Detect language
        language_map = {
            ".py": "Python", ".js": "JavaScript", ".ts": "TypeScript",
            ".java": "Java", ".cpp": "C++", ".c": "C", ".go": "Go",
            ".rs": "Rust", ".php": "PHP", ".rb": "Ruby", ".swift": "Swift",
            ".kt": "Kotlin", ".scala": "Scala", ".r": "R", ".m": "Objective-C",
            ".sh": "Shell", ".yml": "YAML", ".yaml": "YAML", ".json": "JSON",
            ".xml": "XML", ".md": "Markdown", ".html": "HTML", ".css": "CSS"
        }

        language = language_map.get(ext, "Unknown")

        # Calculate metrics
        lines = content.split('\n')
        lines_of_code = len([l for l in lines if l.strip() and not l.strip().startswith('#')])

        # Simple complexity indicator (based on nesting and length)
        complexity = "low"
        if lines_of_code > 500:
            complexity = "high"
        elif lines_of_code > 200:
            complexity = "medium"

        # Extract imports/dependencies (simplified)
        imports = []
        if language == "Python":
            imports = re.findall(r'^import\s+(\w+)|^from\s+(\w+)', content, re.MULTILINE)
            imports = [i[0] or i[1] for i in imports if i]
        elif language in ["JavaScript", "TypeScript"]:
            imports = re.findall(r'import.*from\s+["\'](.+?)["\']', content)

        return {
            "language": language,
            "size_bytes": len(content.encode('utf-8')),
            "lines_of_code": lines_of_code,
            "complexity": complexity,
            "category": "documentation" if ext == ".md" else "source_code" if ext in language_map else "config",
            "imports": list(set(imports))[:10],  # Limit to 10 unique imports
            "extension": ext
        }

    @staticmethod
    def _analyze_code_structure(files: List[Dict[str, Any]], tree: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze overall code structure and organization."""
        # Language breakdown
        languages = {}
        total_loc = 0

        for file in files:
            lang = file.get("language", "Unknown")
            loc = file.get("lines_of_code", 0)
            languages[lang] = languages.get(lang, 0) + loc
            total_loc += loc

        language_percentages = {
            lang: round((count / total_loc * 100), 1) if total_loc > 0 else 0
            for lang, count in languages.items()
        }

        # Directory structure
        directories = {}
        for item in tree:
            if item.get("type") == "tree":
                path = item.get("path", "")
                depth = path.count('/')
                directories[path] = depth

        # Identify entry points
        entry_points = []
        for file in files:
            path = file.get("path", "")
            if any(name in path.lower() for name in ["main", "index", "app", "__init__"]):
                entry_points.append(path)

        return {
            "total_files": len(files),
            "total_lines_of_code": total_loc,
            "languages": language_percentages,
            "primary_language": max(language_percentages.items(), key=lambda x: x[1])[0] if language_percentages else "Unknown",
            "directory_count": len(directories),
            "max_directory_depth": max(directories.values()) if directories else 0,
            "entry_points": entry_points[:5]  # Top 5 entry points
        }

    @staticmethod
    async def _get_file_content(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        path: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Optional[str]:
        """Get content of a specific file."""
        try:
            success, data, error = await GithubTask._github_api_request(
                client, f"repos/{owner}/{repo}/contents/{path}", headers
            )

            if not success:
                return None

            # Decode base64 content
            content = base64.b64decode(data.get("content", "")).decode("utf-8", errors="ignore")

            # Limit file size
            if len(content) > 100000:  # 100KB limit
                content = content[:100000] + "\n... (truncated)"

            return content

        except Exception as e:
            _log(logger, "debug", f"Could not read file {path}: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # PULL REQUEST DATA
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _get_pull_requests(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger,
        max_prs: int = 50
    ) -> List[Dict[str, Any]]:
        """Get pull requests with discussions and context."""
        success, data, error = await GithubTask._github_api_request(
            client, f"repos/{owner}/{repo}/pulls",
            headers,
            {"state": "all", "per_page": max_prs, "sort": "updated", "direction": "desc"}
        )

        if not success:
            _log(logger, "warning", f"Could not fetch pull requests: {error}")
            return []

        prs = []
        for pr in data[:max_prs]:
            pr_data = {
                "number": pr.get("number"),
                "title": pr.get("title"),
                "state": pr.get("state"),
                "body": (pr.get("body") or "")[:2000],  # Limit body length
                "created_at": pr.get("created_at"),
                "updated_at": pr.get("updated_at"),
                "closed_at": pr.get("closed_at"),
                "merged_at": pr.get("merged_at"),
                "merged": pr.get("merged", False),
                "user": pr.get("user", {}).get("login"),
                "labels": [label.get("name") for label in pr.get("labels", [])],
                "comments_count": pr.get("comments", 0),
                "review_comments_count": pr.get("review_comments", 0),
                "commits_count": pr.get("commits", 0),
                "url": pr.get("html_url")
            }
            prs.append(pr_data)

        _log(logger, "info", f"Collected {len(prs)} pull requests")
        return prs

    # ═══════════════════════════════════════════════════════════════════════
    # CONTRIBUTOR & COMMUNITY DATA
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _get_contributors(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger,
        max_contributors: int = 30
    ) -> Dict[str, Any]:
        """Get contributor insights and community data."""
        success, data, error = await GithubTask._github_api_request(
            client, f"repos/{owner}/{repo}/contributors",
            headers,
            {"per_page": max_contributors}
        )

        if not success:
            _log(logger, "warning", f"Could not fetch contributors: {error}")
            return {"contributors": [], "community_health": {}}

        contributors = []
        for contrib in data[:max_contributors]:
            contributors.append({
                "username": contrib.get("login"),
                "contributions": contrib.get("contributions", 0),
                "avatar_url": contrib.get("avatar_url"),
                "profile_url": contrib.get("html_url"),
                "type": contrib.get("type", "User")
            })

        # Community health indicators
        total_contributions = sum(c["contributions"] for c in contributors)
        active_contributors = len([c for c in contributors if c["contributions"] >= 5])

        community_health = {
            "total_contributors": len(contributors),
            "active_contributors": active_contributors,
            "total_contributions": total_contributions,
            "top_contributor": contributors[0]["username"] if contributors else None,
            "contribution_distribution": "balanced" if len(contributors) > 10 else "concentrated"
        }

        _log(logger, "info", f"Collected {len(contributors)} contributors")
        return {
            "contributors": contributors,
            "community_health": community_health
        }

    # ═══════════════════════════════════════════════════════════════════════
    # DEPENDENCY ANALYSIS
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _extract_dependencies(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Extract and analyze project dependencies."""
        dependencies = {
            "production": {},
            "development": {},
            "dependency_files": []
        }

        # Check for common dependency files
        dep_files = [
            ("package.json", "npm"),
            ("requirements.txt", "pip"),
            ("Pipfile", "pipenv"),
            ("pyproject.toml", "poetry"),
            ("Cargo.toml", "cargo"),
            ("go.mod", "go"),
            ("pom.xml", "maven"),
            ("build.gradle", "gradle")
        ]

        for filename, package_manager in dep_files:
            content = await GithubTask._get_file_content(
                client, owner, repo, filename, headers, logger
            )

            if content:
                dependencies["dependency_files"].append(filename)

                # Parse package.json
                if filename == "package.json":
                    try:
                        pkg_data = json.loads(content)
                        dependencies["production"].update(pkg_data.get("dependencies", {}))
                        dependencies["development"].update(pkg_data.get("devDependencies", {}))
                    except:
                        pass

                # Parse requirements.txt
                elif filename == "requirements.txt":
                    lines = content.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            pkg = line.split('==')[0].split('>=')[0].strip()
                            dependencies["production"][pkg] = line.split(pkg)[-1].strip() if '==' in line else "latest"

        total_deps = len(dependencies["production"]) + len(dependencies["development"])

        return {
            **dependencies,
            "total_dependencies": total_deps,
            "has_dependencies": total_deps > 0
        }

    # ═══════════════════════════════════════════════════════════════════════
    # REPOSITORY HEALTH METRICS
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _calculate_health_metrics(
        metadata: Dict[str, Any],
        files: List[Dict[str, Any]],
        prs: List[Dict[str, Any]],
        contributors_data: Dict[str, Any],
        dependencies: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate comprehensive repository health metrics."""

        # Documentation score
        has_readme = any("readme" in f.get("path", "").lower() for f in files)
        has_contributing = any("contributing" in f.get("path", "").lower() for f in files)
        has_license = metadata.get("license") is not None
        doc_files_count = len([f for f in files if f.get("category") == "documentation"])

        doc_score = 0
        if has_readme:
            doc_score += 0.4
        if has_contributing:
            doc_score += 0.2
        if has_license:
            doc_score += 0.2
        if doc_files_count > 3:
            doc_score += 0.2

        # Activity indicators
        from datetime import datetime as dt
        try:
            last_push = dt.fromisoformat(metadata.get("pushed_at", "").replace('Z', '+00:00'))
            days_since_push = (dt.now(last_push.tzinfo) - last_push).days
            is_active = days_since_push < 90
        except:
            days_since_push = 999
            is_active = False

        # Test indicators
        has_tests = any("test" in f.get("path", "").lower() for f in files)
        has_ci = any(f.get("path", "").startswith(".github/workflows/") for f in files)

        # Community engagement
        community = contributors_data.get("community_health", {})
        community_score = 0
        if community.get("active_contributors", 0) >= 5:
            community_score += 0.5
        if metadata.get("stars", 0) >= 100:
            community_score += 0.3
        if metadata.get("forks", 0) >= 20:
            community_score += 0.2

        # Overall health score
        health_score = (doc_score * 0.3 + community_score * 0.3 + (0.2 if has_tests else 0) + (0.2 if is_active else 0))

        return {
            "documentation_score": round(doc_score, 2),
            "has_readme": has_readme,
            "has_contributing_guide": has_contributing,
            "has_license": has_license,
            "has_tests": has_tests,
            "has_ci_cd": has_ci,
            "is_actively_maintained": is_active,
            "days_since_last_push": days_since_push,
            "community_engagement_score": round(community_score, 2),
            "dependency_health": dependencies.get("has_dependencies", False),
            "overall_health_score": round(health_score, 2),
            "health_rating": "excellent" if health_score >= 0.8 else "good" if health_score >= 0.6 else "fair" if health_score >= 0.4 else "poor"
        }

    # ═══════════════════════════════════════════════════════════════════════
    # ISSUES (EXISTING)
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _get_issues(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger,
        max_issues: int = 50
    ) -> List[Dict[str, Any]]:
        """Get repository issues."""
        success, data, error = await GithubTask._github_api_request(
            client, f"repos/{owner}/{repo}/issues",
            headers,
            {"state": "all", "per_page": max_issues, "sort": "updated"}
        )

        if not success:
            _log(logger, "warning", f"Could not fetch issues: {error}")
            return []

        issues = []
        for issue in data:
            if issue.get("pull_request"):  # Skip pull requests
                continue

            issues.append({
                "number": issue.get("number"),
                "title": issue.get("title"),
                "body": (issue.get("body") or "")[:2000],  # Limit body length
                "state": issue.get("state"),
                "created_at": issue.get("created_at"),
                "updated_at": issue.get("updated_at"),
                "closed_at": issue.get("closed_at"),
                "labels": [label.get("name") for label in issue.get("labels", [])],
                "comments": issue.get("comments", 0),
                "user": issue.get("user", {}).get("login"),
                "url": issue.get("html_url")
            })

        _log(logger, "info", f"Collected {len(issues)} issues")
        return issues

    # ═══════════════════════════════════════════════════════════════════════
    # RELEASES (EXISTING)
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _get_releases(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger,
        max_releases: int = 20
    ) -> List[Dict[str, Any]]:
        """Get repository releases."""
        success, data, error = await GithubTask._github_api_request(
            client, f"repos/{owner}/{repo}/releases",
            headers,
            {"per_page": max_releases}
        )

        if not success:
            _log(logger, "warning", f"Could not fetch releases: {error}")
            return []

        releases = []
        for release in data:
            releases.append({
                "tag_name": release.get("tag_name"),
                "name": release.get("name"),
                "body": (release.get("body") or "")[:2000],  # Limit body length
                "created_at": release.get("created_at"),
                "published_at": release.get("published_at"),
                "prerelease": release.get("prerelease"),
                "draft": release.get("draft"),
                "url": release.get("html_url")
            })

        _log(logger, "info", f"Collected {len(releases)} releases")
        return releases

    # ═══════════════════════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def run(browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Main entry point for RAG-optimized GitHub repository analysis.

        Parameters:
        - url: GitHub repository URL (required)
        - github_token: Optional GitHub API token for higher rate limits
        - max_files: Maximum number of files to collect (default: 500)
        - max_issues: Maximum number of issues to collect (default: 50)
        - max_prs: Maximum number of pull requests (default: 50)
        - max_releases: Maximum number of releases (default: 20)
        - max_contributors: Maximum number of contributors (default: 30)

        Output Structure:
        - files/: Actual code files organized by directory
        - metadata/: Per-file metadata JSON
        - pull_requests.json: PR discussions and context
        - contributors.json: Contributor insights
        - dependencies.json: Dependency tree
        - code_structure.json: Structure analysis
        - health_metrics.json: Repository health
        - issues.json: Issues data
        - releases.json: Release information
        - repository_summary.json: Overall summary
        """

        # Validate parameters
        url = params.get("url", "").strip()
        if not url:
            raise ValueError("GitHub repository URL is required")

        owner, repo = GithubTask._parse_github_url(url)
        if not owner or not repo:
            raise ValueError(f"Invalid GitHub URL: {url}")

        # Configuration
        github_token = params.get("github_token")
        max_files = min(1000, max(10, int(params.get("max_files", 500))))
        max_issues = min(100, max(10, int(params.get("max_issues", 50))))
        max_prs = min(100, max(10, int(params.get("max_prs", 50))))
        max_releases = min(50, max(5, int(params.get("max_releases", 20))))
        max_contributors = min(50, max(5, int(params.get("max_contributors", 30))))

        # Setup headers
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Repo-Analyzer-RAG"
        }
        if github_token:
            headers["Authorization"] = f"token {github_token}"

        # Setup output directory
        output_dir = pathlib.Path(job_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        (output_dir / "files").mkdir(exist_ok=True)
        (output_dir / "metadata").mkdir(exist_ok=True)

        _log(logger, "info", f"Starting RAG-optimized GitHub analysis: {owner}/{repo}")
        _log(logger, "info", f"Max files: {max_files}, Max PRs: {max_prs}, Max issues: {max_issues}")

        timeout = httpx.Timeout(30.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:

            # Collect all data
            _log(logger, "info", "Fetching repository metadata...")
            metadata = await GithubTask._get_repo_metadata(client, owner, repo, headers, logger)

            if "error" in metadata:
                raise ValueError(f"Repository access failed: {metadata['error']}")

            _log(logger, "info", "Collecting repository files with smart prioritization...")
            files, code_structure = await GithubTask._get_repo_contents(
                client, owner, repo, headers, logger, max_files, output_dir
            )

            # Save per-file metadata
            _log(logger, "info", "Generating per-file metadata...")
            for file_data in files:
                filename = file_data.get("path", "").replace("/", "_").replace(".", "_")
                meta_file = output_dir / "metadata" / f"{filename}_meta.json"
                try:
                    meta_file.write_text(
                        json.dumps({
                            "path": file_data.get("path"),
                            "filename": file_data.get("filename"),
                            "language": file_data.get("language"),
                            "size_bytes": file_data.get("size_bytes"),
                            "lines_of_code": file_data.get("lines_of_code"),
                            "complexity": file_data.get("complexity"),
                            "category": file_data.get("category"),
                            "imports": file_data.get("imports"),
                            "priority": file_data.get("priority")
                        }, indent=2),
                        encoding='utf-8'
                    )
                except Exception as e:
                    _log(logger, "debug", f"Could not save metadata for {file_data.get('path')}: {e}")

            _log(logger, "info", "Fetching pull requests...")
            prs = await GithubTask._get_pull_requests(client, owner, repo, headers, logger, max_prs)

            _log(logger, "info", "Fetching contributors and community data...")
            contributors_data = await GithubTask._get_contributors(client, owner, repo, headers, logger, max_contributors)

            _log(logger, "info", "Extracting dependencies...")
            dependencies = await GithubTask._extract_dependencies(client, owner, repo, headers, logger)

            _log(logger, "info", "Fetching issues...")
            issues = await GithubTask._get_issues(client, owner, repo, headers, logger, max_issues)

            _log(logger, "info", "Fetching releases...")
            releases = await GithubTask._get_releases(client, owner, repo, headers, logger, max_releases)

            _log(logger, "info", "Calculating repository health metrics...")
            health_metrics = GithubTask._calculate_health_metrics(
                metadata, files, prs, contributors_data, dependencies
            )

            # Save individual data files
            _log(logger, "info", "Saving structured output files...")

            (output_dir / "code_structure.json").write_text(
                json.dumps(code_structure, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            (output_dir / "pull_requests.json").write_text(
                json.dumps(prs, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            (output_dir / "contributors.json").write_text(
                json.dumps(contributors_data, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            (output_dir / "dependencies.json").write_text(
                json.dumps(dependencies, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            (output_dir / "health_metrics.json").write_text(
                json.dumps(health_metrics, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            if issues:
                (output_dir / "issues.json").write_text(
                    json.dumps(issues, indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )

            if releases:
                (output_dir / "releases.json").write_text(
                    json.dumps(releases, indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )

            # Create repository summary
            summary = {
                "repository": {
                    "owner": owner,
                    "name": repo,
                    "url": url,
                    **metadata
                },
                "collection_summary": {
                    "total_files": len(files),
                    "total_prs": len(prs),
                    "total_issues": len(issues),
                    "total_releases": len(releases),
                    "total_contributors": len(contributors_data.get("contributors", [])),
                    "total_dependencies": dependencies.get("total_dependencies", 0),
                    "files_by_priority": {
                        "critical": len([f for f in files if f.get("priority") == "critical"]),
                        "high": len([f for f in files if f.get("priority") == "high"]),
                        "medium": len([f for f in files if f.get("priority") == "medium"]),
                        "low": len([f for f in files if f.get("priority") == "low"])
                    }
                },
                "code_structure": code_structure,
                "health_metrics": health_metrics,
                "collected_at": datetime.utcnow().isoformat() + "Z"
            }

            (output_dir / "repository_summary.json").write_text(
                json.dumps(summary, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            _log(logger, "info", f"✅ Analysis complete: {len(files)} files, {len(prs)} PRs, {len(issues)} issues, {len(releases)} releases")
            _log(logger, "info", f"Repository health: {health_metrics['health_rating']} ({health_metrics['overall_health_score']})")

            return {
                "repository": f"{owner}/{repo}",
                "url": url,
                "total_files": len(files),
                "total_prs": len(prs),
                "total_issues": len(issues),
                "total_releases": len(releases),
                "total_contributors": len(contributors_data.get("contributors", [])),
                "primary_language": code_structure.get("primary_language"),
                "health_rating": health_metrics["health_rating"],
                "health_score": health_metrics["overall_health_score"],
                "output_files": {
                    "summary": "repository_summary.json",
                    "code_structure": "code_structure.json",
                    "pull_requests": "pull_requests.json" if prs else None,
                    "contributors": "contributors.json",
                    "dependencies": "dependencies.json",
                    "health_metrics": "health_metrics.json",
                    "issues": "issues.json" if issues else None,
                    "releases": "releases.json" if releases else None,
                    "files_directory": "files/",
                    "metadata_directory": "metadata/"
                }
            }
