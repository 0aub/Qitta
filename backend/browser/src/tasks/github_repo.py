"""
GitHubRepoTask - Comprehensive GitHub repository analyzer.

Collects code, issues, releases, documentation, and repository metadata 
for creating a complete knowledge base of a GitHub repository.
"""

import asyncio
import logging
import json
import urllib.parse
import pathlib
from typing import Any, Dict, List, Optional, Tuple
import httpx

from .base import _log


class GitHubRepoTask:
    """
    Comprehensive GitHub repository analyzer that collects code, issues, releases, documentation,
    and repository metadata for creating a complete knowledge base of a GitHub repository.
    Uses the official GitHub API for efficient and reliable data collection.
    """
    
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
    
    @staticmethod
    async def _github_api_request(
        client: httpx.AsyncClient,
        endpoint: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
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
    
    @staticmethod
    async def _get_repo_metadata(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Get basic repository metadata."""
        success, data, error = await GitHubRepoTask._github_api_request(
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
            "open_issues": data.get("open_issues_count", 0),
            "created_at": data.get("created_at"),
            "updated_at": data.get("updated_at"),
            "clone_url": data.get("clone_url"),
            "homepage": data.get("homepage"),
            "topics": data.get("topics", []),
            "license": data.get("license", {}).get("name") if data.get("license") else None
        }
    
    @staticmethod
    async def _get_repo_contents(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger,
        max_files: int = 100
    ) -> List[Dict[str, Any]]:
        """Get repository file contents (focusing on important files)."""
        important_files = [
            "README.md", "README.rst", "README.txt", "README",
            "CHANGELOG.md", "CHANGELOG.rst", "CHANGELOG.txt", "CHANGELOG",
            "LICENSE", "LICENSE.md", "LICENSE.txt",
            "CONTRIBUTING.md", "CONTRIBUTING.rst", "CONTRIBUTING.txt",
            "CODE_OF_CONDUCT.md", "SECURITY.md",
            "package.json", "requirements.txt", "Pipfile", "pyproject.toml",
            "Cargo.toml", "go.mod", "pom.xml", "build.gradle"
        ]
        
        files_collected = []
        
        # Get repository tree
        success, tree_data, error = await GitHubRepoTask._github_api_request(
            client, f"repos/{owner}/{repo}/git/trees/HEAD?recursive=1", headers
        )
        
        if not success:
            _log(logger, "warning", f"Could not fetch repo tree: {error}")
            return []
        
        # Priority 1: Important files
        for file_info in tree_data.get("tree", []):
            if file_info.get("type") != "blob":
                continue
                
            path = file_info.get("path", "")
            filename = path.split("/")[-1]
            
            if filename in important_files:
                content = await GitHubRepoTask._get_file_content(
                    client, owner, repo, path, headers, logger
                )
                if content:
                    files_collected.append({
                        "path": path,
                        "filename": filename,
                        "content": content,
                        "priority": "high"
                    })
        
        # Priority 2: Code files (if we haven't hit the limit)
        if len(files_collected) < max_files:
            for file_info in tree_data.get("tree", []):
                if len(files_collected) >= max_files:
                    break
                    
                if file_info.get("type") != "blob":
                    continue
                    
                path = file_info.get("path", "")
                filename = path.split("/")[-1]
                
                # Skip if already collected
                if any(f["path"] == path for f in files_collected):
                    continue
                
                # Focus on main code files
                if (path.endswith((".py", ".js", ".ts", ".java", ".cpp", ".c", ".go", ".rs", ".php")) 
                    and "/" not in path):  # Root level files only
                    
                    content = await GitHubRepoTask._get_file_content(
                        client, owner, repo, path, headers, logger
                    )
                    if content:
                        files_collected.append({
                            "path": path,
                            "filename": filename,
                            "content": content,
                            "priority": "medium"
                        })
        
        _log(logger, "info", f"Collected {len(files_collected)} repository files")
        return files_collected
    
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
            success, data, error = await GitHubRepoTask._github_api_request(
                client, f"repos/{owner}/{repo}/contents/{path}", headers
            )
            
            if not success:
                return None
            
            # Decode base64 content
            import base64
            content = base64.b64decode(data.get("content", "")).decode("utf-8", errors="ignore")
            
            # Limit file size
            if len(content) > 50000:  # 50KB limit
                content = content[:50000] + "\n... (truncated)"
            
            return content
            
        except Exception as e:
            _log(logger, "debug", f"Could not read file {path}: {e}")
            return None
    
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
        success, data, error = await GitHubRepoTask._github_api_request(
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
                "body": issue.get("body", "")[:2000],  # Limit body length
                "state": issue.get("state"),
                "created_at": issue.get("created_at"),
                "updated_at": issue.get("updated_at"),
                "labels": [label.get("name") for label in issue.get("labels", [])],
                "url": issue.get("html_url")
            })
        
        _log(logger, "info", f"Collected {len(issues)} issues")
        return issues
    
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
        success, data, error = await GitHubRepoTask._github_api_request(
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
                "body": release.get("body", "")[:2000],  # Limit body length
                "created_at": release.get("created_at"),
                "published_at": release.get("published_at"),
                "prerelease": release.get("prerelease"),
                "url": release.get("html_url")
            })
        
        _log(logger, "info", f"Collected {len(releases)} releases")
        return releases
    
    @staticmethod
    async def run(browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Main entry point for GitHub repository analysis.
        
        Parameters:
        - url: GitHub repository URL
        - github_token: Optional GitHub API token for higher rate limits
        - max_files: Maximum number of files to collect (default: 100)
        - max_issues: Maximum number of issues to collect (default: 50)
        - max_releases: Maximum number of releases to collect (default: 20)
        """
        
        # Validate parameters
        url = params.get("url", "").strip()
        if not url:
            raise ValueError("GitHub repository URL is required")
        
        owner, repo = GitHubRepoTask._parse_github_url(url)
        if not owner or not repo:
            raise ValueError(f"Invalid GitHub URL: {url}")
        
        # Configuration
        github_token = params.get("github_token")
        max_files = min(200, max(10, int(params.get("max_files", 100))))
        max_issues = min(100, max(10, int(params.get("max_issues", 50))))
        max_releases = min(50, max(5, int(params.get("max_releases", 20))))
        
        # Setup headers
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Repo-Analyzer"
        }
        if github_token:
            headers["Authorization"] = f"token {github_token}"
        
        # Setup output directory
        output_dir = pathlib.Path(job_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        _log(logger, "info", f"Analyzing GitHub repository: {owner}/{repo}")
        
        timeout = httpx.Timeout(30.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            
            # Collect all data in parallel where possible
            tasks = [
                GitHubRepoTask._get_repo_metadata(client, owner, repo, headers, logger),
                GitHubRepoTask._get_repo_contents(client, owner, repo, headers, logger, max_files),
                GitHubRepoTask._get_issues(client, owner, repo, headers, logger, max_issues),
                GitHubRepoTask._get_releases(client, owner, repo, headers, logger, max_releases)
            ]
            
            try:
                metadata, files, issues, releases = await asyncio.gather(*tasks)
            except Exception as e:
                _log(logger, "error", f"Failed to collect repository data: {e}")
                raise
            
            # Check for API errors
            if isinstance(metadata, dict) and "error" in metadata:
                raise ValueError(f"Repository access failed: {metadata['error']}")
            
            # Organize collected data
            result_data = {
                "repository": {
                    "owner": owner,
                    "name": repo,
                    "url": url,
                    **metadata
                },
                "files": files,
                "issues": issues,
                "releases": releases,
                "collection_summary": {
                    "total_files": len(files),
                    "total_issues": len(issues),
                    "total_releases": len(releases),
                    "files_by_priority": {
                        "high": len([f for f in files if f.get("priority") == "high"]),
                        "medium": len([f for f in files if f.get("priority") == "medium"]),
                        "low": len([f for f in files if f.get("priority") == "low"])
                    }
                }
            }
            
            # Save comprehensive data
            data_file = output_dir / "repository_data.json"
            data_file.write_text(
                json.dumps(result_data, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            
            # Save separate files for easy access
            if files:
                (output_dir / "files.json").write_text(
                    json.dumps(files, indent=2, ensure_ascii=False),
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
            
            _log(logger, "info", f"Repository analysis complete: {len(files)} files, {len(issues)} issues, {len(releases)} releases")
            
            return {
                "repository": f"{owner}/{repo}",
                "url": url,
                "total_files": len(files),
                "total_issues": len(issues),
                "total_releases": len(releases),
                "stars": metadata.get("stars", 0),
                "language": metadata.get("language"),
                "data_file": "repository_data.json",
                "files_file": "files.json" if files else None,
                "issues_file": "issues.json" if issues else None,
                "releases_file": "releases.json" if releases else None
            }