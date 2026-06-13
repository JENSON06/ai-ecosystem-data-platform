"""
GitHub API ingester.

Searches for AI-related repositories using multiple query terms.
Uses the GitHub Search API v3 — no authentication required but
with a token you get 5000 requests/hour vs 60 without one.

API docs: https://docs.github.com/en/rest/search/search#search-repositories
"""

import os
import time
import requests
from loguru import logger
from ingestion.base import BaseIngester


SEARCH_QUERIES = [
    "machine learning",
    "deep learning",
    "large language model",
    "computer vision",
    "natural language processing",
]

RESULTS_PER_PAGE = 100   # GitHub's maximum per request
PAGES_PER_QUERY = 3      # 3 pages × 100 results × 5 queries = 1500 repos max


class GitHubIngester(BaseIngester):

    def __init__(self, raw_base_path: str):
        super().__init__("github", raw_base_path)
        token = os.getenv("GITHUB_TOKEN")
        # Authorization header doubles your rate limit headroom
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        else:
            logger.warning("GITHUB_TOKEN not set — rate limited to 60 req/hour")

    def fetch(self) -> list[dict]:
        all_repos = []
        seen_ids = set()   # Deduplicate across overlapping search queries

        for query in SEARCH_QUERIES:
            logger.info(f"Searching GitHub: '{query}'")
            for page in range(1, PAGES_PER_QUERY + 1):
                try:
                    data = self._get(
                        "https://api.github.com/search/repositories",
                        params={
                            "q": f"{query} language:python",
                            "sort": "stars",
                            "order": "desc",
                            "per_page": RESULTS_PER_PAGE,
                            "page": page,
                        },
                        headers=self.headers,
                    )
                    items = data.get("items", [])
                    if not items:
                        break

                    for repo in items:
                        if repo["id"] in seen_ids:
                            continue
                        seen_ids.add(repo["id"])
                        all_repos.append(self._extract_fields(repo))

                    # GitHub rate limit: be polite between pages
                    time.sleep(0.5)

                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 401:
                        raise RuntimeError(
                            "GitHub API returned 401 Unauthorized. "
                            "Set a valid GITHUB_TOKEN in your .env file. "
                            "Get one at: https://github.com/settings/tokens"
                        ) from e
                    logger.error(f"Failed query='{query}' page={page}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Failed query='{query}' page={page}: {e}")
                    break

        logger.info(f"GitHub total unique repos fetched: {len(all_repos)}")
        return all_repos

    def _extract_fields(self, repo: dict) -> dict:
        """
        Extract only the fields we need.

        Why not store the entire API response?
        GitHub returns ~80 fields per repo. Most are URLs, internal IDs,
        and permission flags we'll never use. Storing only what we need:
        - Reduces storage cost
        - Speeds up Spark reads (fewer columns to parse)
        - Makes the schema predictable

        This is called "schema-on-write" for the raw layer — we still
        store JSON (flexible) but we've already narrowed the field set.
        """
        return {
            "repo_id": repo.get("id"),
            "name": repo.get("name"),
            "full_name": repo.get("full_name"),
            "description": repo.get("description"),
            "stars": repo.get("stargazers_count", 0),
            "forks": repo.get("forks_count", 0),
            "watchers": repo.get("watchers_count", 0),
            "open_issues": repo.get("open_issues_count", 0),
            "language": repo.get("language"),
            "topics": repo.get("topics", []),
            "owner_login": repo.get("owner", {}).get("login"),
            "owner_type": repo.get("owner", {}).get("type"),
            "created_at": repo.get("created_at"),
            "updated_at": repo.get("updated_at"),
            "pushed_at": repo.get("pushed_at"),
            "size_kb": repo.get("size", 0),
            "is_fork": repo.get("fork", False),
            "license": repo.get("license", {}).get("spdx_id") if repo.get("license") else None,
            "html_url": repo.get("html_url"),
        }
