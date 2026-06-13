"""
arXiv API ingester.

arXiv is the primary repository for AI/ML research papers.
We use the official `arxiv` Python library which wraps their Atom feed API.

Why the official library instead of raw HTTP?
It handles XML parsing, pagination, and rate limiting for us.
The arXiv API requires a 3-second delay between requests by their ToS —
the library enforces this automatically.

API docs: https://info.arxiv.org/help/api/index.html
"""

import arxiv
from loguru import logger
from ingestion.base import BaseIngester

# These are arXiv subject classifications for AI/ML
# cs.AI = Artificial Intelligence
# cs.LG = Machine Learning
# cs.CL = Computation and Language (NLP)
# cs.CV = Computer Vision
CATEGORIES = ["cs.AI", "cs.LG", "cs.CL", "cs.CV"]
MAX_RESULTS_PER_CATEGORY = 100


class ArxivIngester(BaseIngester):

    def __init__(self, raw_base_path: str):
        super().__init__("arxiv", raw_base_path)
        # ArXiv client — max_retries handles transient API failures
        self.client = arxiv.Client(page_size=100, delay_seconds=3, num_retries=3)

    def fetch(self) -> list[dict]:
        all_papers = []
        seen_ids = set()

        for category in CATEGORIES:
            logger.info(f"Fetching arXiv papers: category={category}")
            try:
                search = arxiv.Search(
                    query=f"cat:{category}",
                    max_results=MAX_RESULTS_PER_CATEGORY,
                    sort_by=arxiv.SortCriterion.SubmittedDate,
                    sort_order=arxiv.SortOrder.Descending,
                )

                for paper in self.client.results(search):
                    paper_id = paper.entry_id
                    if paper_id in seen_ids:
                        continue
                    seen_ids.add(paper_id)
                    all_papers.append(self._extract_fields(paper, category))

            except Exception as e:
                logger.error(f"Failed category='{category}': {e}")

        logger.info(f"arXiv total unique papers fetched: {len(all_papers)}")
        return all_papers

    def _extract_fields(self, paper: arxiv.Result, primary_category: str) -> dict:
        """
        Extract paper metadata into a flat dict.

        Why flatten authors into a list of strings?
        Author objects have name, affiliation, etc. For our analytics
        we only need names. Keeping objects would complicate the Spark schema.
        Spark handles arrays of strings natively with explode().
        """
        return {
            "paper_id": paper.entry_id,
            "title": paper.title,
            "summary": paper.summary[:500] if paper.summary else None,  # truncate long abstracts
            "authors": [a.name for a in paper.authors],
            "primary_category": primary_category,
            "categories": paper.categories,
            "published": paper.published.isoformat() if paper.published else None,
            "updated": paper.updated.isoformat() if paper.updated else None,
            "doi": paper.doi,
            "pdf_url": paper.pdf_url,
            "comment": paper.comment,
        }
