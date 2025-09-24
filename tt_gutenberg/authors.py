import re
from typing import List, Dict, Optional
from tt_gutenberg.utils import read_gutenberg_tables
import pandas as pd

"""A helper function for working with the Gutenberg tidyTuesday datasets.
"""
def _split_aliases(s: str) -> List[str]:
    # Split a string of author aliases into individual aliases.
    if not isinstance(s, str):
        return []
    s = s.strip()
    # remove surrounding brackets/quotes
    s = re.sub(r"^[\[\(\"]+|[\]\)\"]+$", "", s)
    # split on common separators and the word 'and'
    parts = re.split(r"[,;|/\\]|\band\b", s, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p and re.search(r"[A-Za-z]", p)]

    """
    Split a raw alias string into parts using common separators.
    """
def _clean_alias(a: str, author_name: Optional[str]) -> Optional[str]:
    if not isinstance(a, str):
        return None
    # remove surrounding brackets/quotes and trim whitespace
    a = re.sub(r"^[\[\(\"]+|[\]\)\"]+$", "", a.strip()).strip()
    if not a or a.lower() in {"none", "nan"}:
        return None
    if isinstance(author_name, str) and a.strip().lower() == author_name.strip().lower():
        return None
    # reject obvious junk (urls, emails, too few letters)
    if a.lower().startswith(("http://", "https://")) or "@" in a:
        return None
    if len(re.sub(r"[^A-Za-z]", "", a)) < 2:
        return None
    return a

    """
    Summarize the author's translation statistics.
    """
def get_author_translation_stats(metadata: Optional[pd.DataFrame] = None,
                                 authors: Optional[pd.DataFrame] = None,
                                 languages: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Return a DataFrame with one row per author and their translation count and birth_century.

    All parameters are optional; when omitted the function will load datasets using
    `tt_gutenberg.utils.read_gutenberg_tables()` so that this module contains only function
    definitions (no top-level side-effects) and works nicely in a notebook with reload().
    """
    # Load datasets if not provided
    ds = None
    # Check for required columns
    if metadata is None or authors is None or languages is None:
        ds = read_gutenberg_tables()
    metadata = metadata if metadata is not None else ds.get("gutenberg_metadata")
    authors = authors if authors is not None else ds.get("gutenberg_authors")
    languages = languages if languages is not None else ds.get("gutenberg_languages")

    # normalize column names we rely on
    meta = metadata.copy() if metadata is not None else pd.DataFrame()
    auth = authors.copy() if authors is not None else pd.DataFrame()
    #   Rename columns for consistency
    if "gutenberg_author_id" in meta.columns:
        meta = meta.rename(columns={"gutenberg_author_id": "author_id"})
    if "gutenberg_author_id" in auth.columns or "author" in auth.columns:
        auth = auth.rename(columns={"gutenberg_author_id": "author_id", "author": "author_name"})

    # Determine the correct gutenberg_id column name
    gid = "gutenberg_id" if "gutenberg_id" in meta.columns else None
    # Count unique translation languages per author if possible 
    if gid and gid in languages.columns and "author_id" in meta.columns:
        tmp = meta[[gid, "author_id"]].dropna().merge(
            languages[[gid, "language"]].drop_duplicates(), on=gid, how="left")
        author_langs = tmp.groupby("author_id")["language"].nunique().reset_index(name="trans_count")
    elif "author_id" in meta.columns:
        author_langs = meta.groupby("author_id").size().reset_index(name="trans_count")
    else:
        author_langs = pd.DataFrame({"author_id": auth.get("author_id", pd.Series(dtype="Int64")), "trans_count": 0})
    # Merge authors with their translation counts
    df = auth.merge(author_langs, on="author_id", how="left")
    df["trans_count"] = pd.to_numeric(df.get("trans_count", 0), errors="coerce").fillna(0)
    df["birthdate"] = pd.to_numeric(df.get("birthdate"), errors="coerce")
    # keep authors with a birthdate so plotting by birth_century works
    df = df.dropna(subset=["birthdate"]).copy()
    df["birth_century"] = (df["birthdate"].astype(int) // 100) * 100
    return df

def list_authors(by_languages: bool = True, alias: bool = True) -> List[str]:
    """Return aliases ordered by translation count (descending).

    The function will return an empty list if alias columns aren't present or on error.
    """
    if not alias:
        return []
    from tt_gutenberg.utils import read_gutenberg_tables
    # Load datasets
    try:
        ds = read_gutenberg_tables()
    except Exception as e:
        print(f"Error loading datasets: {e}")
        return []
    # Check for required columns
    authors = ds.get("gutenberg_authors", pd.DataFrame())
    if authors is None or authors.empty:
        return []
    # Get author translation stats
    stats = get_author_translation_stats(metadata=ds.get("gutenberg_metadata"),
                                         authors=authors,
                                         languages=ds.get("gutenberg_languages"))
    # Check for alias columns
    alias_col = "alias" if "alias" in authors.columns else ("aliases" if "aliases" in authors.columns else None)
    if not alias_col:
        return []
    # Merge author stats with aliases
    merged = authors.rename(columns={"gutenberg_author_id": "author_id", "author": "author_name"}).merge(
        stats[["author_id", "trans_count"]], on="author_id", how="left")
    merged["trans_count"] = merged["trans_count"].fillna(0)
    # Aggregate and rank aliases by max translation count
    alias_scores: Dict[str, int] = {}
    for _, r in merged.iterrows():
        raw = r.get(alias_col)
        name = r.get("author_name", "")
        parts = _split_aliases(raw) if isinstance(raw, str) else []
        if not parts and isinstance(raw, str):
            parts = [raw]
            # handle single alias without separators
        for p in parts:
            a = _clean_alias(p, name)
            if a:
                alias_scores[a] = max(alias_scores.get(a, 0), int(r.get("trans_count", 0) or 0))
    # Remove duplicates, sort and return
    return [a for a, _ in sorted(alias_scores.items(), key=lambda x: (-x[1], x[0].lower()))]
