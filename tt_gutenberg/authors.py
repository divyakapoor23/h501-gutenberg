import re
from typing import List
import pandas as pd
from tt_gutenberg.utils import read_gutenberg_tables


def _split_aliases(s: str) -> List[str]:
    """Split an alias string into candidate items using common separators."""
    if not isinstance(s, str):
        return []
    s = s.strip()
    s = re.sub(r"^[\[\(\"]+|[\]\)\"]+$", "", s)
    parts = re.split(r"[,;|/\\]", s)
    parts = [p.strip() for p in parts if p and isinstance(p, str)]
    final: List[str] = []
    for p in parts:
        if ' and ' in p.lower():
            final += [q.strip() for q in re.split(r"\band\b", p, flags=re.IGNORECASE)]
        else:
            final.append(p)
    return [p for p in final if p]


def _clean_alias(a: str, author_name: str) -> str:
    """Clean and validate an alias candidate; return empty string for junk."""
    if not isinstance(a, str):
        return ''
    a = a.strip()
    a = re.sub(r"^[\[\(\"]+|[\]\)\"]+$", "", a).strip()
    if not a or a.lower() in {'none', 'nan'}:
        return ''
    if isinstance(author_name, str) and a.lower() == author_name.strip().lower():
        return ''
    if not re.search(r"[A-Za-z]", a):
        return ''
    return a


def list_authors(by_languages: bool = True, alias: bool = True) -> List[str]:
    """Return a list of author aliases ordered by translation count (desc).

    Reads datasets via `read_gutenberg_tables()` and normalizes column names
    so code works with either 'gutenberg_author_id' or 'author_id'.
    """
    try:
        datasets = read_gutenberg_tables()
    except Exception as e:
        print(f"Error loading datasets: {e}")
        return []

    authors = datasets.get('gutenberg_authors', pd.DataFrame()).copy()
    metadata = datasets.get('gutenberg_metadata', pd.DataFrame()).copy()
    languages = datasets.get('gutenberg_languages', pd.DataFrame()).copy()

    # Normalize author id/name columns to 'author_id' and 'author_name'
    if 'gutenberg_author_id' in authors.columns:
        authors = authors.rename(columns={'gutenberg_author_id': 'author_id'})
    if 'author' in authors.columns and 'author_name' not in authors.columns:
        authors = authors.rename(columns={'author': 'author_name'})
    if 'gutenberg_author_id' in metadata.columns and 'author_id' not in metadata.columns:
        metadata = metadata.rename(columns={'gutenberg_author_id': 'author_id'})

    # Determine gutenberg id column
    gutenberg_id_col = None
    for c in ['gutenberg_id', 'id']:
        if c in metadata.columns or c in languages.columns:
            gutenberg_id_col = c
            break

    # Build translation count per author
    trans_count = {}
    if gutenberg_id_col and 'language' in languages.columns and gutenberg_id_col in metadata.columns:
        tmp = metadata[[gutenberg_id_col, 'author_id']].dropna()
        tmp = tmp.merge(languages[[gutenberg_id_col, 'language']].drop_duplicates(), on=gutenberg_id_col, how='left')
        for aid, g in tmp.groupby('author_id'):
            langs = set([x for x in g['language'].dropna().astype(str).unique() if x.strip()])
            trans_count[aid] = len(langs) if langs else g[g[gutenberg_id_col].notna()][gutenberg_id_col].nunique()
    elif gutenberg_id_col and gutenberg_id_col in metadata.columns:
        for aid, g in metadata.groupby('author_id'):
            trans_count[aid] = g[gutenberg_id_col].nunique()
    elif 'author_id' in metadata.columns:
        for aid, g in metadata.groupby('author_id'):
            trans_count[aid] = len(g)
    else:
        for _, row in authors.iterrows():
            trans_count[row.get('author_id')] = 0

    # If aliases not requested or none present, return empty list
    alias_col = None
    for c in ['alias', 'aliases']:
        if c in authors.columns:
            alias_col = c
            break
    if not alias or alias_col is None:
        return []

    # Collect cleaned aliases and map to counts
    alias_to_count = {}
    name_col = 'author_name' if 'author_name' in authors.columns else None
    for _, row in authors.iterrows():
        aid = row.get('author_id')
        if pd.isna(aid):
            continue
        raw = row.get(alias_col)
        author_name = row.get(name_col) if name_col else ''
        parts = _split_aliases(raw) if isinstance(raw, str) else []
        cleaned = []
        for p in parts:
            c = _clean_alias(p, author_name)
            if c:
                cleaned.append(c)
        if not cleaned and isinstance(raw, str):
            c = _clean_alias(raw, author_name)
            if c:
                cleaned = [c]

        count = trans_count.get(aid, 0)
        for a in cleaned:
            alias_to_count[a] = max(alias_to_count.get(a, 0), count)

    items = sorted(alias_to_count.items(), key=lambda x: (-x[1], x[0].lower()))
    return [a for a, _ in items]


def get_author_translation_stats(metadata=None, authors=None, languages=None):
    """Return a DataFrame with one row per author containing:
    - author_id, author_name (if present), birthdate
    - birth_century (int like 1700 for 1753)
    - trans_count (number of unique languages associated with that author's works)

    If metadata/authors/languages are not provided, they will be loaded via
    read_gutenberg_tables(). This keeps the computation colocated in the
    authors module so notebooks can call it instead of duplicating logic.
    """
    # Lazy-load tables if not provided
    if metadata is None or authors is None or languages is None:
        try:
            datasets = read_gutenberg_tables()
        except Exception as e:
            raise RuntimeError(f"Could not load Gutenberg tables: {e}")
        metadata = datasets.get('gutenberg_metadata', metadata)
        authors = datasets.get('gutenberg_authors', authors)
        languages = datasets.get('gutenberg_languages', languages)

    # Work on copies
    meta = metadata.copy() if metadata is not None else pd.DataFrame()
    auth = authors.copy() if authors is not None else pd.DataFrame()
    langs = languages.copy() if languages is not None else pd.DataFrame()

    # Normalize column names
    if 'gutenberg_author_id' in auth.columns:
        auth = auth.rename(columns={'gutenberg_author_id': 'author_id'})
    if 'author' in auth.columns and 'author_name' not in auth.columns:
        auth = auth.rename(columns={'author': 'author_name'})
    if 'gutenberg_author_id' in meta.columns and 'author_id' not in meta.columns:
        meta = meta.rename(columns={'gutenberg_author_id': 'author_id'})

    # Determine gutenberg id column
    gutenberg_id_col = 'gutenberg_id' if 'gutenberg_id' in meta.columns else None

    # Compute trans_count per author
    if gutenberg_id_col and gutenberg_id_col in langs.columns:
        tmp = meta[[gutenberg_id_col, 'author_id']].dropna()
        tmp = tmp.merge(langs[[gutenberg_id_col, 'language']].drop_duplicates(), on=gutenberg_id_col, how='left')
        author_langs = tmp.groupby('author_id')['language'].nunique().reset_index(name='trans_count')
    else:
        # fallback
        if 'author_id' in meta.columns:
            author_langs = meta.groupby('author_id').size().reset_index(name='trans_count')
        else:
            # no metadata: return authors with trans_count=0
            author_langs = pd.DataFrame({'author_id': auth.get('author_id', pd.Series(dtype='int')), 'trans_count': 0})

    # Merge with authors to get birthdate
    if 'author_id' not in auth.columns and 'gutenberg_author_id' in auth.columns:
        auth = auth.rename(columns={'gutenberg_author_id': 'author_id'})

    df = auth.merge(author_langs, on='author_id', how='left')
    df['trans_count'] = pd.to_numeric(df.get('trans_count', pd.Series()), errors='coerce').fillna(0)

    # Coerce birthdate to numeric and compute birth_century
    df['birthdate'] = pd.to_numeric(df.get('birthdate', pd.Series()), errors='coerce')
    df = df.dropna(subset=['birthdate'])
    df['birth_century'] = (df['birthdate'].astype(int) // 100) * 100

    return df[['author_id'] + ([c for c in ['author_name','birthdate','birth_century','trans_count'] if c in df.columns])]
