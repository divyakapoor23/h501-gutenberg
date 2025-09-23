import pandas as pd
from typing import Dict

_CACHE: Dict[str, pd.DataFrame] = {}


def read_gutenberg_tables(reload: bool = False, verbose: bool = False) -> Dict[str, pd.DataFrame]:
    """Read (and optionally cache) the Gutenberg tidyTuesday tables.

    Parameters
    - reload: if True, always re-download the CSVs; otherwise return cached data if present.
    - verbose: if True, print success/error messages while loading.

    Returns a dict with keys: 'gutenberg_authors', 'gutenberg_languages', 'gutenberg_metadata', 'gutenberg_subjects'.
    """
    global _CACHE
    if _CACHE and not reload:
        if verbose:
            print("Using cached Gutenberg datasets.")
        return _CACHE

    urls = {
        "gutenberg_authors": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_authors.csv",
        "gutenberg_languages": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_languages.csv",
        "gutenberg_metadata": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_metadata.csv",
        "gutenberg_subjects": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_subjects.csv"
    }

    data: Dict[str, pd.DataFrame] = {}
    for key, url in urls.items():
        try:
            df = pd.read_csv(url)
            data[key] = df
            if verbose:
                print(f"Successfully loaded {key} dataset.")
        except Exception as e:
            if verbose:
                print(f"Error loading {key} dataset from {url}: {e}")
            data[key] = pd.DataFrame()

    _CACHE = data
    return data
