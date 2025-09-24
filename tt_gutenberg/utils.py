import pandas as pd
from typing import Dict


def read_gutenberg_tables(reload: bool = False, verbose: bool = False) -> Dict[str, pd.DataFrame]:
    """Read (and optionally cache) the Gutenberg tidyTuesday tables.

    Parameters
    - reload: if True, always re-download the CSVs; otherwise return cached data if present.
    - verbose: if True, print success/error messages while loading.

    Returns a dict with keys: 'gutenberg_authors', 'gutenberg_languages', 'gutenberg_metadata', 'gutenberg_subjects'.
    """
    # Use a function attribute for caching to avoid module-level state
    cache = getattr(read_gutenberg_tables, "_cache", None)
    if cache and not reload:
        # Return cached data
        if verbose:
            print("Using cached Gutenberg datasets.")
        return cache
    # URLs for the datasets
    urls = {
        "gutenberg_authors": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_authors.csv",
        "gutenberg_languages": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_languages.csv",
        "gutenberg_metadata": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_metadata.csv",
        "gutenberg_subjects": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_subjects.csv"
    }
    # Dowbload and Load datasets
    data: Dict[str, pd.DataFrame] = {}
    for key, url in urls.items():
        try:
            df = pd.read_csv(url)
            data[key] = df
            # store in dict
            if verbose:
                print(f"Successfully loaded {key} dataset.")
                # Print success message
        except Exception as e:
            # Print error message
            if verbose:
                print(f"Error loading {key} dataset from {url}: {e}")
            data[key] = pd.DataFrame()

    # store cache on function object
    setattr(read_gutenberg_tables, "_cache", data)
    # Return loaded datasets
    return data
