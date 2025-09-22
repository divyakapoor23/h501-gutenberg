import pandas as pd
def read_gutenberg_tables():
    """Reads the raw TidyTuesday Gutenberg dataset from the provided URLs.

    Returns:
        dict: A dictionary containing the three datasets as pandas DataFrames.
              Keys are 'books', 'authors', and 'languages'.
    """
    urls = {
        "gutenberg_authors": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_authors.csv",
        "gutenberg_languages": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_languages.csv",
        "gutenberg_metadata": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_metadata.csv",
        "gutenberg_subjects": "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-06-03/gutenberg_subjects.csv"
    }

    data = {}
    for key, url in urls.items():
        try:
            data[key] = pd.read_csv(url)
            print(f"Successfully loaded {key} dataset.")
        except Exception as e:
            print(f"Error loading {key} dataset from {url}: {e}")
            data[key] = pd.DataFrame()  # Assign an empty DataFrame on failure
    return data
