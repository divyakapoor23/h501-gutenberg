import pandas as pd
from tt_gutenberg.utils import read_gutenberg_tables
# data = read_gutenberg_tables()
# authors_df = data["gutenberg_authors"]
# languages_df = data["gutenberg_languages"]
# metadata_df = data["gutenberg_metadata"]
def list_authors(by_languages=True, alias=False):
    """Lists authors from the Gutenberg dataset.

    Args:
        by_languages (bool): If True, includes the languages spoken by each author.
        alias (bool): If True, includes any aliases for each author.
    Returns:
        pd.DataFrame: A DataFrame containing authors and optionally their languages and aliases.
    """
    # authors = authors_df.copy()
    if not(by_languages and alias):
        return []
        # authors = authors.merge(languages_df, on="author_id", how="left")
        # authors = authors.groupby(["author_id", "author_name"], as_index=False).agg({
        #     "language": lambda x: ', '.join(sorted(set(x.dropna())))
        # })
        
    # if not alias:
    #     authors = authors[["author_id", "author_name"]]
    # return authors
#merge the dataframes to get a comprehensive view of authors, 
# their languages, and aliases if needed

    try:
        datasets = read_gutenberg_tables()
        authors = datasets["gutenberg_authors"]
        languages = datasets["gutenberg_languages"]
        metadata = datasets["gutenberg_metadata"]
    except Exception as e:
        print(f"Error loading datasets: {e}")
        return [pd.DataFrame()]  # Return an empty DataFrame on failure

def get_author_details(author_id):
    """Fetches detailed information about a specific author.

    Args:
        author_id (int): The ID of the author to fetch details for.

    Returns:
        dict: A dictionary containing author details including name, languages, aliases, and book count.
    """
    author_info = authors_df[authors_df["author_id"] == author_id].to_dict(orient="records")
    if not author_info:
        return {}
    author_info = author_info[0]
    
    languages = languages_df[languages_df["author_id"] == author_id]["language"].tolist()
    aliases = metadata_df[metadata_df["author_id"] == author_id]["alias"].dropna().unique().tolist()
    book_count = metadata_df[metadata_df["author_id"] == author_id].shape[0]
    
    author_info.update({
        "languages": languages,
        "aliases": aliases,
        "book_count": book_count
    })
    
    return author_info
