from typing import Any

def list_authors(*args: Any, **kwargs: Any):
	# Proxy to tt_gutenberg.authors.list_authors to avoid eager imports.
	from .authors import list_authors as _list_authors
	return _list_authors(*args, **kwargs)
