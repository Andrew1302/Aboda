from typing import List, Tuple

from fastapi import HTTPException

def paginate(data: List, page: int, page_size: int) -> Tuple[List, int, int]:
    total_items = len(data)
    total_pages = (total_items + page_size - 1) // page_size  # Calcula o número total de páginas
    start = (page - 1) * page_size
    end = start + page_size
    paginated_data = data[start:end]
    if (page>total_pages):
        raise HTTPException(status_code=400, detail=f"Page {page} is off limit, total pages is {total_pages}")
    return paginated_data, page, total_pages