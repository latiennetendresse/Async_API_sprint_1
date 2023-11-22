def es_search_from_size(page_number: int, page_size: int) -> tuple[int, int]:
    """По параметрам страницы возвращает from и size для поиска в Elastic.

    Пагинация с from и size в Elastic даёт просмотреть до 10000 значений:
    https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
    При необходимости уменьшаем параметры, чтобы вписаться в эти ограничения.
    """
    search_from = min((page_number - 1) * page_size, 10000)
    search_size = min(page_size, 10000 - search_from)
    return search_from, search_size
