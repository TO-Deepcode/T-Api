from typing import Callable, Dict

from bs4 import BeautifulSoup

from . import coindesk, cointelegraph

SITE_EXTRACTORS: Dict[str, Callable[[BeautifulSoup], Dict[str, str]]] = {
    "coindesk": coindesk.adapter,
    "cointelegraph": cointelegraph.adapter,
}
