from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

def extract_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        full_url = urljoin(base_url, href)  # Обработка относительных ссылок
        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            links.add(full_url)
    return links
