import requests
import re
import asyncio
from urllib import parse
from bs4 import BeautifulSoup

from typing import List

RESPONSE_REDIRECTS = [301, 302]
RESPONSE_PAGE = [2000]


def detect_url_in_content(content: str):        
    url = re.search(r"(?P<url>https?://[^\s]+)", content).group("url")
    return url 


def detect_url_in_document(content: str):
    soup = BeautifulSoup(content, "html.parser")

    filtered = filter(
        lambda item: item.get("href", None) is not None,
        soup.select("a")
    )

    return map(
        lambda item: item.get("href"),
        filtered
    )

def get_next_urls(response):
    if response.status_code in RESPONSE_REDIRECTS:
        next_urls = [response.headers.get("Location")]
    elif response.status_code in RESPONSE_PAGE:
        next_urls = detect_url_in_document(response.text)
    else:
        raise ValueError

    return next_urls

def check_url_in_domains(url, domains):
    url_parts = parse.urlparse(url)
    return True if url_parts.netloc in domains else False        

def is_spam(content: str, spam_link_domains: List[str], redirectionDepth: int):

    next_urls = [detect_url_in_content(content)]
    
    while redirectionDepth >= 0:
        for next_url in next_urls:
            in_spam = check_url_in_domains(next_url, spam_link_domains)

            if in_spam:
                return True

            response = requests.get(
                url=next_url,
                allow_redirects=False
            )

            try:
                response.raise_for_status()
                next_urls = get_next_urls(response)
            except:
                continue

        redirectionDepth -= 1

        if not next_url:
            break
    
    return False




print(is_spam("spam spam https://unurl.kr/99a7", ["bit.ly"], 1)) # true
print(is_spam("spam spam https://unurl.kr/99a7", ["unurl.kr"], 1)) # true
print(is_spam("spam spam https://unurl.kr/99a7", ["tvtv24.com"], 1)) # false

print(is_spam("spam spam https://unurl.kr/99a7", ["bit.ly"], 2)) # true
print(is_spam("spam spam https://unurl.kr/99a7", ["unurl.kr"], 2)) # true
print(is_spam("spam spam https://unurl.kr/99a7", ["tvtv24.com"], 2)) # false



