import requests
import re
import asyncio
import functools

from urllib import parse
from bs4 import BeautifulSoup

from typing import List

RESPONSE_REDIRECTS = [301, 302]
RESPONSE_PAGE = [200]

already_visited = {}
already_visited_lock = asyncio.Lock()

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


async def get_response(url):

    response = requests.get(
        url=url,
        allow_redirects=False,
    )
        
    return response


async def is_spam(content: str, spam_link_domains: List[str], redirectionDepth: int):
    
    next_urls = [detect_url_in_content(content)]
    visited = {}
        
    while redirectionDepth >= 0:
        futures = []

        for next_url in next_urls:
            in_spam = check_url_in_domains(next_url, spam_link_domains)
            if in_spam:
                return True

            if next_url in visited:
                futures.append(asyncio.ensure_future(lambda: visited[next_url]))
            else:            
                futures.append(asyncio.ensure_future(get_response(next_url)))

        responses = await asyncio.gather(*futures)
        for response in responses:
            try:
                response.raise_for_status()
                next_urls = get_next_urls(response)
            except:
                continue

            visited[response.request.url] = response

        redirectionDepth -= 1

        if not next_url:
            break
    
    return False


def main():
    loop = asyncio.get_event_loop()    

    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["bit.ly"], 1))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["unurl.kr"], 1))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["tvtv24.com"], 1))) # false

    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["bit.ly"], 2))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["unurl.kr"], 2))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["tvtv24.com"], 2))) # false

main()








