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

def detect_urls_in_content(content: str) -> str:        
    urls = list(re.findall(r"https?://[^\s]+", content))
    return urls


def detect_urls_in_document(document: str, url: str) -> List[str]:
    
    url_parts = parse.urlparse(url)
    soup = BeautifulSoup(document, "html.parser")
    links = list(soup.select("a"))


    # document 내에 a 링크에 대한 처리
    # 상대 적인 좌표의 경우, 현재 url scheme 및 netloc 사용
    urls = []
    for item in links:          
        
        href = item.get("href", None)          
        
        if not href:
            continue
            
        parts = parse.urlparse("{}".format(href))
        if not parts.path:
            continue     

        if parts.scheme not in ['http', 'https']:
            continue   
    
        updated_parts = parse.ParseResult(
            scheme=parts.scheme if parts.scheme else url_parts.scheme,
            netloc=parts.netloc if parts.netloc else url_parts.netloc,
            path=parts.path,
            query=parts.query,
            params=parts.params,
            fragment=parts.fragment
        )
        urls.append(parse.urlunparse(updated_parts))

    return urls


def get_next_urls(response: requests.Response) -> List[str]:
    if response.status_code in RESPONSE_REDIRECTS:
        next_urls = [response.headers.get("Location")]
    elif response.status_code in RESPONSE_PAGE:
        next_urls = detect_urls_in_document(document=response.text, url=response.request.url)
    else:
        next_urls = []

    return next_urls

def check_url_in_domains(url: str, domains: List[str]) -> bool:
    url_parts = parse.urlparse(url)
    return True if url_parts.netloc in domains else False      


async def get_response(url: str) -> requests.Response:

    response = requests.get(
        url=url,
        allow_redirects=False,
    )
        
    return response

async def get_response_cache(url: str, visited) -> requests.Response:
    return visited[url]


async def is_spam(content: str, spam_link_domains: List[str], redirectionDepth: int) -> bool:
    
    next_urls = detect_urls_in_content(content)
    visited = {}
        
    while redirectionDepth >= 0:
        futures = []                

        for next_url in next_urls:            
            in_spam = check_url_in_domains(next_url, spam_link_domains)
            if in_spam:
                return True

            # 기존에 Visited 한 url 은 다시 방문하지 않고, 응답에 대한 cache 사용
            if next_url in visited:
                futures.append(asyncio.ensure_future(get_response_cache(next_url, visited)))
            else:            
                futures.append(asyncio.ensure_future(get_response(next_url)))

        responses = await asyncio.gather(*futures)
        for response in responses:
            try:
                response.raise_for_status()
                next_urls = get_next_urls(response)
            except:
                continue

            # 방문한 페이지에 대해 cache 설정
            visited[response.request.url] = response

        redirectionDepth -= 1

        if not next_url:
            break
    
    return False


def main():
    loop = asyncio.get_event_loop()    

    print(loop.run_until_complete(is_spam("spam spam https://m.gelatofactory.co.kr/goods/event_sale_list.php", ["naver.me"], 1))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["bit.ly"], 1))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["unurl.kr"], 1))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["tvtv24.com"], 1))) # false

    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["bit.ly"], 2))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["unurl.kr"], 2))) # true
    print(loop.run_until_complete(is_spam("spam spam https://unurl.kr/99a7", ["tvtv24.com"], 2))) # true

main()








