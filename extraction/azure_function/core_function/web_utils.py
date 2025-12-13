import requests
import random
from bs4 import BeautifulSoup as bs
import logging

logger = logging.getLogger()

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Mozilla/5.0 (X11; Linux x86_64)',
    'Mozilla/5.0'
]

def Generate_Soup(url, max_retries=3, timeout=5):
    """Fetch and parse HTML with retries and exponential backoff."""
    headers = {'user-agent': random.choice(USER_AGENTS)}
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.encoding = 'utf-8'
            response.raise_for_status()
            logger.info(f"Successful request to {url}")
            return bs(response.text, "html.parser"), True
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt}. Retrying...")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None, False
    logger.error(f"Failed to fetch data after {max_retries} attempts")
    return None, False
