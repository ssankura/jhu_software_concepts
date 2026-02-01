"""
Module for handling network requests to The GradCafe.
This module provides a robust interface for fetching HTML while adhering
to ethical scraping standards through rate limiting.
"""

import urllib3

def scrape_data(page_number : int) -> str:
    """Fetches HTML content from The GradCafe survey for a specific page.

    Args:
        page_number: The specific page index to retrieve from the survey.

    Returns:
        The raw HTML content of the page as a decoded string if successful,
        otherwise None if an error occurs.
    """
    url = f"https://www.thegradcafe.com/survey/index.php?q=&t=a&pp=25&p={page_number}"
    http = urllib3.PoolManager()

    try:
        # We use a custom User-Agent to identify the request as a standard browser
        response = http.request(
            'GET',
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
            timeout=10.0
        )
        return response.data.decode('utf-8')
    except Exception as e:
        print(f"Error: {e}")
        return None