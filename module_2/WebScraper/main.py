"""
Orchestration script for the GradCafe data collection pipeline.

This module coordinates the scraping and cleaning phases, managing the
iteration through 1,200 pages of search results and persisting the
final dataset to a JSON file.
"""

import json
import time
from scrape import scrape_data
from clean import clean_data


def save_data(data, filename="applicant_data.json"):
    """Saves cleaned data into a JSON file.

        Args:
            data: A list of dictionaries containing the parsed applicant records.
            filename: The string path/name of the file to be created.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to {filename}")


def load_data(filename="applicant_data.json"):
    """Loads cleaned data from a JSON file.
        Args:
            filename: The string path/name of the file to read.
        Returns:
            A list of dictionaries if the file exists, otherwise an empty list.
    """
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def main():
    """Executes the primary scraping and data persistence loop.

        This function iterates through the survey results. Each page contains
        25 records; therefore, 1,200 pages are processed to reach the target
        of 30,000 records.
    """
    num_pages = 1200   #25 records per page => 30K records
    all_results = []

    for i in range(1, num_pages + 1):
        # Progress tracking: print status every 10 pages
        if i % 10 == 0:
            print(f"Scraping page {i}...")

        html = scrape_data(i)
        if html:
            page_results = clean_data(html)
            all_results.extend(page_results)

        # Ethical Scraping: Pause for 1 second between requests
        # to respect server resources and prevent IP throttling.
        time.sleep(1)

    # Save gathered data only if the results list is not empty
    if all_results:
        save_data(all_results)


if __name__ == "__main__":
    main()