import json
import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import datetime
import re

# 🔹 Logger Configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 🔹 Function to Scrape Website
def scrape_website(url, data_class):
    """Scrapes the given URL for press releases."""
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        extracted_data = []

        if data_class == 'cibc_CSA_data_class':
            # ✅ CSA Scraper
            articles = soup.select("article.listing-item")
            for article in articles:
                title_tag = article.select_one("h2.listing-title a")
                date_tag = article.select_one("div.entry-meta time")

                title = title_tag.text.strip() if title_tag else "No Title"
                link = title_tag["href"] if title_tag and title_tag.has_attr("href") else "No Link"
                date = date_tag["datetime"] if date_tag and date_tag.has_attr("datetime") else "No Date"

                # ✅ Fetch content from href link
                full_content = scrape_article_content(link) if link != "No Link" else "No Content Found"

                extracted_data.append({
                    "title": title,
                    "link": link,
                    "date": date,
                    "content": full_content
                })

            logger.info(f"✅ Scraped {len(extracted_data)} CSA records.")
        elif data_class == 'cibc_FCA_data_class':
            # ✅ FCA Scraper (Updated)
            articles = soup.select("div.content-feed__inner div.item-list ul li")

            if not articles:
                logger.warning("🚨 No FCA articles found. The page structure may have changed.")

            for article in articles:
                title_tag = article.find("a")
                date_text = None

                # ✅ Extract Date from Title Using Regex
                title = title_tag.text.strip() if title_tag else "No Title"
                link = "https://www.fca.org.uk" + title_tag["href"] if title_tag and title_tag.has_attr("href") else "No Link"

                # ✅ Use Regex to Extract Date (Format: DD/MM/YYYY)
                date_match = re.search(r"(\d{2}/\d{2}/\d{4})", title)
                if date_match:
                    date_text = date_match.group(0)

                # ✅ Fetch content from href link
                full_content = scrape_article_content(link) if link != "No Link" else "No Content Found"

                extracted_data.append({
                    "title": title,
                    "link": link,
                    "date": date_text if date_text else "No Date",
                    "content": full_content
                })

            logger.info(f"✅ FCA extracted {len(extracted_data)} records.")


        elif data_class == 'cibc_CFTC_data_class':
            # ✅ CFTC Scraper
            articles = soup.select("div.view-content div.table-responsive table tbody tr")

            for article in articles:
                title_tag = article.select_one("td a")
                date_tag = article.select_one("td:nth-child(1)")  # Assuming first column has the date

                title = title_tag.text.strip() if title_tag else "No Title"
                link = title_tag["href"] if title_tag and title_tag.has_attr("href") else "No Link"
                date = date_tag.text.strip() if date_tag else "No Date"

                if link.startswith("/"):
                    link = "https://www.cftc.gov" + link

                # ✅ Fetch content from href link
                full_content = scrape_article_content(link) if link != "No Link" else "No Content Found"

                extracted_data.append({
                    "title": title,
                    "link": link,
                    "date": date,
                    "content": full_content
                })

            logger.info(f"✅ Scraped {len(extracted_data)} CFTC records.")

        return extracted_data

    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 Error scraping {url}: {e}")
        return None

# 🔹 Function to Scrape Content from Each Article
def scrape_article_content(url):
    """Fetches full article content from the given URL."""
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # 🔎 Identify the website and apply the correct selector
        if "cftc.gov" in url:
            content_tag = soup.select_one("div#content-container section.col-sm-7 div.region div.press-release, div.article-body")

        elif "fca.org.uk" in url:
            content_tag = soup.select_one("div.region-content, div.article-content, div.text-content, div.main-content")

        elif "securities-administrators.ca" in url:
            content_tag = soup.select_one("div.article-content, div.entry-content, div.post-content, div.news-content, section.article-body")

        else:
            content_tag = None  # Default case if the domain is not recognized

        return content_tag.text.strip() if content_tag else "No Content Available"

    except requests.exceptions.RequestException as e:
        logger.error(f"⚠️ Error fetching article {url}: {e}")
        return "Error fetching content"


# 🔹 Function to Save Data to S3
def save_to_s3(bucket_name, file_name, data, tags):
    """Saves JSON data to S3"""
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data, indent=4))
        s3.put_object_tagging(
            Bucket=bucket_name,
            Key=file_name,
            Tagging={'TagSet': tags}
        )
        logger.info(f"✅ Successfully saved {file_name} to S3")
    except (NoCredentialsError, ClientError) as e:
        logger.error(f"🚨 Error saving to S3: {e}")

# 🔹 Lambda Function
def lambda_handler(event, context):
    """Scrapes all sites and stores results in S3 as separate JSON files"""
    websites = [
        {'url': 'https://www.securities-administrators.ca/news/', 'data_class': 'cibc_CSA_data_class', 'tags': [{'Key': 'CSASite', 'Value': 'CSA'}]},
        {'url': 'https://www.fca.org.uk/news', 'data_class': 'cibc_FCA_data_class', 'tags': [{'Key': 'FCASite', 'Value': 'FCA'}]},
        {'url': 'https://www.cftc.gov/PressRoom/PressReleases', 'data_class': 'cibc_CFTC_data_class', 'tags': [{'Key': 'CFTCSite', 'Value': 'CFTC'}]},
    ]

    bucket_name = 'cibcscraperresults'

    for site in websites:
        retries = 3
        for attempt in range(retries):
            logger.info(f"🔍 Scraping data from {site['url']} (Attempt {attempt + 1}/{retries})")
            data = scrape_website(site['url'], site['data_class'])
            
            if data:
                date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                file_name = f"regulatory-scraped-data/{site['data_class']}/{site['data_class']}_{date_str}_data.json"
                save_to_s3(bucket_name, file_name, data, site['tags'])
                break  # Stop retrying if successful

    return {'statusCode': 200, 'body': json.dumps('All data processed successfully!')}
