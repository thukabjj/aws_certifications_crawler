import os
import requests
from tqdm import tqdm
from PyPDF2 import PdfMerger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

# Configuration
BASE_URL = "https://docs.aws.amazon.com/"
OUTPUT_DIR = "aws_docs"
PRODUCT_DIR = os.path.join(OUTPUT_DIR, "products")
CHROME_DRIVER_PATH = "/opt/homebrew/bin/chromedriver"
MAX_DEPTH = 2
MAX_THREADS = 5

AWS_PRODUCTS = [
    "ec2",
    "lambda",
    "s3",
    "ebs"
    "vpc",
    "route53",
    "rds",
    "dynamodb",
    "iam",
    "kms",
    "kinesis",
    "cloudwatch",
    "elb",
    "cloudtrail",
    "autoscaling",
    "trusted-advisor",
    "config",
    "wellarchitected",
    "cloudfront",
    "api-gateway",
    "sqs",
    "sns",
    "eventbridge",
    "codepipeline",
    "codebuild",
    "codedeploy",
    "systems-manager",
    "aws-backup",
    "aws-organizations",
    "transit-gateway",
    "step-functions",
    "cloudformation",

]

# Thread-safe data structures
downloaded_files = {}
aggregation_status = {}
lock = Lock()

def initialize_driver():
    """Initialize Selenium WebDriver."""
    service = Service(CHROME_DRIVER_PATH)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    return webdriver.Chrome(service=service, options=options)

def fetch_related_links(driver, url, depth=0):
    """Fetch all related resource links from the product page."""
    if depth > MAX_DEPTH:
        return []

    print(f"Fetching related links from: {url} (Depth: {depth})")
    driver.get(url)
    time.sleep(3)

    related_links = []

    try:
        elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/latest/')]")
        for element in elements:
            related_link = element.get_attribute("href")
            if related_link and related_link.startswith(BASE_URL):
                related_links.append(related_link)
    except Exception as e:
        print(f"Error fetching related links from {url}: {e}")

    return list(set(related_links))

def fetch_pdf_links(driver, url, product_name):
    """Fetch PDF links from a given page."""
    print(f"Fetching PDF links from: {url}")
    driver.get(url)
    time.sleep(3)

    pdf_links = []

    try:
        elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
        for element in elements:
            pdf_link = element.get_attribute("href")
            if pdf_link and pdf_link.endswith(".pdf"):
                filename = os.path.basename(pdf_link)

                # Thread-safe check for already downloaded files
                with lock:
                    if filename in downloaded_files:
                        print(f"Skipping already downloaded file: {filename}")
                        continue

                pdf_links.append(pdf_link)
    except Exception as e:
        print(f"Error fetching PDF links from {url}: {e}")

    return list(set(pdf_links))

def download_pdf(pdf_url, product_name):
    """Download the PDF with progress bar."""
    if not pdf_url:
        return

    product_dir = os.path.join(PRODUCT_DIR, product_name)
    os.makedirs(product_dir, exist_ok=True)
    filename = os.path.join(product_dir, os.path.basename(pdf_url))

    # Thread-safe check for already downloaded files
    with lock:
        if os.path.basename(filename) in downloaded_files:
            print(f"Already exists: {filename}")
            return

    print(f"Downloading: {pdf_url}")
    try:
        response = requests.get(pdf_url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        with open(filename, "wb") as file, tqdm(
            desc=f"Saving {os.path.basename(pdf_url)}",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
                bar.update(len(chunk))

        # Thread-safe addition to the downloaded files map
        with lock:
            downloaded_files[os.path.basename(filename)] = filename

        print(f"Saved: {filename}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {pdf_url}: {e}")

def merge_pdfs(product_name):
    """Merge all PDFs for a product."""
    product_dir = os.path.join(PRODUCT_DIR, product_name)
    output_pdf = os.path.join(PRODUCT_DIR, f"{product_name}.pdf")
    merger = PdfMerger()

    if not os.path.exists(product_dir):
        print(f"No files found for {product_name}. Skipping merge.")
        return

    pdf_files = [os.path.join(product_dir, f) for f in sorted(os.listdir(product_dir)) if f.endswith(".pdf")]

    if not pdf_files:
        print(f"No PDFs found for {product_name}. Skipping merge.")
        return

    print(f"Merging PDFs for {product_name}...")
    for pdf in pdf_files:
        merger.append(pdf)

    merger.write(output_pdf)
    merger.close()
    print(f"Aggregated PDF created: {output_pdf}")

    # Thread-safe update for aggregation status
    with lock:
        aggregation_status[product_name] = True

def summarize_sizes(product_name):
    """Summarize sizes of individual and aggregated PDFs."""
    product_dir = os.path.join(PRODUCT_DIR, product_name)
    pdf_files = [os.path.join(product_dir, f) for f in os.listdir(product_dir) if f.endswith(".pdf")]
    total_size = 0

    print(f"\nSummary for {product_name}:")
    for pdf in pdf_files:
        size = os.path.getsize(pdf) / (1024 * 1024)  # Convert to MB
        total_size += size
        print(f"  - {os.path.basename(pdf)}: {size:.2f} MB")

    print(f"  Total size: {total_size:.2f} MB\n")

def process_product(product):
    """Process a single product."""
    driver = initialize_driver()
    product_url = f"{BASE_URL}{product}/"
    print(f"Processing product: {product} at {product_url}")

    try:
        related_links = fetch_related_links(driver, product_url)
        all_pdf_links = fetch_pdf_links(driver, product_url, product)

        for related_link in related_links:
            pdf_links = fetch_pdf_links(driver, related_link, product)
            all_pdf_links.extend(pdf_links)

        for pdf_url in set(all_pdf_links):
            download_pdf(pdf_url, product)

        # Merge PDFs and summarize
        merge_pdfs(product)
        summarize_sizes(product)
    finally:
        driver.quit()

def main():
    """Main function."""
    print("Starting AWS Documentation Crawler...")
    os.makedirs(PRODUCT_DIR, exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_product, product) for product in AWS_PRODUCTS]
        for future in as_completed(futures):
            future.result()  # Raise any exceptions that occurred

    print("All tasks completed!")

if __name__ == "__main__":
    main()
