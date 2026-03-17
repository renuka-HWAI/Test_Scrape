import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Use a reliable test website
url = "https://httpbin.org/html"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raises error for bad status codes

    soup = BeautifulSoup(response.text, "html.parser")

    # This page has an <h1> tag
    title = soup.find("h1").text.strip()

    output = f"{datetime.now()} - {title}"

    # Save output
    with open("output.txt", "a") as f:
        f.write(output + "\n")

    print("✅ Scraping successful:")
    print(output)

except requests.exceptions.RequestException as e:
    print("❌ Error occurred:", e)
