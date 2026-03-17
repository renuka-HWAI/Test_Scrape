# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 14:15:43 2026

@author: Sherwin Mathias
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = "https://example.com"

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

title = soup.find("h1").text

with open("output.txt", "a") as f:
    f.write(f"{datetime.now()} - {title}\n")

print("Done:", title)