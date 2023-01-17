from dagster import asset
from dagster_airbyte import (
    AirbyteManagedElementReconciler,
    airbyte_resource,
    AirbyteConnection,
    AirbyteSyncMode,
    load_assets_from_connections,
)
from dagster_airbyte.managed.generated.sources import GithubSource
from dagster_airbyte.managed.generated.destinations import (
    LocalJsonDestination,
    PostgresDestination,
)
from typing import List
from dagster_dbt import load_assets_from_dbt_project


from bs4 import BeautifulSoup
import os
import requests

import asyncio
import aiohttp

async def get(url, session):
    try:
        # check if status_code is 200
        async with session.get(url) as response:
            if response.status == 200:
                return url
            else:
                return None

    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))
async def check_websites_exists(urls) -> List[str]:
    async with aiohttp.ClientSession(trust_env=True, connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        # get url and sessionm if return is not None
        tasks = [get(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
        results = [result for result in results if result is not None]
    return results

url = "https://github.com/igorbarinov/awesome-data-engineering"
html = requests.get(url)
soup = BeautifulSoup(html.text, "html.parser")
# parse all links into a list starting with github.com
links = [
    link.get("href")
    for link in soup.find_all("a")
    if link.get("href").startswith("https://github.com")
]
# remove links that start with url
links = [
    link
    for link in links
    if not link.startswith(url) and not link.endswith("github.com")
]
# remove last slash if there
links = [link[:-1] if link.endswith("/") else link for link in links]
# remove repos without organization
links = [link for link in links if len(link.split("/")) == 5]
# check if links are still existing in parallel to save time
existings_links = asyncio.run(check_websites_exists(links))
# remove `https://github.com/` from links
links = [link.replace("https://github.com/", "") for link in existings_links]

# due to timeout limits while airbyte is checking each repo, I limited it here to make this demo work for you
links = links[0:10]

# return links as a string with blank space as separator
print(links)