import requests
from vars import *


def get_request(curr, date=None):
    # Check if date exists. If not use 'latest' instead.
    date = date if date else 'latest'

    # Construct URL
    url = f"{base_url}{api_endpoint}@{date}/v1/currencies/{curr}.json"

    # Send request
    resp = requests.request("GET", url, verify=False).json()

    return resp


if __name__ == '__main__':
    # Get API data
    response = get_request(curr=base_curr)

    # Clean response data
    response[base_curr] = {val: response[base_curr][val] for val in curr_exchange}

    print(response)
