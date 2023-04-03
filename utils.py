import csv
import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()

def get_eth_data_from_API():
    
    """
    This function get EtherScan API Ethereum price data using the API key. To generate API key, please visit https://docs.etherscan.io/getting-started/viewing-api-usage-statistics

    Returns:
        data (dict): Dictionary of Ethereum price data
    """

    api_key = os.getenv('API_KEY')

    url = 'https://api.etherscan.io/api'
    params = {
        'module': 'stats',
        'action': 'ethprice',
        'apikey': api_key
    }

    response = requests.get(url, params=params)
    data = response.json()['result']

    return data

def transform_eth_data(dict_data):

    """
    This function converts the dictionary data to a Pandas DataFrame.
    
    Args:
        dict_data (dict): Dictionary of Ethereum price data
    Returns:
        df (pandas.DataFrame): DataFrame of Ethereum price data
    """

    df = pd.DataFrame.from_dict(dict_data, orient='index').T

    return df

def write_eth_data_to_csv(data):

    """
    This function writes the real-time Ethereum price data to a CSV file.
    Args:
        data (dict): Dictionary of Ethereum price data
    """

    output_path = './output/real-time-eth.csv' # Define output path

    fieldnames = ['ethbtc', 'ethbtc_timestamp', 'ethusd', 'ethusd_timestamp']
    df = transform_eth_data(data)[fieldnames]

    with open(output_path, 'a', newline='') as file:
        writer = csv.writer(file)

        if file.tell() == 0: # Write the header row if the file is empty
            writer.writerow(fieldnames)

        for row in df.values: # Write each row of the selected DataFrame to the file
            writer.writerow(row)