pip install requests tenacity

#-------script that uses the requests library to download parquet files for each month of the year 2019. The script handles network errors and retries using the tenacity library.------------
# Create a directory to store the downloaded files
output_dir = 'C:/Users/ashki/Downloads/Trip_data_2019'
os.makedirs(output_dir, exist_ok=True)

# Base URL for the parquet files
base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-{}.parquet'

# List of months to download
months = [f'{i:02}' for i in range(1, 13)]

# Retry configuration: 5 attempts with exponential backoff starting at 2 seconds
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=60))
def download_file(url, dest_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an error for bad status codes
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f'Successfully downloaded {dest_path}')

for month in months:
    file_url = base_url.format(month)
    file_name = f'yellow_tripdata_2019-{month}.parquet'
    file_path = os.path.join(output_dir, file_name)

    try:
        download_file(file_url, file_path)
    except Exception as e:
        print(f'Failed to download {file_name}: {e}')
