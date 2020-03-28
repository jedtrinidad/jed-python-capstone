import csv
import requests
import argparse
import logging

from logginator_client import LogginatorClient
# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'
    )


log = logging.getLogger(__name__)
log_url = '''
https://3tdgwj7eog.execute-api.ap-southeast-1.amazonaws.com/beta/logs
'''
client = LogginatorClient()
client.set_url(log_url)
log.addHandler(client)


parser = argparse.ArgumentParser(
    description='Download a csv file and parse it'
    )
parser.add_argument(
    'url',
    help='the url of the csv file')
parser.add_argument(
    '--version', '-v',
    action='version',
    version='%(prog)s 2.1.2'
    )

args = parser.parse_args()


def download_csv_file(url):
    try:
        response = requests.get(url)
        print("Dowloading file...")
        log.info(f"Downloading file from: {url}")
        if response.status_code == 200:
            open('file.csv', 'wb').write(response.content)
        log.info(f'Downloaded: {url}')
    except Exception as e:
        log.critical(f"Error: {e}")


def get_rows(file):
    rows = []
    try:
        with open(file) as csvfile:
            print("Reading File...")
            log.info("Reading File...")
            csvdialect = csv.Sniffer().sniff(
                csvfile.read(1024), delimiters=',')
            csvfile.seek(0)
            csvreader = csv.DictReader(csvfile, dialect=csvdialect)
            for row in csvreader:
                rows.append(row)
    except Exception as e:
        log.critical(f"Error: {e}")

    return rows


def remove_items_without_categories(items):
    clean_items = []
    if len(items) != 0:
        print("Removing Items without categories.")
        for item in items:
            if item['Categories'] == '':
                log.info(f"Removed Item: {item}")
                items.remove(item)
    else:
        log.error('No rows')
    clean_items = items
    return clean_items


def write_items(items):
    print("Writing cleaned items to file: 'new_file.csv'")
    log.info("Writing cleaned items to file: 'new_file.csv'")
    if len(items) != 0:
        with open('new_file.csv', 'w', newline='') as csvfile:
            fields = list(items[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fields)

            writer.writeheader()
            writer.writerows(items)
            log.info(f"Written items to file")
    else:
        log.info('No File Written.')


if __name__ == '__main__':
    url = args.url
    download_csv_file(url)
    filename = 'file.csv'
    items = get_rows(filename)
    cleaned_items = remove_items_without_categories(items)
    write_items(cleaned_items)
