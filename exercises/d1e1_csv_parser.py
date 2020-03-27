import csv


def get_rows(file):
    rows = []
    with open(file) as csvfile:
        print("Reading File...")
        csvdialect = csv.Sniffer().sniff(csvfile.read(1024), delimiters=',')
        csvfile.seek(0)
        csvreader = csv.DictReader(csvfile, dialect=csvdialect)
        for row in csvreader:
            rows.append(row)
    return rows


def remove_items_without_categories(items):
    clean_items = []
    print("Removing Items without categories.")
    for item in items:
        if item['Categories'] == '':
            items.remove(item)
    clean_items = items
    return clean_items


def write_items(items):
    print("Writing cleaned items to file: 'new_file.csv'")
    with open('new_file.csv', 'w', newline='') as csvfile:
        fields = list(items[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(items)


if __name__ == '__main__':
    filename = input("Please enter a filename: ")
    items = get_rows(filename)
    cleaned_items = remove_items_without_categories(items)
    write_items(cleaned_items)
