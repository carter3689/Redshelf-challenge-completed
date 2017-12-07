# Creating ETL Redshelf - Joel Carter
import csv
import json

def create_purchases_etl(**kwargs):
    buckets = []
    purchases = []

    with open('purchase_buckets.csv', newline='') as purchase_buckets:
        purchase_buckets_reader = csv.reader(purchase_buckets, delimiter=',', quotechar='|')
        for row in purchase_buckets_reader:
            buckets.append(row)

    with open('purchase_data.csv', newline='') as purchase_data:
        purchase_data_reader = csv.reader(purchase_data, delimiter=',', quotechar='|')
        for row in purchase_data_reader:
            purchases.append(row)



    purchase_dictionary = {};

    # Create case insensitive keys
    for key in buckets:
        purchase_dictionary[key[0].lower() + "," + key[1].lower() + "," + key[2].lower()] = []

    for purchase in purchases:
        publisher = purchase[2].lower()
        price = purchase[4].lower()
        duration = purchase[5].lower()
        # Bucket Publisher,Price,Duration
        key_to_find = publisher + "," + price + "," + duration
        if key_to_find in purchase_dictionary:
            purchase_dictionary[key_to_find].append(purchase)
        else:
            # Bucket Publisher, *, Duration
            key_to_find = publisher + "," + ",*" + duration
            if key_to_find in purchase_dictionary:
                purchase_dictionary[key_to_find].append(purchase)
            else:
                # Bucket Publisher, price, *
                key_to_find = publisher + "," + price + ",*"
                if key_to_find in purchase_dictionary:
                    purchase_dictionary[key_to_find].append(purchase)
                else:
                    # Bucket *, *, duration
                    key_to_find = "*" + "," + ",*" + "," + duration
                    if key_to_find in purchase_dictionary:
                        purchase_dictionary[key_to_find].append(purchase)
                    else:
                        # Bucket *, price, duration
                        key_to_find = "*" + "," + price + "," + duration
                        if key_to_find in purchase_dictionary:
                            purchase_dictionary[key_to_find].append(purchase)
                        else:
                            # Bucket *,*,*
                            key_to_find = "*" + "," + "*" + "," + "*"
                            if key_to_find in purchase_dictionary:
                                purchase_dictionary[key_to_find].append(purchase)

    # Create Output List to hold data
    output_format = []
    # Through each iteration, add the correct data to the list
    for key, value in purchase_dictionary.items():
        output_format.append({'bucket': key, 'purchases': value})

    # Output a json file
    with open('result.json', 'w') as result_file:
        json.dump(output_format, result_file)


if __name__ == '__main__':
    create_purchases_etl();
