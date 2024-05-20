import json
from aifc import Error
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

import pymysql
import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes
from sqlalchemy import create_engine, text


def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    instance_connection_name = "gold-bond-417912:asia-east2:bagtracker"
    db_user = "root"
    db_pass = "6CI|iI{p^Xe0/pi<"
    db_name = "bagTracker"

    ip_type = IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    return connector.connect(
        instance_connection_name,
        "pymysql",
        user=db_user,
        password=db_pass,
        db=db_name,
        ip_type=ip_type,
    )


def save_master_to_db(db_conn: pymysql.Connection):
    try:
        with open('master_handbag_data.json') as input_file:
            data = json.load(input_file)

        insert_query = """
            INSERT INTO bagTracker.MasterBag (
                masterBagId, image, brand, model, category, color, material, size, `like`, lowestPrice, highestPrice, fluctuation, volatility, pricePremium
            ) VALUES (
                %(masterBagId)s, %(image)s, %(brand)s, %(model)s, %(category)s, %(color)s, %(material)s, %(size)s, %(like)s, %(lowestPrice)s, %(highestPrice)s, %(fluctuation)s, %(volatility)s, %(pricePremium)s
            )
            ON DUPLICATE KEY UPDATE
                image = %(image)s,
                brand = %(brand)s,
                model = %(model)s,
                category = %(category)s,
                color = %(color)s,
                material = %(material)s,
                size = %(size)s,
                `like` = %(like)s,
                lowestPrice = %(lowestPrice)s,
                highestPrice = %(highestPrice)s,
                fluctuation = %(fluctuation)s,
                volatility = %(volatility)s,
                pricePremium = %(pricePremium)s
        """
        # Execute the insert statements
        for item in data:
            item['lowestPrice'] = Decimal(str(item['lowestPrice']))
            item['highestPrice'] = Decimal(str(item['highestPrice']))
            item['fluctuation'] = Decimal(str(item['fluctuation']))
            item['volatility'] = Decimal(str(item['volatility']))
            item['pricePremium'] = Decimal(str(item['pricePremium']))

        db_conn.cursor().executemany(insert_query, data)

        db_conn.commit()
        print("Data inserted successfully.")
    except Error as e:
        print(f"Error inserting data: {e}")


def save_bag_to_db(db_conn: pymysql.Connection):
    try:
        with open('handbag_data.json') as input_file:
            data = json.load(input_file)

        insert_query = ("INSERT INTO bagTracker.Bag (brand, model, price, color, size, category, material, title, "
                            "`like`, source, url, currency, image, folder, `condition`, bagId, masterBagId) VALUES ("
                            ":brand, :model, :price, :color, :size, :category, :material, :title, :like, :source, "
                            ":url, :currency, :image, :folder, :condition, :bagId, :masterBagId)")
        # Execute the insert statements
        for item in data:
            item['price'] = Decimal(str(item['price']))

        db_conn.cursor().executemany(insert_query, data)

        db_conn.commit()
        print("Data inserted successfully.")
    except Error as e:
        print(f"Error inserting data: {e}")


def save_price_trend_to_db(db_conn: pymysql.Connection):
    try:
        with open('priceTrend_data.json') as input_file:
            data = json.load(input_file)

        insert_query = (
            "INSERT INTO bagTracker.PriceTrend (priceTrendId, date, price, bagId) VALUES (:priceTrendId, "
            ":date, :price, :bagId)")
        # Execute the insert statements
        for item in data:
            date_obj = datetime.strptime(item['date'], "%Y-%m-%d")
            item['date'] = date_obj.date()
            item['price'] = Decimal(str(item['price'])).quantize(Decimal('0.00'), rounding=ROUND_DOWN)
        db_conn.cursor().executemany(insert_query, data)

        db_conn.commit()
        print("Data inserted successfully.")
    except Error as e:
        print(f"Error inserting data: {e}")

def update_collection_by_masterBagId(collection_data, master_data):
    updated_data = []
    for collection in collection_data:
        brand = collection[1].lower()
        model = collection[2].lower()
        category = collection[8].lower()
        size = collection[11]
        color = collection[9]
        material = collection[10]
        collection_id = collection[0]
        masterBag_id = collection[14]
        last_month_avg_price = float(collection[3])
        last_month_fluctuation = float(collection[4])
        purchasedPrice = float(collection[7])
        if size is None:
            size = 'null'
        if material is None:
            material = 'null'
        if color is None:
            color = 'null'
        print(brand, model, category, size, masterBag_id, purchasedPrice)
        candidates = [master for master in master_data if
                      brand == master['brand'].lower() and model == master['model'].lower() and category == master[
                          'category'].lower()]
        if len(candidates) == 0:
            new_masterBag_id = masterBag_id
        else:
            target = next((candidate for candidate in candidates if candidate['size'].lower() == size), None)
            if target:
                new_masterBag_id = target['masterBagId']
            else:
                new_masterBag_id = candidates[0]['masterBagId']
        updated_data.append({
            'collectionId': collection_id,
            'masterBagId': new_masterBag_id,  # fixed brand/model/category
            'size': size,
            'color': color,
            'material': material,
            'price': last_month_avg_price,
            'fluctuation': last_month_fluctuation,
            'purchasedPrice': purchasedPrice
        })
    return updated_data

def update_collection_by_price_fluctuation(collection_data, data):
    for i, collection in enumerate(collection_data):
        masterBagId = collection['masterBagId']
        size = collection['size']
        material = collection['material']
        color = collection['color']
        purchasedPrice = collection['purchasedPrice']
        candidates = [item for item in data if item['masterBagId'] == masterBagId]
        if len(candidates) == 0:
            continue
        if size != 'null':
            size_candidates = [cand for cand in candidates if cand['size'] == size]
            if len(size_candidates) > 0:
                next_candidates = size_candidates
                if color != 'null' and material != 'null':
                    next_candidates = [cand for cand in size_candidates if cand['material'] == material and cand['color'] == color]
                elif color != 'null' and material == 'null':
                    next_candidates = [cand for cand in size_candidates if cand['color'] == color]
                elif color == 'null' and material != 'null':
                    next_candidates = [cand for cand in size_candidates if cand['material'] == material]

                if len(next_candidates) > 0:
                    candidates = next_candidates
        print(len(candidates))
        sum_price = sum(fc['price'] for fc in candidates)
        avg_price = sum_price / len(candidates)
        collection_data[i]['price'] = avg_price
        fluctuation = ((avg_price - purchasedPrice) / purchasedPrice) * 100
        collection_data[i]['fluctuation'] = fluctuation
    return collection_data

def update_db_collection(db_conn: pymysql.Connection):
    with open('handbag_data.json') as input_file:
        data = json.load(input_file)
    with open('master_handbag_data.json') as input_file:
        master_data = json.load(input_file)
    try:
        # Get ALL Collection records
        select_query = "SELECT * FROM Collection"
        cur = db_conn.cursor()
        cur.execute(select_query)
        collection_data = cur.fetchall()

        # update new masterBagId
        updated_data = update_collection_by_masterBagId(collection_data, master_data)
        print(updated_data)

        # calculate new price and fluctuation
        updated_data = update_collection_by_price_fluctuation(updated_data, data)
        print(updated_data)

        # Update each Collection record by specific fields
        update_query = ("UPDATE Collection SET price = %(new_price)s, fluctuation = %(new_fluctuation)s, "
                        "masterBagId = %(new_master_bag_id)s WHERE collectionId = %(collection_id)s ")

        data_list = []
        for item in updated_data:
            # Extract the necessary data for updating
            collection_id = item['collectionId']
            new_price = Decimal(str(item['price']))
            new_fluctuation = Decimal(str(item['fluctuation']))
            new_masterBagId = item['masterBagId']
            data_list.append({
                    'new_price': new_price,
                    'new_fluctuation': new_fluctuation,
                    'collection_id': collection_id,
                    'new_master_bag_id': new_masterBagId
                })

            # Execute the update query with the provided values
        db_conn.cursor().executemany(update_query, data_list)
        db_conn.commit()
    except Error as e:
        print(f"Error updating data: {e}")


with connect_with_connector() as db_conn:
    save_master_to_db(db_conn)
    save_bag_to_db(db_conn)
    save_price_trend_to_db(db_conn)
    update_db_collection(db_conn)
