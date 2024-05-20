import datetime
import json
import os
import shutil
from collections import Counter
from json import JSONDecodeError

import numpy as np
from unidecode import unidecode
from constant import bag_brands
from crawl import BAG_DETAILS
from google.cloud import storage
from scipy.stats import skew, normaltest

CS_CONDITION = {
    'new': 'New',
    'like new': 'New',
    'very good condition': 'Excellent',
    'good condition': 'Good',
    'good': 'Good',
    'pre-owned': 'Good',
    'used': 'Fair'
}
RB_CONDITION = {
    'pristine': 'New',
    'excellent': 'Excellent',
    'great': 'Excellent',
    'very good': 'Good',
    'good': 'Fair',
    'fair': 'Fair'
}
FP_CONDITION = {
    'giftable': 'New',
    'new': 'New',
    'excellent': 'Excellent',
    'very good': 'Good',
    'good': 'Fair',
    'fair': 'Fair',
    'flawed': 'Fair'
}
VC_CONDITION = {
    'new': 'Excellent',
    'very good condition': 'Good',
    'good condition': 'Good',
    'pre-owned': 'Good',
    'fair condition': 'Fair',
}
TF_CONDITION = {
    'never worn': 'New',
    'never-worn': 'New',
    'pre-owned': 'Good',
}

merge_cat_1 = ["satchel", "handbag", "bag", "shoulder bag"]
merge_cat_2 = ["pouch", "clutch"]
merge_cat_3 = ["travel bag", "duffle"]
merge_cat_4 = ["crossbody", "messenger"]
distinct_cat = ["backpack", "bucket", "coin purse", "suitcase", "wallet", "briefcase", "vanity case", "hobo", "tote",
                "top handle", "trunk"]


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def get_bag_collection():
    brand_model_mapping = {}

    for brand in bag_brands:
        model_set = set()

        with open('CollectorSquare_bag_' + brand + '.json') as input_file:
            data = json.load(input_file)
            for item in data:
                if item['model'] != '':
                    model_set.add(item['model'])

        brand_model_mapping[brand] = model_set

    with open('brand_model_mapping.json', 'w') as output_file:
        json.dump(brand_model_mapping, output_file, indent=2, default=set_default)


def get_value_from_mapping(mapping, title, field_key):
    selected_value = None

    for value in mapping[field_key]:
        if value.lower() in title:
            selected_value = value.lower()
            break

    return selected_value


def add_field_by_type(brand_map, model, field, comb, price):
    if field not in brand_map[model][comb]:
        brand_map[model][comb][field] = {
            'count': 1,
            'min_prices': price,
            'max_prices': price
        }
        # brand_map[model][comb][field] = 1
    else:
        # brand_map[model][comb][field] += 1
        brand_map[model][comb][field]['count'] += 1
        brand_map[model][comb][field]['min_prices'] = min(brand_map[model][comb][field]['min_prices'], price)
        brand_map[model][comb][field]['max_prices'] = max(brand_map[model][comb][field]['max_prices'], price)


def get_value(value):
    if value is None:
        return "null"
    else:
        return value.lower()


def sort_object_key(brand_map, model, type):
    obj = brand_map[model][type]
    obj = dict(sorted(obj.items()))
    brand_map[model][type] = obj


def classify_image(brand, target_model, category, item_id, source_path):
    try:
        destination_path = 'bag_image_v2/' + brand + "/" + target_model + "/" + category
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)
        destination_path = destination_path + "/" + str(item_id) + '.jpg'
        shutil.copy(source_path, destination_path)
    except FileNotFoundError:
        print(source_path)

    return brand + "/" + target_model + "/" + category


def merge_new_old_data():
    sources = ['CollectorSquare', 'Fashionphile', 'Rebag', 'Truefacet', 'Vestiaire Collective']
    for source in sources:
        for brand in bag_brands:
            try:
                merged_data = []
                with open(source + '_bag_' + brand + '.json') as input_file_1:
                    new_data = json.load(input_file_1)
                with open('./old_data/2024_04/' + source + '_bag_' + brand + '.json', 'r') as input_file_2:
                    old_data = json.load(input_file_2)
                for new_item in new_data:
                    for old_item in old_data:
                        if new_item['id'] == old_item['id']:
                            new_item['trends'].extend(old_item['trends'])
                            break
                    merged_data.append(new_item)
                with open('./2024_05/' + source + '_bag_' + brand + '.json', 'w') as f:
                    json.dump(new_data, f, indent=2, default=set_default)
            except JSONDecodeError:
                print(source + '_bag_' + brand + '.json')


def get_overall_mapping():
    bag_counter_mapping = {}

    sources = ['CollectorSquare', 'Fashionphile', 'Rebag', 'Truefacet', 'Vestiaire Collective']
    for source in sources:
        for brand in bag_brands:
            try:
                with open('./2024_05/' + source + '_bag_' + brand + '.json') as input_file:
                    data = json.load(input_file)
                    for item in data:
                        brand = item['brand'].lower()
                        title = unidecode(item['title'].lower())
                        size = get_value(item['size'])
                        color = get_value(item['color'])
                        material = get_value(item['material'])
                        category = get_value(item['category'])
                        item_id = get_value(item['id'])
                        currency = item['currency']
                        price = item['price']

                        if brand not in bag_counter_mapping:
                            bag_counter_mapping[brand] = {}

                        target_model = item['collection']

                        if target_model not in bag_counter_mapping[brand]:
                            bag_counter_mapping[brand][target_model] = {
                                'count': 0,
                                'category': {},
                                # 'color': {},
                                # 'material': {},
                                # 'size': {},
                                'category_size': {}
                            }
                        else:
                            bag_counter_mapping[brand][target_model]['count'] += 1

                        add_field_by_type(bag_counter_mapping[brand], target_model, category, 'category', price)
                        # add_field_by_type(bag_counter_mapping[brand], target_model, color, 'color')
                        # add_field_by_type(bag_counter_mapping[brand], target_model, size, 'size', price)
                        # add_field_by_type(bag_counter_mapping[brand], target_model, material, 'material')
                        add_field_by_type(bag_counter_mapping[brand], target_model, category + "-" + size,
                                          'category_size', price)
            except FileNotFoundError:
                print(source + '_bag_' + brand + '.json')
    with open('overall_mapping.json', 'w') as output_file:
        json.dump(bag_counter_mapping, output_file, indent=2, default=set_default)


def remove_inactive_model():
    collection_map = BAG_DETAILS['collection']
    with open('overall_mapping.json') as input_file:
        overall_map = json.load(input_file)
    bag_counters = overall_map['BAG_COUNTERS']

    for brand, brand_data in bag_counters.items():
        collections = brand_data["collection"]
        collections_to_keep = [collection for collection, count in collections.items() if count >= 20]
        collection_map[brand] = collections_to_keep

    with open('brand_model_mapping.json', 'w') as output_file:
        json.dump(collection_map, output_file, indent=2, default=set_default)


def sort_bag_model():
    with open('brand_model_mapping.json') as input_file:
        collection_map = json.load(input_file)
    for brand in collection_map:
        collection_map[brand].sort(key=len)
        collection_map[brand].reverse()
    with open('brand_model_mapping.json', 'w') as output_file:
        json.dump(collection_map, output_file, indent=2, default=set_default)


def get_entry_with_max_count(category_list, count_dict):
    max_count = 0
    max_entry = None
    for entry in category_list:
        if entry in count_dict and count_dict[entry] > max_count:
            max_count = count_dict[entry]
            max_entry = entry
    return max_entry


def master_classify_v1():
    master_mapping = {}
    sum = 0
    with open('overall_mapping.json') as input_file:
        data = json.load(input_file)
        for brand, models in data.items():
            master_mapping[brand] = {}
            for model, item in models.items():
                master_mapping[brand][model] = []
                category_count = item['category']
                # 替换空数据的键并叠加到最大键的值上
                # if "null" in category_count:
                #     null_value = category_count.pop("null")
                #     max_key = max(category_count, key=category_count.get)
                #     max_value = category_count[max_key]
                #     category_count[max_key] = max_value + null_value
                # 无视空数据
                filtered_category_count = {key: value for key, value in category_count.items() if key != "null"}
                # 寻找最高的前k个峰值作为master
                count_list = [filtered_category_count[key]["count"] for key in filtered_category_count]
                mean = np.mean(count_list)
                std = np.std(count_list)
                # 计算每组数据的自适应阈值并提取峰值
                factor = 0.1
                threshold = mean + factor * std
                group_peaks = {}
                for category, obj in filtered_category_count.items():
                    if obj["count"] >= threshold:
                        group_peaks[category] = obj["count"]
                if len(group_peaks) == 0:
                    max_key = max(filtered_category_count, key=lambda x: filtered_category_count[x]["count"])
                    max_value = filtered_category_count[max_key]["count"]
                    group_peaks[max_key] = max_value
                # 移除相似类别
                filtered_group_peaks = {}
                distinct_entries = {key: value for key, value in group_peaks.items() if key in distinct_cat}
                filtered_group_peaks.update(distinct_entries)
                cat_1_entry = get_entry_with_max_count(merge_cat_1, group_peaks)
                if cat_1_entry:
                    filtered_group_peaks[cat_1_entry] = group_peaks[cat_1_entry]
                cat_2_entry = get_entry_with_max_count(merge_cat_2, group_peaks)
                if cat_2_entry:
                    filtered_group_peaks[cat_2_entry] = group_peaks[cat_2_entry]
                cat_3_entry = get_entry_with_max_count(merge_cat_3, group_peaks)
                if cat_3_entry:
                    filtered_group_peaks[cat_3_entry] = group_peaks[cat_3_entry]
                cat_4_entry = get_entry_with_max_count(merge_cat_4, group_peaks)
                if cat_4_entry:
                    filtered_group_peaks[cat_4_entry] = group_peaks[cat_4_entry]
                master_mapping[brand][model] = filtered_group_peaks
                sum += len(filtered_group_peaks)
    with open('master_mapping.json', 'w') as output_file:
        json.dump(master_mapping, output_file, indent=2, default=set_default)
    print('total num of master: ', sum)


def master_classify_v2():
    master_mapping = {}
    sum = 0
    with open('overall_mapping.json') as input_file:
        data = json.load(input_file)
    for brand, models in data.items():
        master_mapping[brand] = {}
        for model, item in models.items():
            master_mapping[brand][model] = []

            cat_size = {}
            category_size = item['category_size']
            for key, value in category_size.items():
                if not key.startswith("null") and value["count"] > 10:
                    if key.endswith("-null"):
                        specific_type = key.split("-")[0]
                        specific_type_entries = [k for k in category_size.keys() if
                                                 k.startswith(specific_type) and k != key]

                        if len(specific_type_entries) == 0:
                            cat_size[key] = value["count"]
                    else:
                        cat_size[key] = value["count"]
            if len(cat_size) == 0:
                continue
            # 寻找最高的前k个峰值作为master
            count_list = [cat_size[key] for key in cat_size]
            mean = np.mean(count_list)
            std = np.std(count_list)
            # 计算每组数据的自适应阈值并提取峰值
            factor = 0.1
            threshold = mean + factor * std
            group_peaks = []
            for category, count in cat_size.items():
                if count >= threshold:
                    group_peaks.append(category)
            if len(group_peaks) == 0:
                max_key = max(cat_size, key=lambda x: cat_size[x])
                max_value = cat_size[max_key]
                group_peaks.append(max_key)
            sum += len(group_peaks)
            master_mapping[brand][model] = group_peaks
    with open('master_mapping_v2.json', 'w') as output_file:
        json.dump(master_mapping, output_file, indent=2, default=set_default)
    print('total num of master: ', sum)


def get_condition_avg():
    condition_avg = {}
    sources = ['CollectorSquare', 'Fashionphile', 'Rebag', 'Truefacet', 'Vestiaire Collective']
    brand = 'Hermes'
    model = 'birkin'
    for source in sources:
        condition_avg[source] = {}
        if source == 'CollectorSquare':
            cons = CS_CONDITION.keys()
        elif source == 'Rebag':
            cons = RB_CONDITION.keys()
        elif source == 'Fashionphile':
            cons = FP_CONDITION.keys()
        elif source == 'Vestiaire Collective':
            cons = VC_CONDITION.keys()
        else:
            cons = TF_CONDITION.keys()
        for cond in cons:
            sum = 0
            count = 0
            with open('./2024_05/' + source + '_bag_' + brand + '.json') as input_file:
                data = json.load(input_file)
                for item in data:
                    price = item['price']
                    condition = item['condition'].lower()
                    collection = item['collection']
                    if condition == cond and collection == model:
                        sum += price
                        count += 1
            if count != 0:
                condition_avg[source][cond] = sum // count
            else:
                condition_avg[source][cond] = 0
    print(condition_avg)


def define_condition(source, condition):
    cond = 'Pre-Owned'
    condition = condition.lower()
    if source == 'CollectorSquare':
        cond = CS_CONDITION[condition]
    elif source == 'Rebag':
        cond = RB_CONDITION[condition]
    elif source == 'Fashionphile':
        cond = FP_CONDITION[condition]
    elif source == 'Vestiaire Collective':
        cond = VC_CONDITION[condition]
    elif source == 'Truefacet':
        cond = TF_CONDITION[condition]
    return cond


def merge_all_data_v1():
    items = []
    with open('master_mapping.json') as input_file:
        master_mapping = json.load(input_file)

    sources = ['CollectorSquare', 'Fashionphile', 'Rebag', 'Truefacet', 'Vestiaire Collective']
    for source in sources:
        for brand in bag_brands:
            with open('./2024_05/' + source + '_bag_' + brand + '.json') as input_file:
                data = json.load(input_file)
            for item in data:
                model = item['collection']
                category = item['category']
                candidates = master_mapping[brand.lower()][model]
                condition = define_condition(source, item['condition'])
                if category in candidates:
                    item_id = item['id']
                    source_path = get_value(item['folder']) + '/' + str(item_id) + '.jpg'
                    destination_path = classify_image(brand.lower(), model, category, item_id, source_path)
                    folder = brand + "/" + model + "/" + category
                    items.append({
                        'brand': brand,
                        'model': model,
                        'price': item['price'],
                        'color': item['color'],
                        'size': item['size'],
                        'category': category,
                        'material': item['material'],
                        'trends': item['trends'],
                        'title': item['title'],
                        'like': item['like'],
                        'source': item['source'],
                        'url': item['url'],
                        'currency': item['currency'],
                        'image': item['image'],
                        'folder': folder,
                        'condition': condition,
                        'bagId': item_id
                    })
    with open('handbag_data.json', 'w') as output_file:
        json.dump(items, output_file, indent=2, default=set_default)


def merge_all_data_v2():
    items = []
    with open('master_mapping_v2.json') as input_file:
        master_mapping = json.load(input_file)

    sources = ['CollectorSquare', 'Fashionphile', 'Rebag', 'Truefacet', 'Vestiaire Collective']
    for source in sources:
        for brand in bag_brands:
            with open('./2024_05/' + source + '_bag_' + brand + '.json') as input_file:
                data = json.load(input_file)
            for item in data:
                model = item['collection']
                category = item['category']
                if category is None:
                    continue
                size = item['size']
                if size is None:
                    size = 'null'
                match_key = category + '-' + size
                candidates = master_mapping[brand.lower()][model]
                condition = define_condition(source, item['condition'])
                if match_key in candidates:
                    item_id = item['id']
                    source_path = get_value(item['folder']) + '/' + str(item_id) + '.jpg'
                    destination_path = classify_image(brand.lower(), model, category, item_id, source_path)
                    if size == 'null':
                        folder = brand + "/" + model + "/" + category
                    else:
                        folder = brand + "/" + model + "/" + category + "/" + size
                    items.append({
                        'brand': brand,
                        'model': model,
                        'price': item['price'],
                        'color': item['color'],
                        'size': item['size'],
                        'category': category,
                        'material': item['material'],
                        'trends': item['trends'],
                        'title': item['title'],
                        'like': item['like'],
                        'source': item['source'],
                        'url': item['url'],
                        'currency': item['currency'],
                        'image': item['image'],
                        'folder': folder,
                        'condition': condition,
                        'bagId': item_id
                    })
    with open('handbag_data.json', 'w') as output_file:
        json.dump(items, output_file, indent=2, default=set_default)
    print(len(items))


def remove_outlier():
    item_list = []
    master_group = {}
    with open('handbag_data.json') as input_file:
        data = json.load(input_file)
    for item in data:
        folder = item['folder']
        if folder in master_group:
            master_group[folder].append(item)
        else:
            master_group[folder] = [item]
    for key, group in master_group.items():
        price_list = [x['price'] for x in group]
        # Calculate skewness of prices
        prices_skew = skew(np.array(price_list))
        threshold = 0.8
        if abs(prices_skew) > threshold:
            # Use IQR method for skewed prices
            Q1 = np.percentile(price_list, 25)
            Q3 = np.percentile(price_list, 75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            filtered_item = [x for x in group if lower_bound <= x['price'] <= upper_bound]
            print('1', key, len(group), len(filtered_item))
            item_list += filtered_item
        else:
            # Use standard deviations method for normally distributed prices
            mean = np.mean(price_list)
            std = np.std(price_list)
            lower_bound = mean - 1 * std
            upper_bound = mean + 1 * std
            filtered_item = [x for x in group if lower_bound <= x['price'] <= upper_bound]
            print('2', key, len(group), len(filtered_item))
            item_list += filtered_item
        filtered_prices = [p for p in price_list if lower_bound <= p <= upper_bound]
        print(min(filtered_prices), max(filtered_prices))
    with open('handbag_data.json', 'w') as output_file:
        json.dump(item_list, output_file, indent=2, default=set_default)
    print(len(item_list))


def get_price_last_month():
    with open('priceTrend_data.json') as input_file:
        priceTrend_data = json.load(input_file)

    current_date = datetime.date.today()
    previous_month = current_date.month - 1 if current_date.month > 1 else 12
    previous_year = current_date.year if current_date.month > 1 else current_date.year - 1

    price_last_month = []
    for item in priceTrend_data:
        item_date = datetime.datetime.fromisoformat(item['date']).date()
        current_year = item_date.year
        current_month = item_date.month
        if current_year == previous_year and current_month == previous_month:
            candidate = {
                'price': item['price'],
                'bagId': item['bagId']
            }
            price_last_month.append(candidate)
    return price_last_month


def get_price_within_three_months():
    with open('priceTrend_data.json') as input_file:
        priceTrend_data = json.load(input_file)

    current_date = datetime.date.today()
    start_month = (current_date.month - 2) % 12 if current_date.month > 2 else (current_date.month + 10) % 12
    if start_month == 0:
        start_month = 12
    start_year = current_date.year if current_date.month > 2 else current_date.year - 1
    end_month = current_date.month
    end_year = current_date.year

    price_within_three_months = {}
    for item in priceTrend_data:
        item_date = datetime.datetime.fromisoformat(item['date']).date()
        year = item_date.year
        month = item_date.month
        if (start_year == year and start_month <= month) or (end_year == year and start_month <= month <= end_month):
            price = item['price']
            bagId = item['bagId']
            if bagId not in price_within_three_months:
                price_within_three_months[bagId] = [None] * 3
            # store the price by time sequence and specified by id in price_within_three_months
            index = (month - start_month) % 12
            print(month, index)
            price_within_three_months[bagId][index] = price
    return price_within_three_months


def find_master(group):
    master = None
    colors = Counter([item['color'] for item in group])
    sizes = Counter([item['size'] for item in group])
    materials = Counter([item['material'] for item in group])

    most_common_color = colors.most_common(1)[0][0]
    most_common_size = sizes.most_common(1)[0][0]
    most_common_material = materials.most_common(1)[0][0]

    for item in group:
        if (item['color'] == most_common_color
                and item['size'] == most_common_size
                and item['material'] == most_common_material):
            master = item
            break
    if master is None:
        for item in group:
            if item['size'] == most_common_size and item['material'] == most_common_material:
                master = item
                break
    if master is None:
        for item in group:
            if item['size'] == most_common_size:
                master = item
                break
    return master


def get_fluctuation(group, price_last_month):
    total_price = sum(item['price'] for item in group)
    avg_price = total_price / len(group)

    last_month_price = []
    for item in group:
        matching_prices = [pair['price'] for pair in price_last_month if pair['bagId'] == item['bagId']]
        if matching_prices:
            last_month_price.append(sum(matching_prices) / len(matching_prices))
    if last_month_price:
        last_month_avg_price = sum(last_month_price) / len(last_month_price)
    else:
        last_month_avg_price = avg_price

    fluctuation = ((avg_price - last_month_avg_price) / last_month_avg_price) * 100
    return fluctuation


def get_volatility(group, get_price_within_three_months):
    three_months_price = {
        'p1': [],
        'p2': [],
        'p3': []
    }
    for item in group:
        id = item['bagId']
        three_prices = get_price_within_three_months[id]
        if three_prices[0] is not None:
            three_months_price['p1'].append(three_prices[0])
        if three_prices[1] is not None:
            three_months_price['p2'].append(three_prices[1])
        if three_prices[2] is not None:
            three_months_price['p3'].append(three_prices[2])
    if three_months_price['p3']:
        p3 = sum(three_months_price['p3']) / len(three_months_price['p3'])
    else:
        raise Exception('no price data of this month')
    if three_months_price['p1']:
        p1 = sum(three_months_price['p1']) / len(three_months_price['p1'])
    else:
        p1 = p3
    if three_months_price['p2']:
        p2 = sum(three_months_price['p2']) / len(three_months_price['p2'])
    else:
        p2 = p3
    prices = [p1, p2, p3]
    print(prices)
    returns = np.array(prices[1:]) / np.array(prices[:-1])
    log_returns = np.log(returns)
    volatility = np.std(log_returns)
    return volatility


def get_price_premium(group):
    new_price = []
    pre_owned_price = []
    for item in group:
        if item['condition'] == 'New':
            new_price.append(item['price'])
        else:
            pre_owned_price.append(item['price'])
    if pre_owned_price and new_price:
        avg_pre_owned_price = sum(pre_owned_price) / len(pre_owned_price)
        avg_new_price = sum(new_price) / len(new_price)
    elif pre_owned_price:
        avg_pre_owned_price = sum(pre_owned_price) / len(pre_owned_price)
        avg_new_price = avg_pre_owned_price
    elif new_price:
        avg_new_price = sum(new_price) / len(new_price)
        avg_pre_owned_price = avg_new_price
    else:
        raise Exception('No retail price!')
    return avg_pre_owned_price - avg_new_price


def select_master():
    master_group = {}
    with open('handbag_data.json') as input_file:
        handbag_data = json.load(input_file)
    for item in handbag_data:
        folder = item['folder']
        if folder in master_group:
            master_group[folder].append(item)
        else:
            master_group[folder] = [item]
    print(len(master_group.keys()))
    price_last_month = get_price_last_month()
    price_within_three_months = get_price_within_three_months()
    master_handbags = []
    all_handbags = []
    for key, group in master_group.items():
        master = find_master(group)
        if master is None:
            raise ValueError('master not found!')

        total_like = sum(item['like'] for item in group)
        min_price = min(item['price'] for item in group)
        max_price = max(item['price'] for item in group)
        fluctuation = get_fluctuation(group, price_last_month)
        volatility = get_volatility(group, price_within_three_months)
        pricePremium = get_price_premium(group)
        print(volatility, pricePremium)
        master_handbags.append({
            'masterBagId': master['bagId'],
            'image': master['image'],
            'brand': master['brand'],
            'model': master['model'],
            'category': master['category'],
            'color': master['color'],
            'material': master['material'],
            'size': master['size'],
            'like': total_like,
            'lowestPrice': min_price,
            'highestPrice': max_price,
            'fluctuation': fluctuation,
            'volatility': volatility,
            'pricePremium': pricePremium
        })
        for item in group:
            item['masterBagId'] = master['bagId']
            del item['trends']
            all_handbags.append(item)
    with open('master_handbag_data.json', 'w') as output_file:
        json.dump(master_handbags, output_file, indent=2, default=set_default)
    with open('handbag_data.json', 'w') as output_file:
        json.dump(all_handbags, output_file, indent=2, default=set_default)
    print(len(master_handbags), len(all_handbags))


def get_price_trend():
    price_trend = []
    with open('handbag_data.json') as input_file:
        handbag_data = json.load(input_file)
    for item in handbag_data:
        i = 0
        bagId = item['bagId']
        trends = item['trends']
        for pair in trends:
            i += 1
            date = pair['date']
            price = pair['price']
            price_trend_id = str(date) + "-" + str(bagId)
            entry = {
                'priceTrendId': price_trend_id,
                'date': date,
                'price': price,
                'bagId': bagId
            }
            price_trend.append(entry)
    with open('priceTrend_data.json', 'w') as output_file:
        json.dump(price_trend, output_file, indent=2, default=set_default)


def get_vc_master_image():
    with open('master_handbag_data.json') as input_file:
        master_handbags = json.load(input_file)
    vc_master = []
    for item in master_handbags:
        if 'vestiaire collective' in item['masterBagId']:
            vc_master.append(item['masterBagId'])

    with open('handbag_data.json') as input_file:
        handbag_data = json.load(input_file)
    for item in handbag_data:
        for masterId in vc_master:
            if item['bagId'] == masterId:
                source_path = ('bag_image_v2/' + item['brand'].lower() + "/" + item['model'].lower() + "/" +
                               item['category'].lower() + "/" + str(item['bagId']) + '.jpg')
                destination_path = 'vc_image/' + str(item['bagId']) + '.jpg'
                try:
                    if not os.path.exists(destination_path):
                        shutil.copy(source_path, destination_path)
                except FileNotFoundError:
                    print(source_path)


# this function is for crawling all vc images by Power-Automate
def get_vc_data_has_no_image():
    image_folder_mapping = []
    with open('handbag_data.json') as input_file:
        handbag_data = json.load(input_file)

    for item in handbag_data:
        if item['source'] == 'Vestiaire Collective':
            folder = item['brand'].lower() + "/" + item['model'].lower() + "/" + item['category'].lower()
            path = 'bag_image_v2/' + folder + "/" + str(item['bagId']) + '.jpg'
            if not os.path.exists(path):
                image_folder = {'image': item['image'], 'path': folder}
                image_folder_mapping.append(image_folder)
    with open('vc_image_path.json', 'w') as output_file:
        json.dump(image_folder_mapping, output_file, indent=2, default=set_default)
    print(len(image_folder_mapping))


def list_vc_image_path():
    vc_image_mapping = {}
    prefix = 'https://storage.cloud.google.com/handbag_image/'
    client = storage.Client.from_service_account_json(os.path.abspath('./credential/gcp-storage-key.json'))
    bucket = client.get_bucket('handbag_image')
    vc_image = bucket.list_blobs(prefix='vc_image')
    for blob_item in vc_image:
        url = blob_item.public_url
        # Extract the desired string
        start_index = url.rfind('/') + 1
        end_index = url.rfind('.jpg')
        desired_string = url[start_index:end_index]
        bag_id = desired_string.replace("%20", " ")
        vc_image_mapping[bag_id] = url
    return vc_image_mapping


def replace_master_vc_image_path():
    vc_image_mapping = list_vc_image_path()
    with open('master_handbag_data.json') as input_file:
        master_handbags = json.load(input_file)
    new_masters = []
    for master in master_handbags:
        if 'vestiairecollective' in master['image']:
            master['image'] = vc_image_mapping[master['masterBagId']]
        new_masters.append(master)
    with open('master_handbag_data.json', 'w') as output_file:
        json.dump(master_handbags, output_file, indent=2, default=set_default)


def update_price_trend():
    with open('priceTrend_data.json') as input_file:
        priceTrend_data = json.load(input_file)
    with open('handbag_data.json') as input_file:
        handbag_data = json.load(input_file)
    new_priceTrend_data = []
    # remove duplicate
    print(len(priceTrend_data))
    for item in priceTrend_data:
        found = False
        curr = item['priceTrendId']
        for new in new_priceTrend_data:
            if curr == new['priceTrendId']:
                found = True
                break
        if not found:
            new_priceTrend_data.append(item)

    # add field masterBagId
    for i, trend in enumerate(new_priceTrend_data):
        for bag in handbag_data:
            if trend['bagId'] == bag['bagId']:
                new_priceTrend_data[i]['masterBagId'] = bag['masterBagId']
                break
    with open('priceTrend_data.json', 'w') as output_file:
        json.dump(new_priceTrend_data, output_file, indent=2, default=set_default)
    print(len(new_priceTrend_data))

def get_collection_filters():
    collection_filters = {}
    with open('handbag_data.json') as input_file:
        data = json.load(input_file)
    for item in data:
        if item['brand'] not in collection_filters:
            collection_filters[item['brand']] = {}
        if item['model'] not in collection_filters[item['brand']]:
            collection_filters[item['brand']][item['model']] = {
                'category': set(),
                'color': set(),
                'size': set(),
                'material': set()
            }
            collection_filters[item['brand']][item['model']]['material'].add("")
            collection_filters[item['brand']][item['model']]['color'].add("")
            collection_filters[item['brand']][item['model']]['size'].add("")
        if not item['category'] is None:
            collection_filters[item['brand']][item['model']]['category'].add(item['category'])
        if not item['size'] is None:
            collection_filters[item['brand']][item['model']]['size'].add(item['size'])
        if not item['color'] is None:
            collection_filters[item['brand']][item['model']]['color'].add(item['color'])
        if not item['material'] is None:
            collection_filters[item['brand']][item['model']]['material'].add(item['material'])
    with open('collection_filters.json', 'w') as output_file:
        json.dump(collection_filters, output_file, indent=2, default=set_default)


if __name__ == '__main__':
    # merge_new_old_data()
    # get_overall_mapping()
    # master_classify_v2()
    # merge_all_data_v2()
    # remove_outlier()
    # get_price_trend()
    # select_master()
    # update_price_trend()
    # get_vc_data_has_no_image()
    get_collection_filters()
    # ------------------- DB operation -------------------
    # get_vc_master_image()
    # upload vc_image to gcp storage by hand
    # replace_master_vc_image_path()
