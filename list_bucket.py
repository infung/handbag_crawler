import os
from google.cloud import storage


def _item_to_value(iterator, item):
    return item


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
        bag_id = desired_string.replace("%", " ")
        vc_image_mapping[bag_id] = url
    return vc_image_mapping


if __name__ == '__main__':
    vc_image_mapping = list_vc_image_path()
