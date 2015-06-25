import hashlib
import os
import sys
import time
from elasticsearch import Elasticsearch
from lxml import etree
from urllib import error, parse, request

es = Elasticsearch()

INDEX_NAME = "products"
#Go find the token if you must... simple google foo should do...
TOKEN = "derpderpderpderpderpderpderpderp"
TOKEN_MD5 = hashlib.md5(TOKEN.encode('utf-8')).hexdigest()

index_mapping = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "product": {
            "_all": {"enabled": True},
            "properties": {
                "image_url":
                    {
                        "type": "string",
                        "include_in_all": False,
                        "index": "no"
                    },
                "product_id":
                {
                    "type": "string",
                    "include_in_all": False,
                    "index": "no"
                },
                "product_url":
                {
                    "type": "string",
                    "include_in_all": False,
                    "index": "no"
                },
                "price":
                {
                    "type": "string",
                    "include_in_all": False,
                },
                "float_price":
                {
                    "type": "float",
                    "include_in_all": False,
                },
                "id":
                {
                    "type": "string",
                    "include_in_all": False,
                }
            }
        }
    }
}


def download_feed():
    """
    Grab feed to local file, make sure it actually has some data.
    """
    advertiser = sys.argv[1]
    print("Grabbing Feed For Advertiser: {0}<br>".format(
        parse.unquote(advertiser))
    )
    start_time = time.time()
    if os.path.isfile("feed.xml"):
        os.remove("feed.xml")
    try:
        feed_url = "https://api.rewardstyle.com/v1/product_feed?" \
            "oauth_token={0}&advertiser={1}"\
            .format(TOKEN, advertiser)
        request.urlretrieve(feed_url, "feed.xml")
        print("Feed download time: {0}ms<br>"
          .format(str(round((time.time() - start_time) * 1000))))

    except error.HTTPError as e:
        print("Feed most likely does not exist: {0}".format(e))
        sys.exit()

    file = open("feed.xml", "rb")
    file.seek(0, 2)
    size = file.tell()
    file.close()
    if size <= 64:
        print("Feed looks empty")
        sys.exit()


def elem2dict(node):
    """
    Easy conversion of etree to dict.
    """
    d = {}
    for e in node:
        d[e.tag] = e.text
    return d


def insert_token(item):
    """
    Insert md5 hashed auth token.
    """
    item["product_url"] = item["product_url"].replace("MD5-YOUR-OAUTH-TOKEN",
                                                      TOKEN_MD5)
    return item


def bulk_elasticsearch_insert(data):
    """
    Bulk insert and indexing.
    """
    es.bulk(index=INDEX_NAME, body=data, refresh=True)


def prepare_index():
    """
    Nuke the previous index if it exists and prepare for new one.
    """
    if es.indices.exists(INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print("Deleting '{0}' index<br>".format(INDEX_NAME))

    es.indices.create(index=INDEX_NAME, body=index_mapping)
    print("Created '{0}' index<br>".format(INDEX_NAME))


def read_bulk_es_insert():
    """
    Grab the feed to a local file, traverse file in a memory efficient
    way with etree, do some conversion to dict in preparation for json
    data for db insert.
    """
    start_time = time.time()

    prepare_index()

    context = etree.iterparse("feed.xml", events=("start", "end"))
    event, root = context.__next__()

    bulk_data = []
    product_id = 0
    for event, elem in context:
        if event == "end" and elem.tag == "item":
            product_id = elem.get("id")
            item = elem.getchildren()
            product = insert_token(elem2dict(item))

            product["float_price"] = float(product["price"])
            product["id"] = product_id
            root.clear()

            op_dict = {
                "index": {
                "_index": INDEX_NAME,
                "_type": "product",
                "_id": product_id
                }
            }
            bulk_data.append(op_dict)
            bulk_data.append(product)
            if int(product_id) % 10000 == 0 and int(product_id) is not 0:
                bulk_elasticsearch_insert(bulk_data)
                print("{0} Records inserted<br>".format(product_id))
                bulk_data = []

    if bulk_data:
        bulk_elasticsearch_insert(bulk_data)
        print("Total of {0} Records inserted<br>".format(product_id))

    if os.path.isfile("feed.xml"):
        os.remove("feed.xml")

    print("Product Feed transform and elasticsearch insert time: {0}ms"
          .format(str(round((time.time() - start_time) * 1000))))


def main():
    download_feed()
    read_bulk_es_insert()


if __name__ == "__main__":
    main()