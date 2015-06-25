import os
import subprocess
import time
import tornado
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.web import RequestHandler, Application, url, asynchronous, gen
from tornado_elasticsearch import AsyncElasticsearch
from collections import OrderedDict
from urllib import parse


class AdvertiserHandler(RequestHandler):
    @asynchronous
    def get(self):
        product_advertiser = self.get_argument("advertiser", None, True)
        if product_advertiser:
            if os.path.isfile("feed.xml"):
                self.finish(
                    "<img src='http://i.imgur.com/8a3eP8u.jpg'><br>"
                    "<br>Woah there chuck! A feed job is already"
                    "<br>in progress. Try again in a bit...")
            else:
                cmd = "python3.4 {0}/feedService.py {1}"\
                    .format(os.path.dirname(os.path.abspath(__file__)),
                            parse.quote(product_advertiser))
                print(cmd)

                def send(data):
                    if data:
                        self.write(data)
                        print(data)
                        self.flush()
                    else:
                        self.finish()

                self.subprocess(cmd, send)
        else:
            self.finish()

    def subprocess(self, cmd, callback):
        ioloop = tornado.ioloop.IOLoop.instance()
        _pipe = subprocess.PIPE
        pipe = subprocess.Popen(cmd, shell=True, stdin=_pipe, stdout=_pipe,
                                stderr=subprocess.STDOUT, close_fds=True)
        fd = pipe.stdout.fileno()

        def receive(*args):
            data = pipe.stdout.readline()
            if data:
                callback(data)
            elif pipe.poll() is not None:
                ioloop.remove_handler(fd)
                callback(None)

        ioloop.add_handler(fd, receive, ioloop.READ)


class ProductHandler(RequestHandler):
    def initialize(self):
        self.es = AsyncElasticsearch()

    @asynchronous
    @gen.engine
    def get(self):

        start_time = time.time()

        product_limit = self.get_argument("limit", 1000, True)
        if product_limit.isdigit():
            product_limit = int(product_limit)
        else:
            product_limit = 1000

        product_offset = self.get_argument("offset", 0, True)
        if isinstance(product_offset, str) and product_offset.isdigit():
            product_offset = int(product_offset)
        else:
            product_offset = 0

        product_keywords = self.get_argument("keywords", None, True)

        product_price_min = self.get_argument("priceMin", None, True)
        if product_price_min and product_price_min.isdigit():
            product_price_min = int(product_price_min)

        product_price_max = self.get_argument("priceMax", None, True)
        if product_price_max and product_price_max.isdigit():
            product_price_max = int(product_price_max)

        search_query = {
            "size": product_limit,
            "from": product_offset,
            "query": {
                "filtered": {}
            },
            "partial_fields": {
                "feed": {
                    "exclude": "float_price"
                }
            }
        }

        filtered_query = dict()

        if product_keywords:
            match_query = {"match": {"_all": {
                "query": product_keywords,
                "operator": "or"
            }}}
            filtered_query["query"] = match_query

        if product_price_min or product_price_max:
            price_range = dict()
            range_query = {
                "range": {"float_price": {}}
            }
            if product_price_min:
                price_range["gte"] = product_price_min
            if product_price_max:
                price_range["lte"] = product_price_max

            range_query["range"]["float_price"] = price_range
            filtered_query["filter"] = range_query

        if filtered_query:
            search_query["query"]["filtered"] = filtered_query

        es_json_response = yield self.es.search(index="products",
                                                body=search_query)
        es_time = es_json_response["took"]
        products = es_json_response["hits"]["hits"]
        products_sanitized = []
        for product in products:
            products_sanitized.append(product["fields"]["feed"][0])

        #OrderDict the response so we can ensure meta stays on top.
        res = OrderedDict([
            ("meta",
             {"total": len(products),
              "time": "0ms",
              "limit": product_limit,
              "offset": product_offset}),
            ("products", products_sanitized)
        ])

        res_time = (time.time() - start_time)
        res["meta"]["time"] = str(round(res_time * 1000) + es_time) + "ms"

        self.finish(res)


def make_app():
    return Application([
        url(r"/products", AdvertiserHandler),
        url(r"/search", ProductHandler)
    ])


def main():
    define("port", default=8080, help="app port", type=int)
    options.parse_command_line()

    if os.path.isfile("feed.xml"):
        os.remove("feed.xml")

    app = make_app()
    app.listen(options.port)
    IOLoop.current().start()


if __name__ == "__main__":
    main()