from os import path
from contextlib import closing
import sqlite3
import json
from datetime import date, datetime, timedelta

from scrapy.utils.request import request_fingerprint
from scrapy.responsetypes import responsetypes

SCHEMA = """
CREATE TABLE httpcache (
    request_fingerprint TEXT NOT NULL,
    spider TEXT NOT NULL,
    status INT NOT NULL,
    url TEXT NOT NULL,
    headers BLOB NOT NULL,
    body BLOB NOT NULL,
    seen DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (request_fingerprint, spider)
);
"""

DML = """
INSERT OR REPLACE INTO httpcache (
    request_fingerprint,
    spider,
    status,
    url,
    headers,
    body
)
VALUES (?, ?, ?, ?, ?, ?);
"""

DQL = """
SELECT status, url, headers, body
FROM httpcache
WHERE request_fingerprint = ? AND spider = ? AND seen > ?
"""

class SqliteCacheStorage(object):
    def __init__(self, settings):
        self.path = path.join(
            settings["HTTPCACHE_DIR"],
            "httpcache.sqlite3"
        )
        self.expiration_secs = settings["HTTPCACHE_EXPIRATION_SECS"]

    def open_spider(self, spider):
        if not path.exists(self.path):
            self.conn = sqlite3.connect(self.path)
            self.conn.execute(SCHEMA)
        else:
            self.conn = sqlite3.connect(self.path)

    def close_spider(self, spider):
        self.conn.commit()

    def store_response(self, spider, request, response):
        self.conn.execute(DML, (
            request_fingerprint(request),
            spider.name,
            response.status,
            response.url,
            json.dumps(response.headers),
            response.body
        ))

    def retrieve_response(self, spider, request):
        try:
            if self.expiration_secs == 0:
                seen_threshold = date(1970, 1, 1)
            else:
                seen_threshold = datetime.utcnow() - \
                                 timedelta(seconds=self.expiration_secs)
            status, url, headers_json, body = (
                self.conn.execute(DQL, (
                    request_fingerprint(request),
                    spider.name,
                    seen_threshold
                ))
                .fetchone()
            )
        except TypeError:
            return None
        headers = json.loads(headers_json)
        respcls = responsetypes.from_args(headers=headers, url=url)
        return respcls(url=url, headers=headers, status=status, body=body)
