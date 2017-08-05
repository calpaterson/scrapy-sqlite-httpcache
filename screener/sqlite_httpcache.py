from os import path
from contextlib import closing
import sqlite3
import pickle
from datetime import date, datetime, timedelta
from logging import getLogger

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

UPDATE = """
UPDATE httpcache
SET
    spider = ?,
    status = ?,
    url = ?,
    headers = ?,
    body = ?,
    seen = CURRENT_TIMESTAMP
WHERE
    request_fingerprint = ?;
"""

INSERT = """
INSERT INTO httpcache (
    spider,
    status,
    url,
    headers,
    body,
    request_fingerprint
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
        self.logger = getLogger(__name__)
        self.path = path.join(
            settings["HTTPCACHE_DIR"],
            "httpcache.sqlite3"
        )
        self.expiration_secs = settings["HTTPCACHE_EXPIRATION_SECS"]

    def open_spider(self, spider):
        if not path.exists(self.path):
            self.logger.info("creating httpcache.sqlite3")
            self.conn = sqlite3.connect(self.path)
            self.conn.execute(SCHEMA)
        else:
            self.conn = sqlite3.connect(self.path)

    def close_spider(self, spider):
        self.logger.info("committing")
        self.conn.commit()

    def store_response(self, spider, request, response):
        fingerprint = request_fingerprint(request)
        tup = (
            spider.name,
            response.status,
            response.url,
            pickle.dumps(response.headers, 4),
            response.body,
            fingerprint,
        )
        modified = self.conn.execute(UPDATE, tup).rowcount
        if modified == 0:
            self.conn.execute(INSERT, tup)
            self.logger.info("inserted: (%s) %s", fingerprint, request.url)
        else:
            self.logger.info("updated: (%s) %s", fingerprint, request.url)

    def retrieve_response(self, spider, request):
        if self.expiration_secs == 0:
            seen_threshold = date(1970, 1, 1)
        else:
            seen_threshold = datetime.utcnow() - \
                             timedelta(seconds=self.expiration_secs)
        try:
            fingerprint = request_fingerprint(request)
            status, url, headers_pickle, body = (
                self.conn.execute(DQL, (
                    fingerprint,
                    spider.name,
                    seen_threshold
                ))
                .fetchone()
            )
            self.logger.info("found: (%s) %s", fingerprint, request.url)
        except TypeError:
            self.logger.info("did not find: (%s) %s", fingerprint, request.url)
            return None
        headers = pickle.loads(headers_pickle)
        respcls = responsetypes.from_args(headers=headers, url=url)
        return respcls(url=url, headers=headers, status=status, body=body)
