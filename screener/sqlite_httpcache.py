from os import path
import json
from contextlib import ExitStack
import sqlite3
import pickle
from datetime import date, datetime, timedelta
from logging import getLogger

from scrapy.utils.misc import load_object
from scrapy.utils.request import request_fingerprint
from scrapy.responsetypes import responsetypes
from scrapy.http.headers import Headers

SCHEMA = """
CREATE TABLE IF NOT EXISTS httpcache (
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

def dumps_headers(headers):
    return "{}"

def loads_headers(json_str):
    return Headers()

class SqliteCacheStorage(object):
    def __init__(self, settings):
        self.logger = getLogger(__name__)
        self.path = path.join(
            settings["HTTPCACHE_DIR"],
            settings.get("HTTPCACHE_SQLITE_FILENAME", "httpcache.sqlite3")
        )
        self.expiration_secs = settings["HTTPCACHE_EXPIRATION_SECS"]
        lock = settings.get("HTTPCACHE_SQLITE_WRITE_LOCK", None)
        if lock is not None:
            self.write_lock = load_object(lock)
        else:
            # ExitStack functions as a null context manager
            self.write_lock = ExitStack()

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.path)
        with self.write_lock:
            self.conn.execute(SCHEMA)
            self.conn.commit()

    def close_spider(self, spider):
        self.logger.info("closing")

    def store_response(self, spider, request, response):
        with self.write_lock:
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
                self.logger.debug("inserted: (%s) %s", fingerprint, request.url)
            else:
                self.logger.debug("updated: (%s) %s", fingerprint, request.url)
            self.conn.commit()

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
            self.logger.debug("found: (%s) %s", fingerprint, request.url)
        except TypeError:
            self.logger.debug("did not find: (%s) %s", fingerprint, request.url)
            return None
        headers = pickle.loads(headers_pickle)
        respcls = responsetypes.from_args(headers=headers, url=url)
        return respcls(url=url, headers=headers, status=status, body=body)
