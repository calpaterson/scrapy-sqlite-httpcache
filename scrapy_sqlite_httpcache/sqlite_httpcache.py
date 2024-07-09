from os import path
import json
from contextlib import ExitStack
import sqlite3
from datetime import date, datetime, timedelta
from logging import getLogger

from scrapy.utils.misc import load_object
from scrapy.utils.request import request_fingerprint
from scrapy.responsetypes import responsetypes
from scrapy.http.headers import Headers

def dumps_headers(headers):
    rep = {}
    for key, value in headers.iteritems():
        if isinstance(value, list):
            rep[key.decode(headers.encoding)] = [
                v.decode(headers.encoding) for v in value]
        else:
            rep[key.decode(headers.encoding)] = value.decode(headers.encoding)
    return json.dumps({"encoding": headers.encoding, "rep": rep})

def loads_headers(json_str):
    d = json.loads(json_str)
    encoding = d["encoding"]
    headers = Headers(encoding=encoding)
    for key, value in d["rep"].items():
        if isinstance(value, list):
            headers[key.encode(encoding)] = [v.encode(encoding) for v in value]
        else:
            headers[key.encode(encoding)] = value.encode(encoding)
    return headers


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

        self.schema = """
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

        self.seen_index = """
        CREATE INDEX IF NOT EXISTS httpcache_ix_seen on httpcache (seen);
        """

        self.update = """
        UPDATE httpcache
        SET
            status = ?,
            url = ?,
            headers = ?,
            body = ?,
            seen = CURRENT_TIMESTAMP
        WHERE
            request_fingerprint = ? AND spider = ?;
        """

        self.insert = """
        INSERT INTO httpcache (
            status,
            url,
            headers,
            body,
            request_fingerprint,
            spider
        )
        VALUES (?, ?, ?, ?, ?, ?);
        """

        self.query = """
        SELECT status, url, headers, body
        FROM httpcache
        WHERE request_fingerprint = ? AND spider = ? AND seen > ?
        """

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.path)
        with self.write_lock:
            self.conn.execute(self.schema)
            self.conn.execute(self.seen_index)
            self.conn.commit()

    def close_spider(self, spider):
        self.logger.info("closing")

    def store_response(self, spider, request, response):
        with self.write_lock:
            fingerprint = request_fingerprint(request)
            tup = (
                response.status,
                response.url,
                dumps_headers(response.headers),
                response.body,
                fingerprint,
                spider.name,
            )
            modified = self.conn.execute(self.update, tup).rowcount
            if modified == 0:
                self.conn.execute(self.insert, tup)
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
            status, url, headers_json, body = (
                self.conn.execute(self.query, (
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
        headers = loads_headers(headers_json)
        respcls = responsetypes.from_args(headers=headers, url=url)
        return respcls(url=url, headers=headers, status=status, body=body)