# scrapy-sqlite-httpcache

Scrapy includes [HTTP cache
middleware](https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#module-scrapy.downloadermiddlewares.httpcache)
but it only offers file-based or DBM-based storage.

This module implements storage in a SQLite database.  Put these lines in your scrapy `settings.py`:

```python
HTTPCACHE_ENABLED = True
HTTPCACHE_DIR = "httpcache" # optional, will be created if does not exist
HTTPCACHE_STORAGE = "scrapy_sqlite_httpcache.SQLiteCacheStorage"
```

It also implements an alternate cache policy that is controlled via scrapy configuration.
