# scrapy-sqlite-httpcache

Scrapy has an HTTP cache, provided by the
[`HttpCacheMiddleware`](https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware)
downloader middleware.

`HttpCacheMiddleware` even has pluggable backends to store the cache in
different places.  However only two backends are provided, a filesystem one and
a DBM one.

## `SQLiteCacheStorage`

This package provides an alternate SQLite backend that plugs in to the existing HttpCacheMiddleware.

Put these lines in your scrapy `settings.py`:

```python
HTTPCACHE_ENABLED = True # as usual
HTTPCACHE_DIR = "httpcache" # also as usual - optional; will be created if does not exist
HTTPCACHE_STORAGE = "scrapy_sqlite_httpcache.SQLiteCacheStorage"
```

## `MetaControlledCachePolicy`

This package also provides an alternate cache policy that allows the cache
validity to be set by the spider for each request.  For example:

```python
from datetime import timedelta
import scrapy

class SomeSpider(scrapy.Spider):
    def parse(self):
        yield scrapy.Request(
            url="...",
            meta={"expire": timedelta(days=3)}
        )
```

If a request younger than the `expire` value is found in the cache, it will be
returned.

To use this policy, set:

```python
HTTPCACHE_POLICY = "scrapy_sqlite_httpcache.MetaControlledCachePolicy"
HTTPCACHE_EXPIRATION_SECS = 86400 # this becomes the default, if nothing is in meta
```
