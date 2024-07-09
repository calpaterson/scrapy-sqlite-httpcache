# scrapy-sqlite-httpcache

Scrapy includes [HTTP cache
middleware](https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#module-scrapy.downloadermiddlewares.httpcache)
but it only offers file-based or DBM-based storage.

This module implements storage in a Sqlite database.

It also implements an alternate cache policy that is controlled via scrapy configuration.
