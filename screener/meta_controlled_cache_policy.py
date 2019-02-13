from logging import getLogger
from time import time

from scrapy.utils.httpobj import urlparse_cached
from scrapy.extensions.httpcache import rfc1123_to_epoch

logger = getLogger(__name__)

class MetaControlledCachePolicy(object):
    def __init__(self, settings):
        self.ignore_schemes = settings.getlist('HTTPCACHE_IGNORE_SCHEMES')
        self.ignore_http_codes = [int(x) for x in settings.getlist('HTTPCACHE_IGNORE_HTTP_CODES')]

    def should_cache_request(self, request):
        return urlparse_cached(request).scheme not in self.ignore_schemes

    def should_cache_response(self, response, request):
        return response.status not in self.ignore_http_codes

    def is_cached_response_fresh(self, response, request):
        max_expiration_secs = request.meta.get("max_expiration_secs", None)
        if max_expiration_secs is None:
            logger.debug("cache response fresh: %s", response.url)
            return True
        else:
            now = time()
            t = rfc1123_to_epoch(response.headers.get(b"Date"))
            elapsed = now - t
            logger.debug("elapsed: %d, allowed: %d", elapsed, max_expiration_secs)
            is_fresh = elapsed <= max_expiration_secs
            logger.debug(
                "cache response %s (elapsed: %d, allowed: %d): %s",
                "fresh enough" if is_fresh else "stale",
                elapsed,
                max_expiration_secs,
                response.url)
            return is_fresh

    def is_cached_response_valid(self, cachedresponse, response, request):
        # Never use the cached response over a new response
        return False
