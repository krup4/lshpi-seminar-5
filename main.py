import asyncio
from source_service import Source
from cache_client import LookAsideCache
from measure import warmup, blast, hit_rate

async def main():
    keys = ["k%d" % i for i in range(5)]
    src = Source(capacity=10)
    cache = LookAsideCache(src)
    await warmup(cache, keys)
    for _ in range(100):                 # штатный поток -> высокий hit-rate
        for k in keys:
            await cache.get(k)
    print("hit-rate", hit_rate(cache), "| поход в источник", src.calls)   # ~0.99 | 5

    src.reset(); cache.flush()           # уронили кеш
    ok, err = await blast(cache, keys, copies=40)   # лавина из 200 одновременных промахов
    print("поход в источник", src.calls, "| ошибок", src.errors)          # 200 | >0  (×40, источник падает)

asyncio.run(main())