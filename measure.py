# -*- coding: utf-8 -*-
"""measure.py — замерялка для слота 1 (кеш/амплификация)."""

import asyncio


def hit_rate(cache):
    total = cache.hits + cache.misses
    return round(cache.hits / total, 4) if total else 0.0


async def warmup(cache, keys):
    """Прогреть кеш по ключам (по одному запросу на ключ)."""
    for k in keys:
        await cache.get(k)


async def blast(cache, keys, copies):
    """
    Лавина: len(keys)*copies ОДНОВРЕМЕННЫХ запросов (по copies на каждый ключ).
    Возвращает (ok, errors). Имитирует всплеск после потери кеша.
    """
    tasks = []
    for _ in range(copies):
        for k in keys:
            tasks.append(cache.get(k))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = sum(1 for r in results if not isinstance(r, Exception))
    errors = sum(1 for r in results if isinstance(r, Exception))
    return ok, errors
