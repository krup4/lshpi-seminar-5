# -*- coding: utf-8 -*-
"""
cache_client.py — look-aside кеш над источником. КАРКАС: наивная версия РАБОТАЕТ,
чтобы вы увидели амплификацию своими глазами. Ваш фикс — в TODO задания 4.

Сценарий слота 1:
  1) прогрейте кеш, намерьте hit-rate (~99%) и QPS на источник (~1%) — источник спокоен;
  2) сбросьте кеш (flush) и пустите лавину запросов — увидите ×N поход в источник, он падает;
  3) почините: single-flight ИЛИ concurrency-лимит — и источник выживает.
"""

import asyncio


class LookAsideCache:
    def __init__(self, source, ttl=60.0):
        self.source = source
        self.ttl = ttl
        self.store = {}        # key -> value (TTL для семинара упрощаем)
        self.hits = 0
        self.misses = 0
        self._lock = asyncio.Lock()
        self._inflight = {}    # key -> asyncio.Task

    async def get(self, key):
        if key in self.store:
            self.hits += 1
            return self.store[key]

        self.misses += 1

        async with self._lock:
            if key in self.store:
                return self.store[key]

            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(self._load_from_source(key))
                self._inflight[key] = task
                owner = True
            else:
                owner = False

        try:
            return await task
        finally:
            if owner:
                async with self._lock:
                    if self._inflight.get(key) is task:
                        del self._inflight[key]

    async def _load_from_source(self, key):
        value = await self.source.get(key)
        self.store[key] = value
        return value

    def flush(self):
        """Сброс кеша (рестарт / истёк TTL / эвикция)."""
        self.store = {}
