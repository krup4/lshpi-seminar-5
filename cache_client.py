# -*- coding: utf-8 -*-
"""
cache_client.py — look-aside кеш над источником. КАРКАС: наивная версия РАБОТАЕТ,
чтобы вы увидели амплификацию своими глазами. Ваш фикс — в TODO задания 4.

Сценарий слота 1:
  1) прогрейте кеш, намерьте hit-rate (~99%) и QPS на источник (~1%) — источник спокоен;
  2) сбросьте кеш (flush) и пустите лавину запросов — увидите ×N поход в источник, он падает;
  3) почините: single-flight ИЛИ concurrency-лимит — и источник выживает.
"""


class LookAsideCache:
    def __init__(self, source, ttl=60.0):
        self.source = source
        self.ttl = ttl
        self.store = {}        # key -> value (TTL для семинара упрощаем)
        self.hits = 0
        self.misses = 0

    async def get(self, key):
        if key in self.store:
            self.hits += 1
            return self.store[key]

        # ПРОМАХ. Наивно: каждый промах идёт в источник напрямую.
        # Когда кеш пуст, а запросов по одному ключу много и одновременно — все они
        # ломятся в источник РАЗОМ (амплификация ×N). Источник деградирует и падает.
        #
        # TODO (задание 4): не дать всем промахам по одному ключу одновременно идти в источник.
        #   Вариант А (single-flight): на ключ — один поход, остальные ждут его результат.
        #   Вариант Б (concurrency-лимит): семафор на число одновременных походов в источник.
        self.misses += 1
        value = await self.source.get(key)
        self.store[key] = value
        return value

    def flush(self):
        """Сброс кеша (рестарт / истёк TTL / эвикция)."""
        self.store = {}
