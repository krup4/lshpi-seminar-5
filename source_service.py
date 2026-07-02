# -*- coding: utf-8 -*-
"""
source_service.py — «источник данных» (учебная база) с ёмкостью C. ВЫДАЁТСЯ ГОТОВЫМ.

Считает входящий поток. Пока одновременных запросов <= C — отвечает быстро. Сверх C —
деградирует (растёт задержка), а сверх 2*C — начинает падать (SourceOverloaded ~ HTTP 500).
Именно это вы и увидите при потере кеша: лавина промахов гасит источник.
"""

import asyncio


class SourceOverloaded(Exception):
    pass


class Source:
    def __init__(self, capacity=10, base_latency=0.01):
        self.capacity = capacity
        self.base_latency = base_latency
        self.calls = 0          # сколько раз сходили в источник (метрика амплификации)
        self.errors = 0
        self.inflight = 0
        self.max_inflight = 0

    async def get(self, key):
        self.calls += 1
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            over = self.inflight - self.capacity
            latency = self.base_latency * (1.0 + max(0, over) * 0.5)   # деградация под нагрузкой
            if self.inflight > self.capacity * 2:
                self.errors += 1
                await asyncio.sleep(latency)
                raise SourceOverloaded(
                    "inflight=%d > 2*capacity=%d" % (self.inflight, self.capacity * 2))
            await asyncio.sleep(latency)
            return "value:%s" % key
        finally:
            self.inflight -= 1

    def stats(self):
        return {"calls": self.calls, "errors": self.errors, "max_inflight": self.max_inflight}

    def reset(self):
        self.calls = self.errors = self.inflight = self.max_inflight = 0
