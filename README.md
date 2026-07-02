# lab — заготовки семинара Л5 «Амплификация и метастабильность»

Два слота, оба — *руками и глазами*. Слот 1: увидеть ×N на источник при потере кеша и
обуздать. Слот 2: на симуляторе загнать очередь в метастабильность (goodput→0) и вытащить
лекарством.

## Установка

```bash
pip3 install --user --break-system-packages numpy matplotlib pytest
```

## Слот 1 — амплификация кеша

Файлы: `source_service.py` (источник с ёмкостью C, готов), `cache_client.py` (look-aside кеш —
**вы дописываете фикс**), `measure.py` (`warmup`, `blast`, `hit_rate`).

```python
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
```

Амплификация = число одновременных промахов по ключу (`copies`). Ваша задача (задание 4) —
обуздать её: **single-flight** (один поход на ключ) или **concurrency-лимит** (семафор).

## Слот 2 — метастабильность (симулятор)

Файл: `queue_sim.py` — движок выдан готовым, вы **крутите параметры** `simulate(...)` и
включаете лекарства. Не пишете движок.

```python
from queue_sim import simulate, summary, plot

meta = dict(base_rate=9.0, service=1.0, n_servers=12,
            patience=8, retry=True, orphan=True, w_ramp=(100, 120, 2.2), seed=4)
print("метастабильность:", summary(simulate(300, **meta)))                    # goodput_end ~0
print("лекарство (лимит):", summary(simulate(300, concurrency_limit=12, **meta)))  # goodput ~1
plot(simulate(300, **meta), "Метастабильность", "meta.png", spike_span=(100, 120))
```

Ручки лекарств: `disable_retry_at` (circuit breaker), `lifo`, `drop_stale` (дедлайн),
`queue_cap` (load shedding), `concurrency_limit` (admission control). См. докстринг `simulate`.
