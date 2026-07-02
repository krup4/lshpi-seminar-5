import os

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/mplconfig")

from queue_sim import simulate, summary, plot


DURATION = 300
SPIKE = (100, 120)

# Нормальная система: 12 воркеров успевают обрабатывать 9 заявок/сек,
# но короткое замедление в SPIKE запускает очередь, таймауты и ретраи.
meta = dict(
    base_rate=9.0,
    service=1.0,
    n_servers=12,
    patience=8,
    retry=True,
    orphan=True,
    retry_delay=12,
    w_ramp=(SPIKE[0], SPIKE[1], 2.2),
    seed=4,
)

cases = [
    (
        "bad",
        "Метастабильность без лечения",
        {},
        "slot2_bad.png",
    ),
    (
        "concurrency_limit",
        "Лечение: admission control",
        {"concurrency_limit": 12},
        "slot2_concurrency_limit.png",
    ),
    (
        "queue_cap",
        "Лечение: load shedding",
        {"queue_cap": 20},
        "slot2_queue_cap.png",
    ),
    (
        "disable_retry_plus_cap",
        "Лечение: circuit breaker + load shedding",
        {"disable_retry_at": 120, "queue_cap": 20},
        "slot2_disable_retry_plus_cap.png",
    ),
]


for name, title, params, path in cases:
    res = simulate(DURATION, **meta, **params)
    print(name, summary(res))
    plot(
        res,
        title,
        path,
        spike_span=SPIKE,
        disable_at=params.get("disable_retry_at"),
    )
