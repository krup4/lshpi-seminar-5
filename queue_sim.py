# -*- coding: utf-8 -*-
"""
queue_sim.py — движок симулятора очереди (event-driven, heapq). ВЫДАЁТСЯ ГОТОВЫМ.

Студент НЕ пишет движок — он крутит параметры simulate(...) и включает лекарства.
Движок взят из генератора графиков лекции Л5 (draw_metastable.py) и моделирует:
  - очередь + N воркеров, время обслуживания W (можно растить во времени w_ramp);
  - нетерпеливых клиентов (patience: уходят, не дождавшись);
  - ретраи (ушедший возвращается новым заказом → амплификация);
  - осиротевшие заказы (orphan: клиент ушёл, а воркер всё равно его обслуживает → goodput 0);
  - лекарства: disable_retry_at, lifo, drop_stale (дедлайн), queue_cap (load shedding),
    concurrency_limit (admission control).

simulate(...) -> dict с посекундными рядами (в т.ч. 'goodput').
summary(res)  -> {goodput_end, goodput_min, queue_end, metastable}.
plot(res,...) -> 4-панельный PNG (RPS / timings / orders / goodput), тёмная тема.
"""

import os
import glob
import heapq
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

BG = "#1B1B1B"; FG = "#FFFFFF"; GRID = "#3A3A3A"
C_TOTAL = "#FFFFFF"; C_RETRY = "#FF4E50"; C_OTHER = "#8989FF"; C_GOOD = "#9DED12"
FONT_DIR = "/Users/kor-den/FKN/Шрифты Городские сервисы Яндекса"


def setup_fonts():
    family = None
    try:
        for ext in ("*.ttf", "*.otf"):
            for path in glob.glob(os.path.join(FONT_DIR, "**", ext), recursive=True):
                try:
                    font_manager.fontManager.addfont(path)
                except Exception:
                    pass
        names = {f.name for f in font_manager.fontManager.ttflist}
        for cand in ("YS Text", "YS Text Light", "YS Geo", "YS Display"):
            if cand in names:
                family = cand
                break
    except Exception:
        family = None
    plt.rcParams["font.family"] = family or "DejaVu Sans"
    return family


def _W_at(t, service, w_ramp):
    if not w_ramp:
        return service
    t0, t1, w1 = w_ramp
    if t <= t0:
        return service
    if t >= t1:
        return w1
    return service + (w1 - service) * (t - t0) / (t1 - t0)


def simulate(duration, base_rate, service, n_servers,
             patience=None, retry=False, retry_delay=12, orphan=True,
             service_cv=0.12, seed=0, queue_cap=None, lifo=False,
             drop_stale=None, disable_retry_at=None, w_ramp=None,
             concurrency_limit=None):
    """
    Прогон симуляции. Все «ручки» — параметры здесь.
      base_rate   — входящий RPS (новые клиенты в секунду)
      service     — базовое время обслуживания W (сек)
      n_servers   — число воркеров
      patience    — терпение клиента (сек) до ухода; None = бесконечно терпеливы
      retry       — ушедший клиент возвращается новым заказом (амплификация)
      orphan      — обслуживать ли заказ ушедшего клиента (впустую → goodput 0)
      w_ramp      — (t0,t1,w1): рост W с service до w1 в окне [t0,t1] (триггер аварии)
    Лекарства:
      disable_retry_at — с этого момента ретраи выключены (circuit breaker)
      lifo             — обслуживать свежие вперёд
      drop_stale       — выкидывать заказы старше N сек (дедлайн)
      queue_cap        — макс. длина очереди (load shedding)
      concurrency_limit— макс. число «в работе» на входе (admission control)
    """
    rng = np.random.default_rng(seed)
    D = int(duration)
    orders = {}
    seq = [0]

    def new_order(t, is_retry):
        oid = seq[0]; seq[0] += 1
        orders[oid] = {"enq": t, "start": None, "ab": False, "done": False, "retry": is_retry}
        return oid

    ev = []
    step = 1.0 / base_rate
    for at in np.arange(0.0, duration, step):
        heapq.heappush(ev, (float(at), 0, new_order(float(at), False)))

    busy = [0]
    q = []
    comps = []
    rps_new = np.zeros(D); rps_rty = np.zeros(D); rejected = np.zeros(D)
    state_log = [(0.0, 0, 0)]

    def start(oid, t):
        busy[0] += 1
        orders[oid]["start"] = t
        w = max(0.05, rng.normal(_W_at(t, service, w_ramp), service_cv * service))
        heapq.heappush(ev, (t + w, 1, oid))

    def log(t):
        state_log.append((t, len(q), busy[0]))

    while ev:
        t, kind, oid = heapq.heappop(ev)
        if t > duration:
            break
        if kind == 0:                       # приход
            o = orders[oid]
            b = int(t)
            if 0 <= b < D:
                (rps_rty if o["retry"] else rps_new)[b] += 1
            if concurrency_limit is not None and busy[0] >= concurrency_limit:
                if 0 <= b < D:
                    rejected[b] += 1
                continue
            if queue_cap is not None and len(q) >= queue_cap:
                if 0 <= b < D:
                    rejected[b] += 1
                continue
            if patience is not None:
                heapq.heappush(ev, (t + patience, 2, oid))
            if busy[0] < n_servers:
                start(oid, t)
            else:
                q.append(oid)
            log(t)
        elif kind == 1:                     # завершение обслуживания
            busy[0] -= 1
            o = orders[oid]; o["done"] = True
            comps.append((t, o["start"] - o["enq"], t - o["start"],
                          t - o["enq"], 0 if o["ab"] else 1))
            while q:
                nid = q.pop() if lifo else q.pop(0)
                no = orders[nid]
                if drop_stale is not None and (t - no["enq"]) > drop_stale:
                    no["done"] = True
                    continue
                start(nid, t)
                break
            log(t)
        else:                               # уход нетерпеливого клиента
            o = orders[oid]
            if o["done"]:
                continue
            o["ab"] = True
            if retry and (disable_retry_at is None or t < disable_retry_at):
                heapq.heappush(ev, (t + retry_delay, 0, new_order(t + retry_delay, True)))
            if not orphan and oid in q:
                q.remove(oid)
            log(t)

    # --- посекундная агрегация ---
    secs = np.arange(D)
    rps_total = rps_new + rps_rty
    wb = [[] for _ in range(D)]; sb = [[] for _ in range(D)]
    tb = [[] for _ in range(D)]; gb = [[] for _ in range(D)]
    for ft, wt, sv, tt, good in comps:
        b = int(ft)
        if 0 <= b < D:
            wb[b].append(wt); sb[b].append(sv); tb[b].append(tt); gb[b].append(good)

    def p90_ff(bins):
        out = np.zeros(D); last = 0.0
        for i in range(D):
            if bins[i]:
                last = float(np.percentile(bins[i], 90))
            out[i] = last
        return out

    def ratio_ff(bins):
        out = np.zeros(D); last = 1.0
        for i in range(D):
            if bins[i]:
                last = float(np.mean(bins[i]))
            out[i] = last
        return out

    sl = sorted(state_log)
    queued = np.zeros(D); inflight = np.zeros(D)
    j = 0; cq = 0; cf = 0
    for i in range(D):
        while j < len(sl) and sl[j][0] <= i:
            cq, cf = sl[j][1], sl[j][2]; j += 1
        queued[i] = cq; inflight[i] = cf

    return {
        "secs": secs, "rps_total": rps_total, "rps_retry": rps_rty,
        "rps_other": rps_new, "rejected": rejected,
        "wait": p90_ff(wb), "serve": p90_ff(sb), "total": p90_ff(tb),
        "q_queued": queued, "q_inflight": inflight, "q_total": queued + inflight,
        "goodput": ratio_ff(gb),
    }


def summary(res, tail=20):
    """Свести прогон к цифрам: что с goodput и очередью в конце; впала ли в метастабильность."""
    g = res["goodput"]
    gp_end = float(np.mean(g[-tail:]))
    return {
        "goodput_end": round(gp_end, 3),
        "goodput_min": round(float(np.min(g)), 3),
        "queue_end": round(float(np.mean(res["q_total"][-tail:])), 1),
        "metastable": gp_end < 0.3,
    }


def plot(res, title, path, spike_span=None, disable_at=None):
    secs = res["secs"]
    fig, axes = plt.subplots(4, 1, figsize=(12.5, 8.4), sharex=True, facecolor=BG)
    fig.subplots_adjust(left=0.07, right=0.80, top=0.92, bottom=0.07, hspace=0.28)

    def style(ax, ylabel):
        ax.set_facecolor(BG)
        ax.set_ylabel(ylabel, color=FG, fontsize=10)
        ax.tick_params(colors=FG, labelsize=8)
        for s in ax.spines.values():
            s.set_color(GRID)
        ax.grid(True, color=GRID, linewidth=0.6, alpha=0.8)
        if spike_span:
            ax.axvspan(spike_span[0], spike_span[1], color=C_RETRY, alpha=0.16, lw=0)
        if disable_at is not None:
            ax.axvline(disable_at, color=C_GOOD, lw=1.0, ls=(0, (4, 3)))

    a0, a1, a2, a3 = axes
    style(a0, "RPS")
    a0.plot(secs, res["rps_retry"], color=C_RETRY, lw=1.2, label="retries")
    a0.plot(secs, res["rps_other"], color=C_OTHER, lw=1.2, label="not retries")
    a0.plot(secs, res["rps_total"], color=C_TOTAL, lw=1.2, label="total")
    a0.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, labelcolor=FG, fontsize=8)
    style(a1, "timings p90, s")
    a1.plot(secs, res["wait"], color=C_RETRY, lw=1.2, label="wait")
    a1.plot(secs, res["serve"], color=C_OTHER, lw=1.2, label="serve")
    a1.plot(secs, res["total"], color=C_TOTAL, lw=1.2, label="total")
    a1.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, labelcolor=FG, fontsize=8)
    style(a2, "orders")
    a2.plot(secs, res["q_queued"], color=C_RETRY, lw=1.2, label="queued")
    a2.plot(secs, res["q_inflight"], color=C_OTHER, lw=1.2, label="in flight")
    a2.plot(secs, res["q_total"], color=C_TOTAL, lw=1.2, label="total")
    a2.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, labelcolor=FG, fontsize=8)
    style(a3, "goodput (КПД)")
    a3.plot(secs, res["goodput"], color=C_GOOD, lw=1.5, label="goodput")
    a3.set_ylim(-0.05, 1.05)
    a3.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, labelcolor=FG, fontsize=8)
    a3.set_xlabel("time, seconds", color=FG, fontsize=11)

    fig.suptitle(title, color=FG, fontsize=15, x=0.07, ha="left")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fig.savefig(path, facecolor=BG, dpi=160)
    plt.close(fig)
    return path


if __name__ == "__main__":
    setup_fonts()
    base = simulate(300, 9.0, 1.0, 12, patience=8, retry=True, orphan=True)
    print("без триггера:", summary(base))
    meta = simulate(300, 9.0, 1.0, 12, patience=8, retry=True, orphan=True, w_ramp=(100, 120, 2.2), seed=4)
    print("метастабильность:", summary(meta))
