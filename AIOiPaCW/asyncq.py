#!/usr/bin/env python3

import asyncio
import itertools as it
import os
import random
import time


async def make_item(size: int = 5) -> str:
    return os.urandom(size).hex()


async def rand_sleep(caller=None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)


async def produce(name: int, q: asyncio.Queue) -> None:
    n = random.randint(1, 5)
    for _ in it.repeat(None, n):
        await rand_sleep(caller=f"Producer {name}")
        i = await make_item()
        t = time.perf_counter()
        await q.put((i, t))
        print(f"Producer {name} added <{i}> to queue.")


async def consume(name: int, q: asyncio.Queue) -> None:
    while True:
        await rand_sleep(caller=f"Consumer {name}")
        i, t = await q.get()
        now = time.perf_counter()
        print(f"Consumer {name} got element <{i}> in {now-t:0.5f} seconds.")
        q.task_done()


async def main(n_producers: int, n_consumers: int):
    q = asyncio.Queue()
    producers = [asyncio.create_task(produce(n, q)) for n in range(n_producers)]
    consumers = [asyncio.create_task(consume(n, q)) for n in range(n_consumers)]
    await asyncio.gather(*producers)
    await q.join()
    for c in consumers:
        c.cancel()


if __name__ == "__main__":
    import argparse

    random.seed(444)
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--n-producers", type=int, default=5)
    parser.add_argument("-c", "--n-consumers", type=int, default=10)
    ns = parser.parse_args()
    start = time.perf_counter()
    asyncio.run(main(**ns.__dict__))
    elapsed = time.perf_counter() - start
    print(f"Program completed in {elapsed:0.5f} seconds.")
