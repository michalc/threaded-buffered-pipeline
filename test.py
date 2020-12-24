import asyncio
from unittest import (
    TestCase,
)

from threaded_buffered_pipeline import buffered_pipeline


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


class TestBufferIterable(TestCase):

    @async_test
    async def test_chain_all_buffered(self):
        async def gen_1():
            for value in range(0, 10):
                yield value

        async def gen_2(it):
            async for value in it:
                yield value * 2

        async def gen_3(it):
            async for value in it:
                yield value + 3

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))

        values = [value async for value in it_3]
        self.assertEqual(values, [3, 5, 7, 9, 11, 13, 15, 17, 19, 21])

    @async_test
    async def test_chain_some_buffered(self):
        async def gen_1():
            for value in range(0, 10):
                yield value

        async def gen_2(it):
            async for value in it:
                yield value * 2

        async def gen_3(it):
            async for value in it:
                yield value + 3

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = gen_2(it_1)
        it_3 = buffer_iterable(gen_3(it_2))

        values = [value async for value in it_3]
        self.assertEqual(values, [3, 5, 7, 9, 11, 13, 15, 17, 19, 21])

    @async_test
    async def test_chain_parallel(self):
        num_gen_1 = 0
        num_gen_2 = 0
        num_gen_3 = 0

        async def gen_1():
            nonlocal num_gen_1
            for value in range(0, 10):
                num_gen_1 += 1
                yield

        async def gen_2(it):
            nonlocal num_gen_2
            async for value in it:
                num_gen_2 += 1
                yield

        async def gen_3(it):
            nonlocal num_gen_3
            async for value in it:
                num_gen_3 += 1
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))

        num_done = []
        async for _ in it_3:
            # Slight hack to wait for buffers to be full
            await asyncio.sleep(0.02)
            num_done.append((num_gen_1, num_gen_2, num_gen_3))

        self.assertEqual(num_done, [
            (4, 3, 2), (5, 4, 3), (6, 5, 4), (7, 6, 5), (8, 7, 6),
            (9, 8, 7), (10, 9, 8), (10, 10, 9), (10, 10, 10), (10, 10, 10),
        ])

    @async_test
    async def test_num_tasks(self):
        async def gen_1():
            for value in range(0, 10):
                yield

        async def gen_2(it):
            async for value in it:
                yield

        async def gen_3(it):
            async for value in it:
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))

        num_tasks = []
        async for _ in it_3:
            # Slight hack to wait for buffers to be full
            await asyncio.sleep(0.02)
            num_tasks.append(len(asyncio.all_tasks()))

        self.assertEqual(num_tasks, [4, 4, 4, 4, 4, 4, 4, 3, 2, 1])

    @async_test
    async def test_exception_propagates(self):
        class MyException(Exception):
            pass

        async def gen_1():
            for value in range(0, 10):
                yield

        async def gen_2(it):
            async for value in it:
                yield

        async def gen_3(it):
            async for value in it:
                yield
                raise MyException()

        async def gen_4(it):
            async for value in it:
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))
        it_4 = buffer_iterable(gen_4(it_3))

        with self.assertRaises(MyException):
            async for _ in it_4:
                pass

        self.assertEqual(1, len(asyncio.all_tasks()))

    @async_test
    async def test_cancellation_propagates(self):
        event = asyncio.Event()

        async def gen_1():
            for value in range(0, 10):
                yield

        async def gen_2(it):
            async for value in it:
                yield

        async def gen_3(it):
            async for value in it:
                yield
                event.set()
                await asyncio.Future()

        async def gen_4(it):
            async for value in it:
                yield

        async def pipeline():
            buffer_iterable = buffered_pipeline()
            it_1 = buffer_iterable(gen_1())
            it_2 = buffer_iterable(gen_2(it_1))
            it_3 = buffer_iterable(gen_3(it_2))
            it_4 = buffer_iterable(gen_4(it_3))
            [value async for value in it_4]

        task = asyncio.create_task(pipeline())
        await event.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        await asyncio.sleep(0)
        self.assertEqual(1, len(asyncio.all_tasks()))

    @async_test
    async def test_default_bufsize(self):
        num_gen = 0

        def get_value():
            nonlocal num_gen
            num_gen += 1
            return 1

        async def gen_1():
            for _ in range(0, 10):
                yield get_value()

        num_gens = []
        buffer_iterable = buffered_pipeline()
        async for _ in buffer_iterable(gen_1()):
            # Slight hack to wait for buffers to be full
            await asyncio.sleep(0.02)
            num_gens.append(num_gen)

        self.assertEqual(num_gens, [2, 3, 4, 5, 6, 7, 8, 9, 10, 10])

    @async_test
    async def test_bigger_bufsize(self):
        num_gen = 0

        def get_value():
            nonlocal num_gen
            num_gen += 1
            return 1

        async def gen_1():
            for _ in range(0, 10):
                yield get_value()

        num_gens = []
        buffer_iterable = buffered_pipeline()
        async for _ in buffer_iterable(gen_1(), buffer_size=2):
            # Slight hack to wait for buffers to be full
            await asyncio.sleep(0.02)
            num_gens.append(num_gen)

        self.assertEqual(num_gens, [3, 4, 5, 6, 7, 8, 9, 10, 10, 10])
