import threading
import time
from unittest import (
    TestCase,
)

from threaded_buffered_pipeline import buffered_pipeline


class TestBufferIterable(TestCase):

    def test_chain_all_buffered(self):
        def gen_1():
            for value in range(0, 10):
                yield value

        def gen_2(it):
            for value in it:
                yield value * 2

        def gen_3(it):
            for value in it:
                yield value + 3

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))

        self.assertEqual(list(it_3), [3, 5, 7, 9, 11, 13, 15, 17, 19, 21])

    def test_chain_some_buffered(self):
        def gen_1():
            for value in range(0, 10):
                yield value

        def gen_2(it):
            for value in it:
                yield value * 2

        def gen_3(it):
            for value in it:
                yield value + 3

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = gen_2(it_1)
        it_3 = buffer_iterable(gen_3(it_2))

        self.assertEqual(list(it_3), [3, 5, 7, 9, 11, 13, 15, 17, 19, 21])

    def test_chain_parallel(self):
        num_gen_1 = 0
        num_gen_2 = 0
        num_gen_3 = 0

        def gen_1():
            nonlocal num_gen_1
            for value in range(0, 10):
                num_gen_1 += 1
                yield

        def gen_2(it):
            nonlocal num_gen_2
            for value in it:
                num_gen_2 += 1
                yield

        def gen_3(it):
            nonlocal num_gen_3
            for value in it:
                num_gen_3 += 1
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))

        num_done = []
        for _ in it_3:
            # Slight hack to wait for buffers to be full
            time.sleep(0.1)
            num_done.append((num_gen_1, num_gen_2, num_gen_3))

        self.assertEqual(num_done, [
            (4, 3, 2), (5, 4, 3), (6, 5, 4), (7, 6, 5), (8, 7, 6),
            (9, 8, 7), (10, 9, 8), (10, 10, 9), (10, 10, 10), (10, 10, 10),
        ])

    def test_num_threads(self):
        def gen_1():
            for value in range(0, 10):
                yield

        def gen_2(it):
            for value in it:
                yield

        def gen_3(it):
            for value in it:
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))

        num_threads = []
        for _ in it_3:
            # Slight hack to wait for buffers to be full
            time.sleep(0.1)
            num_threads.append(threading.active_count())

        self.assertEqual(num_threads, [4, 4, 4, 4, 4, 4, 4, 3, 2, 1])

    def test_exception_propagates_after_first_yield(self):
        num_gen = 0

        class MyException(Exception):
            pass

        def gen_1():
            nonlocal num_gen

            for value in range(0, 10):
                num_gen += 1
                yield

        def gen_2(it):
            for value in it:
                yield

        def gen_3(it):
            for value in it:
                yield
                raise MyException()

        def gen_4(it):
            for value in it:
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))
        it_4 = buffer_iterable(gen_4(it_3))

        with self.assertRaises(MyException):
            for _ in it_4:
                pass

        self.assertEqual(1, threading.active_count())
        self.assertEqual(num_gen, 3)

    def test_exception_propagates_before_first_yield(self):
        num_gen = 0

        class MyException(Exception):
            pass

        def gen_1():
            nonlocal num_gen

            for value in range(0, 10):
                num_gen += 1
                yield

        def gen_2(it):
            for value in it:
                yield

        def gen_3(it):
            for value in it:
                raise MyException()
                yield

        def gen_4(it):
            for value in it:
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))
        it_4 = buffer_iterable(gen_4(it_3))

        with self.assertRaises(MyException):
            for _ in it_4:
                pass

        self.assertEqual(1, threading.active_count())
        self.assertEqual(num_gen, 2)

    def test_exception_propagates_before_first_iter(self):
        num_gen = 0

        class MyException(Exception):
            pass

        def gen_1():
            nonlocal num_gen

            for value in range(0, 10):
                num_gen += 1
                yield

        def gen_2(it):
            for value in it:
                yield

        def gen_3(it):
            time.sleep(0.1)  # Slight hack to make sure previous buffers are full
            raise MyException()
            yield

        def gen_4(it):
            for value in it:
                yield

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))
        it_4 = buffer_iterable(gen_4(it_3))

        with self.assertRaises(MyException):
            for _ in it_4:
                pass

        self.assertEqual(1, threading.active_count())
        self.assertEqual(num_gen, 2)

    def test_exception_propagates_slow_previous_iterable(self):
        num_gen = 0

        class MyException(Exception):
            pass

        def gen_1():
            nonlocal num_gen

            for value in range(0, 10):
                num_gen += 1
                yield value
                if value == 2:
                    time.sleep(2)

        def gen_2(it):
            for value in it:
                time.sleep(1)
                yield value

        def gen_3(it):
            for value in it:
                if value == 2:
                    time.sleep(0.1)  # Slight hack to make sure previous buffers are full
                    raise MyException()
                yield value

        def gen_4(it):
            for value in it:
                yield value

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())
        it_2 = buffer_iterable(gen_2(it_1))
        it_3 = buffer_iterable(gen_3(it_2))
        it_4 = buffer_iterable(gen_4(it_3))

        with self.assertRaises(MyException):
            for _ in it_4:
                pass

        self.assertEqual(1, threading.active_count())
        self.assertEqual(num_gen, 4)

    def test_default_bufsize(self):
        num_gen = 0

        def get_value():
            nonlocal num_gen
            num_gen += 1
            return 1

        def gen_1():
            for _ in range(0, 10):
                yield get_value()

        num_gens = []
        buffer_iterable = buffered_pipeline()
        for _ in buffer_iterable(gen_1()):
            # Slight hack to wait for buffers to be full
            time.sleep(0.1)
            num_gens.append(num_gen)

        self.assertEqual(num_gens, [2, 3, 4, 5, 6, 7, 8, 9, 10, 10])

    def test_iteration_starts_immediately(self):
        num_gen = 0

        def gen_1():
            nonlocal num_gen
            for i in range(0, 10):
                num_gen += 1
                yield i

        buffer_iterable = buffered_pipeline()
        it_1 = buffer_iterable(gen_1())

        time.sleep(0.1)  # Slight hack to wait for iteration
        self.assertEqual(num_gen, 1)

        for _ in it_1:
            pass

    def test_bigger_bufsize(self):
        num_gen = 0

        def get_value():
            nonlocal num_gen
            num_gen += 1
            return 1

        def gen_1():
            for _ in range(0, 10):
                yield get_value()

        num_gens = []
        buffer_iterable = buffered_pipeline()
        for _ in buffer_iterable(gen_1(), buffer_size=2):
            # Slight hack to wait for buffers to be full
            time.sleep(0.1)
            num_gens.append(num_gen)

        self.assertEqual(num_gens, [3, 4, 5, 6, 7, 8, 9, 10, 10, 10])
