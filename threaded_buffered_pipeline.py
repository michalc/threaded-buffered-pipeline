import collections
import threading


def buffered_pipeline():
    threads = []

    # The regular queue.Queue doesn't have a function to wait for space in the queue without also
    # immediately putting an item into it, which would mean effective minimum buffer_size is 2: an
    # item in the queue and in memory waiting to put into it. To allow a buffer_size of 1, we need
    # to check there is space _before_ fetching the item from upstream. This seems to require a
    # custom queue implementation.
    #
    # We also can guarantee there will be at most one getter and putter at any one time, and that
    # _put won't be called until there is space in the queue, so we can have much simpler code than
    # Queue
    class ThreadWithQueue(threading.Thread):
        def __init__(self, *args, buffer_size, **kwargs):
            super().__init__(*args, **kwargs)
            self._buffer_size = buffer_size

            self._queue = collections.deque()

            self._queue_lock = threading.Lock()
            self._has_items_or_stopped = threading.Event()
            self._has_space_or_stopped = threading.Event()
            self._has_space_or_stopped.set()

            self._stopped = False

        def queue_wait_until_space_or_stopped(self):
            self._has_space_or_stopped.wait()

        def queue_wait_until_has_items_or_stopped(self):
            self._has_items_or_stopped.wait()

        def queue_get(self):
            with self._queue_lock:
                value = self._queue.popleft()
                self._has_space_or_stopped.set()
                if not self._queue and not self._stopped:
                    # Only the same thread that calls queue_get waits on this event
                    self._has_items_or_stopped = threading.Event()
                return value

        def queue_put(self, item):
            with self._queue_lock:
                self._queue.append(item)
                self._has_items_or_stopped.set()
                if len(self._queue) >= self._buffer_size and not self._stopped:
                    # Only the same thread that calls queue_put waits on this event
                    self._has_space_or_stopped = threading.Event()

        def queue_stop(self):
            with self._queue_lock:
                self._stopped = True
                self._has_items_or_stopped.set()
                self._has_space_or_stopped.set()

        def queue_stopped(self):
            with self._queue_lock:
                return self._stopped

    def _buffer_iterable(iterable, buffer_size=1):

        def _iterate():
            iterator = iter(iterable)
            thread = threading.current_thread()
            try:
                while True:
                    thread.queue_wait_until_space_or_stopped()
                    if thread.queue_stopped():
                        break
                    value = next(iterator)
                    thread.queue_put((None, value))
                    value = None  # So value can be garbage collected
            except Exception as exception:
                thread.queue_put((exception, None))

        thread = ThreadWithQueue(target=_iterate, buffer_size=buffer_size)
        thread.start()
        index = len(threads)
        threads.append(thread)

        try:
            while True:
                thread.queue_wait_until_has_items_or_stopped()
                if thread.queue_stopped():
                    break
                exception, value = thread.queue_get()
                if exception is not None:
                    raise exception from None
                yield value
                value = None  # So value can be garbage collected
        except StopIteration:
            pass
        except Exception as exception:
            # Stop threads earlier in the pipeline, which are actually created later, since they
            # are created when "pulling" from earlier iterables. The later threads are stopped
            # by the propagation of exceptions
            for thread in threads[index + 1:]:
                thread.queue_stop()
                thread.join()
            raise

    return _buffer_iterable
