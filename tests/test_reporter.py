# Modified by SignalFx
# Copyright (c) 2016 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, absolute_import
from six.moves import range

import logging
from threading import Event
import time
import collections

import mock
import jaeger_client.reporter
from jaeger_client import thrift

from jaeger_client import Span, SpanContext
from jaeger_client.metrics import LegacyMetricsFactory, Metrics
from jaeger_client.utils import ErrorReporter
from jaeger_client.reporter import Reporter
from jaeger_client.senders import Sender
from .conftest import wait_for


def test_null_reporter():
    reporter = jaeger_client.reporter.NullReporter()
    reporter.report_span({})
    assert reporter.close() is True


def test_in_memory_reporter():
    reporter = jaeger_client.reporter.InMemoryReporter()
    reporter.report_span({})
    assert reporter.close() is True
    spans = reporter.get_spans()
    assert [{}] == spans


def test_logging_reporter():
    log_mock = mock.MagicMock()
    reporter = jaeger_client.reporter.LoggingReporter(logger=log_mock)
    reporter.report_span({})
    log_mock.info.assert_called_with('Reporting span %s', {})
    assert reporter.close() is True


def test_composite_reporter():
    reporter = jaeger_client.reporter.CompositeReporter(
        jaeger_client.reporter.NullReporter(),
        jaeger_client.reporter.LoggingReporter())
    with mock.patch('jaeger_client.reporter.NullReporter.set_process') \
            as null_mock:
        with mock.patch('jaeger_client.reporter.LoggingReporter.set_process') \
                as log_mock:
            reporter.set_process('x', {}, 123)
            null_mock.assert_called_with('x', {}, 123)
            log_mock.assert_called_with('x', {}, 123)
    with mock.patch('jaeger_client.reporter.NullReporter.report_span') \
            as null_mock:
        with mock.patch('jaeger_client.reporter.LoggingReporter.report_span') \
                as log_mock:
            reporter.report_span({})
            null_mock.assert_called_with({})
            log_mock.assert_called_with({})
    with mock.patch('jaeger_client.reporter.NullReporter.close') \
            as null_mock:
        with mock.patch('jaeger_client.reporter.LoggingReporter.close') \
                as log_mock:

            null_mock.return_value = True
            log_mock.return_value = True
            assert reporter.close()
            null_mock.assert_called_once()
            log_mock.assert_called_once()


class FakeSender(object):
    """
    Mock the send() method of the reporter's Sender by capturing requests
    and returning incomplete futures that can be completed from
    inside the test.
    """
    def __init__(self):
        self.requests = []
        self.futures = []

    def __call__(self, batch):
        self.requests.append(batch)
        fut = Event()
        self.futures.append(fut)

        def waiter():
            fut.wait()
            return 1

        return waiter()


class HardErrorReporter(object):
    def error(self, *args):
        raise ValueError(*args)


FakeTrace = collections.namedtuple(
    'FakeTracer', ['ip_address', 'service_name'])


class FakeMetricsFactory(LegacyMetricsFactory):
    def __init__(self):
        super(FakeMetricsFactory, self).__init__(
            Metrics(count=self._incr_count)
        )
        self.counters = {}

    def _incr_count(self, key, value):
        self.counters[key] = value + self.counters.get(key, 0)


class TestReporter(object):

    @staticmethod
    def _new_span(name):
        tracer = FakeTrace(ip_address='127.0.0.1',
                           service_name='reporter_test')
        ctx = SpanContext(trace_id=1,
                          span_id=1,
                          parent_id=None,
                          flags=1)
        span = Span(context=ctx,
                    tracer=tracer,
                    operation_name=name)
        span.start_time = time.time()
        span.end_time = span.start_time + 0.001  # 1ms
        return span

    @staticmethod
    def _new_reporter(request, batch_size, flush=None, queue_cap=100, send_method=None):
        reporter = Reporter(sender=Sender(batch_size),
                            flush_interval=flush,
                            metrics_factory=FakeMetricsFactory(),
                            error_reporter=HardErrorReporter(),
                            queue_capacity=queue_cap)
        reporter.set_process('service', {}, max_length=0)
        send_method = send_method or FakeSender()
        reporter._sender.send = send_method
        request.addfinalizer(reporter.close)
        return reporter, send_method

    def test_submit_batch_size_1(self, request):
        reporter, sender = self._new_reporter(request, batch_size=1)

        reporter.report_span(self._new_span('1'))

        wait_for(lambda: len(sender.futures) > 0)
        assert 1 == len(sender.futures)

        sender.futures[0].set()
        reporter.close()
        assert 1 == len(sender.futures)

        # send after close
        span_dropped_key = 'jaeger:reporter_spans.result_dropped'
        assert span_dropped_key not in reporter.metrics_factory.counters
        reporter.report_span(self._new_span('1'))
        assert 1 == reporter.metrics_factory.counters[span_dropped_key]
        assert 1 == len(sender.futures)

    def test_sender_flushed_with_partial_batch(self, request):
        reporter, sender = self._new_reporter(request, batch_size=100, flush=.5)
        for i in range(50):
            reporter.report_span(self._new_span(str(i)))

        wait_for(lambda: len(sender.futures) > 0)
        assert 1 == len(sender.futures)
        sender.futures[0].set()

        for future in sender.futures[1:]:
            future.set()

        span_sent_key = 'jaeger:reporter_spans.result_ok'
        wait_for(lambda: span_sent_key in reporter.metrics_factory.counters)
        assert reporter.metrics_factory.counters[span_sent_key] == 50

    def test_sender_flush_errors_handled_with_partial_batch(self, request):
        reporter, sender = self._new_reporter(request, batch_size=100, flush=.5)
        reporter.error_reporter = ErrorReporter(logger=logging.getLogger())
        reporter._sender.send = mock.MagicMock(side_effect=ValueError())
        for i in range(50):
            reporter.report_span(self._new_span(str(i)))

        reporter_failure_key = 'jaeger:reporter_spans.result_err'
        wait_for(lambda: reporter_failure_key in reporter.metrics_factory.counters)
        assert reporter.metrics_factory.counters[reporter_failure_key] == 50

    def test_submit_failure(self, request):
        reporter, sender = self._new_reporter(request, batch_size=1)
        reporter.error_reporter = ErrorReporter(logger=logging.getLogger())

        reporter_failure_key = 'jaeger:reporter_spans.result_err'
        assert reporter_failure_key not in reporter.metrics_factory.counters

        reporter._sender.send = mock.MagicMock(side_effect=ValueError())
        reporter.report_span(self._new_span('1'))
        wait_for(lambda: reporter_failure_key in reporter.metrics_factory.counters)
        assert 1 == reporter.metrics_factory.counters.get(reporter_failure_key)

    def test_submit_queue_full_batch_size_1(self, request):
        reporter, sender = self._new_reporter(request, batch_size=1, queue_cap=1)
        reporter.report_span(self._new_span('1'))

        wait_for(lambda: len(sender.futures) > 0)
        assert 1 == len(sender.futures)
        # the consumer is blocked on a future, so won't drain the queue
        reporter.report_span(self._new_span('2'))
        span_dropped_key = 'jaeger:reporter_spans.result_dropped'
        assert span_dropped_key not in reporter.metrics_factory.counters
        reporter.report_span(self._new_span('3'))
        wait_for(lambda: span_dropped_key in reporter.metrics_factory.counters)
        assert 1 == reporter.metrics_factory.counters.get(span_dropped_key)
        # let it drain the queue
        sender.futures[0].set()
        wait_for(lambda: len(sender.futures) > 1)
        assert 2 == len(sender.futures)

        sender.futures[1].set()
        reporter.close()

    def test_submit_batch_size_2(self, request):
        reporter, sender = self._new_reporter(request, batch_size=2, flush=0.01)
        reporter.report_span(self._new_span('1'))
        time.sleep(0.001)
        assert 0 == len(sender.futures)

        reporter.report_span(self._new_span('2'))
        wait_for(lambda: len(sender.futures) > 0)
        assert 1 == len(sender.futures)
        assert 2 == len(sender.requests[0].spans)
        sender.futures[0].set()

        # 3rd span will not be submitted right away, but after `flush` interval
        reporter.report_span(self._new_span('3'))
        time.sleep(0.001)
        assert 1 == len(sender.futures)
        time.sleep(0.001)
        assert 1 == len(sender.futures)
        time.sleep(0.01)
        assert 2 == len(sender.futures)
        sender.futures[1].set()

        reporter.close()

    def test_close_drains_queue(self, request):
        reporter, sender = self._new_reporter(request, batch_size=1, flush=5)
        reporter.report_span(self._new_span('0'))

        wait_for(lambda: len(sender.futures) > 0)
        assert 1 == len(sender.futures)

        # now that the consumer is blocked on the first future.
        # let's reset Send to actually respond right away
        # and flood the queue with messages
        count = [0]

        def send(_):
            count[0] += 1
            return 1

        reporter._sender.send = send
        reporter._sender.batch_size = 3
        for i in range(10):
            reporter.report_span(self._new_span('%s' % i))
        assert reporter.queue.qsize() == 10, 'queued 10 spans'

        # now unblock consumer
        sender.futures[0].set()
        assert wait_for(lambda: count[0] > 2)

        assert count[0] == 3, '9 out of 10 spans submitted in 3 batches'
        assert wait_for(
            lambda: reporter.queue.unfinished_tasks == 1
        ), 'one span still pending'

        reporter.close()
        assert reporter.queue.qsize() == 0, 'all spans drained'
        assert count[0] == 4, 'last span submitted in one extrac batch'

    def test_force_flush(self, request):
        block_for = 0
        exported = []
        def send(batch):
            time.sleep(block_for)
            for span in batch.spans:
                exported.append(span)
            return 1

        reporter, sender = self._new_reporter(request, batch_size=100, flush=99999, send_method=send)
        span_1 = self._new_span('10')
        reporter.report_span(span_1)

        then = time.time()
        reporter.flush()
        elapsed = time.time() - then
        assert elapsed < 0.1
        assert len(exported) == 1

        batch = thrift.make_jaeger_batch(spans=[span_1], process=None)
        assert exported[0] == batch.spans[0]

        span_2 = self._new_span('20')
        span_3 = self._new_span('30')

        reporter.report_span(span_2)
        reporter.report_span(span_3)

        block_for = 0.5
        exported = []
        then = time.time()
        reporter.flush()
        elapsed = time.time() - then
        assert elapsed > 0.5
        assert len(exported) == 2

        batch = thrift.make_jaeger_batch(spans=[span_2, span_3], process=None)
        assert exported[0] == batch.spans[0]
        assert exported[1] == batch.spans[1]

    def test_flush_interval(self, request):
        reporter_store = [None]
        count = [0]

        def send(_):
            count[0] += 1
            if count[0] < 10:
                reporter_store[0].report_span(self._new_span('span'))
            return 1

        reporter, sender = self._new_reporter(request, batch_size=100, flush=.001, send_method=send)
        reporter_store[0] = reporter

        # initiate lazy thread creation
        reporter.report_span(self._new_span('span'))

        assert wait_for((lambda: count[0] == 10), ms=100)
        reporter.close()
