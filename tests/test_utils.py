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

from __future__ import absolute_import

import mock
import unittest

from jaeger_client import utils


class ConfigTests(unittest.TestCase):

    def check_boolean(self, string, default, correct):
        assert utils.get_boolean(string, default) == correct

    def test_get_false_boolean(self):
        self.check_boolean('false', 'asdf', False)

    def test_get_0_boolean(self):
        self.check_boolean('0', 'asdf', False)

    def test_get_true_boolean(self):
        self.check_boolean('true', 'qwer', True)

    def test_get_1_boolean(self):
        self.check_boolean('1', 'qwer', True)

    def test_get_unknown_boolean(self):
        self.check_boolean('zxcv', 'qwer', 'qwer')

    def test_get_None_boolean(self):
        self.check_boolean(None, 'qwer', False)

#    def test_error_reporter_doesnt_send_metrics_if_not_configured(self):
#        er = utils.ErrorReporter(False)
#        er.error('foo', 1)
#        assert not mock_metrics.count.called

    def test_error_reporter_sends_metrics_if_configured(self):
        mock_metrics = mock.MagicMock()
        er = utils.ErrorReporter()
        er.error('foo', 1)
        assert mock_metrics.count.called_with('foo', 1)


def test_local_ip_does_not_blow_up():
    import socket
    import jaeger_client.utils
    socket.gethostname()
    with mock.patch('socket.gethostbyname',
                    side_effect=[IOError(), '127.0.0.1']):
        jaeger_client.utils.local_ip()


def test_get_local_ip_by_socket_does_not_blow_up():
    import jaeger_client.utils
    jaeger_client.utils.get_local_ip_by_socket()


class CustomException(Exception):
    pass


def test_raise_with_value_reraises():
    try:
        raise CustomException('Initial Value')
    except Exception as e:
        try:
            utils.raise_with_value(e, 'New Value')
        except Exception as e:
            assert type(e) == CustomException
            assert str(e) == 'New Value'
        else:
            assert False, 'Failed to raise'
    else:
        assert False, 'Failed to raise'


def test_raise_with_value_raises():
    try:
        utils.raise_with_value(CustomException, 'Some Value')
    except Exception as e:
        assert type(e) == CustomException
        assert str(e) == 'Some Value'
    else:
        assert False, 'Failed to raise'
