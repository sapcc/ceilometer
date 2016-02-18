# Copyright 2013 IBM Corp
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
import time

from threading import Thread
from Queue import Queue
from oslo_config import cfg
from oslo_log import log
import requests

from ceilometer import dispatcher
from ceilometer.i18n import _, _LE, _LW
from ceilometer.publisher import utils as publisher_utils

LOG = log.getLogger(__name__)

http_dispatcher_opts = [
    cfg.StrOpt('target',
               default='',
               help='The target where the http request will be sent. '
                    'If this is not set, no data will be posted. For '
                    'example: target = http://hostname:1234/path'),
    cfg.StrOpt('event_target',
               help='The target for event data where the http request '
                    'will be sent to. If this is not set, it will default '
                    'to same as Sample target.'),
    cfg.IntOpt('timeout',
               default=5,
               help='The max time in seconds to wait for a request to '
                    'timeout.'),
    cfg.StrOpt('verify_ssl',
               help='The path to a server certificate if the usual CAs '
                    'are not used or if a self-signed certificate is used. '
                    'Set to False to ignore certificate verification.'),
    cfg.BoolOpt('batch_mode',
                default=False,
                help='Indicates whether samples are'
                     ' published in a batch.'),
    cfg.IntOpt('batch_count',
               default=1000,
               help='Maximum number of samples in a batch.'),
    cfg.IntOpt('batch_timeout',
               default=15,
               help='Maximum time interval(seconds) after which '
                    'samples are published in a batch.'),
    cfg.IntOpt('batch_polling_interval',
               default=5,
               help='Frequency of checking if batch criteria is met.'),
]

cfg.CONF.register_opts(http_dispatcher_opts, group="dispatcher_http")


class HttpDispatcher(dispatcher.MeterDispatcherBase,
                     dispatcher.EventDispatcherBase):
    """Dispatcher class for posting metering/event data into a http target.

    To enable this dispatcher, the following option needs to be present in
    ceilometer.conf file::

        [DEFAULT]
        meter_dispatchers = http
        event_dispatchers = http

    Dispatcher specific options can be added as follows::

        [dispatcher_http]
        target = www.example.com
        event_target = www.example.com
        timeout = 2

    Instead of publishing events and meters as JSON objects in individual HTTP
    requests, they can be batched up and published as JSON arrays of objects::

        [dispatcher_http]
        batch_mode = True
        batch_count = 1000
        batch_timeout = 15 # seconds
        batch_polling_interval = 5 # seconds
    """
    batch_timer = None

    def __init__(self, conf):
        super(HttpDispatcher, self).__init__(conf)
        self.headers = {'Content-type': 'application/json'}
        self.timeout = self.conf.dispatcher_http.timeout
        self.target = self.conf.dispatcher_http.target
        self.event_target = (self.conf.dispatcher_http.event_target or
                             self.target)
        self.verify_ssl = self.conf.dispatcher_http.verify_ssl
        # Deal with the case where verify_ssl is set to a boolean or not set at all
        if self.verify_ssl == 'False' or self.verify_ssl == 'True' or self.verify_ssl == '':
            self.verify_ssl = (self.verify_ssl != 'False')
        # Settings for batch mode
        self.batch_mode = self.conf.dispatcher_http.batch_mode
        if self.batch_mode and HttpDispatcher.batch_timer is None:
            LOG.debug(_('Set up and run batch mode'))
            HttpDispatcher.meter_queue = Queue()
            HttpDispatcher.event_queue = Queue()
            HttpDispatcher.batch_timer = BatchFlushThread(self.conf.dispatcher_http.batch_count,
                                                          self.conf.dispatcher_http.batch_timeout,
                                                          self.conf.dispatcher_http.batch_polling_interval)
            HttpDispatcher.batch_timer.start()

    def record_metering_data(self, data):
        if self.target == '':
            # if the target was not set, do not do anything
            LOG.error(_('Dispatcher target was not set, no meter will '
                        'be posted. Set the target in the ceilometer.conf '
                        'file'))
            return

        # We may have received only one counter on the wire
        if not isinstance(data, list):
            data = [data]

        for meter in data:
            LOG.debug(
                'metering data %(counter_name)s '
                'for %(resource_id)s @ %(timestamp)s: %(counter_volume)s',
                {'counter_name': meter['counter_name'],
                 'resource_id': meter['resource_id'],
                 'timestamp': meter.get('timestamp', 'NO TIMESTAMP'),
                 'counter_volume': meter['counter_volume']})
            if publisher_utils.verify_signature(
                    meter, self.conf.publisher.telemetry_secret):
                if self.batch_mode:
                    LOG.debug(_('Adding meter to batch queue'))
                    HttpDispatcher.meter_queue.put((self, meter))
                else:
                    LOG.debug(_('Posting single meter'))
                    meter_json = json.dumps(meter)
                    self.post_meter_json(meter_json)
            else:
                LOG.warning(_(
                    'message signature invalid, discarding message: %r'),
                    meter)

    def post_meter_json(self, meter_json):
        try:
            LOG.debug(_('Meter Message: %s '), meter_json)
            res = requests.post(self.target,
                                data=meter_json,
                                headers=self.headers,
                                verify=self.verify_ssl,
                                timeout=self.timeout)
            LOG.debug(_('Meter Message posting finished with status code %d.'),
                      res.status_code)
            res.raise_for_status()
            return True
        except Exception as err:
            error_code = res.status_code if res else 'unknown'
            LOG.exception(_LE('Status Code: %{code}s. Failed to'
                              'dispatch event: %{event}s'),
                          {'code': error_code, 'event': meter_json})
            return False

    def record_events(self, events):
        if not isinstance(events, list):
            events = [events]

        for event in events:
            if publisher_utils.verify_signature(
                    event, self.conf.publisher.telemetry_secret):
                if self.batch_mode:
                    LOG.debug(_('Adding event to batch queue'))
                    HttpDispatcher.event_queue.put((self, event))
                else:
                    LOG.debug(_('Posting single event'))
                    event_json = json.dumps(event)
                    self.post_event_json(event_json)
            else:
                LOG.warning(_LW(
                    'event signature invalid, discarding event: %s'), event)

    def post_event_json(self, event_json):
        res = None
        try:
            LOG.debug(_('Event Message: %s '), event_json)
            res = requests.post(self.event_target, data=event_json,
                                headers=self.headers,
                                verify=self.verify_ssl,
                                timeout=self.timeout)
            LOG.debug(_('Event Message posting finished with status code %d.'),
                      res.status_code)
            res.raise_for_status()
            return True
        except Exception:
            error_code = res.status_code if res else 'unknown'
            LOG.exception(_LE('Status Code: %{code}s. Failed to'
                              'dispatch event: %{event}s'),
                          {'code': error_code, 'event': event_json})
            return False


class BatchFlushThread(Thread):
    def __init__(self, batch_count, batch_timeout, batch_polling_interval):
        super(BatchFlushThread, self).__init__()
        self.batch_count = batch_count
        self.batch_timeout = batch_timeout
        self.batch_polling_interval = batch_polling_interval
        self.time_of_last_batch_run = time.time()

    def is_batch_ready(self, queue):
        """Method to check if batch is ready to be flushed."""

        previous_time = self.time_of_last_batch_run
        current_time = time.time()
        elapsed_time = current_time - previous_time

        LOG.debug(_('Elapsed time is %d'), elapsed_time)

        if elapsed_time >= self.batch_timeout and not queue.empty():
            LOG.debug(_('Batch timeout exceeded, triggering batch publish.'))
            return True
        else:
            if queue.qsize() >= self.batch_count:
                LOG.debug(_('Batch queue full, triggering batch publish.'))
                return True
            else:
                LOG.debug('Not flushing. Queue size: %d, elapsed time: %d '
                          , queue.qsize(), elapsed_time)
                return False

    def run(self):
        """Method to flush the queued meters & events."""
        LOG.debug('BatchFlushThread.run called')
        poster = None

        while True:
            LOG.debug(_('Check meter queue to see if we are ready to flush a batch'))
            if self.is_batch_ready(HttpDispatcher.meter_queue):
                # publish all meters in queue at this point
                meters = []
                while not HttpDispatcher.meter_queue.empty():
                    poster, meter = HttpDispatcher.meter_queue.get()
                    meters.append(meter)

                if meters:
                    LOG.debug('Meters to publish in batch: '
                              '%d ', len(meters))
                    meter_json = json.dumps(meters)
                    # If posting failed, requeue the events
                    if not poster.post_meter_json(meter_json):
                        for meter in meters:
                            HttpDispatcher.meter_queue.put((poster, meter))

            LOG.debug(_('Check event queue to see if we are ready to flush a batch'))
            if self.is_batch_ready(HttpDispatcher.event_queue):
                # publish all events in queue at this point
                events = []
                while not HttpDispatcher.event_queue.empty():
                    poster, event = HttpDispatcher.event_queue.get()
                    events.append(event)

                if events:
                    LOG.debug('Events to publish in batch: '
                              '%d ', len(events))
                    event_json = json.dumps(events)
                    # If posting failed, requeue the events
                    if not poster.post_event_json(event_json):
                        for event in events:
                            HttpDispatcher.event_queue.put((poster, event))

                self.time_of_last_batch_run = time.time()

            time.sleep(self.batch_polling_interval)
