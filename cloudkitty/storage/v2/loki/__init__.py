# Copyright 2025 Red Hat
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
import datetime

from oslo_config import cfg
from oslo_log import log

from cloudkitty import dataframe
from cloudkitty.storage import v2 as v2_storage
from cloudkitty.storage.v2.loki import client as os_client
from cloudkitty.storage.v2.loki import exceptions
from cloudkitty.utils import tz as tzutils

LOG = log.getLogger(__name__)

CONF = cfg.CONF

LOKI_STORAGE_GROUP = 'storage_loki'

loki_storage_opts = [
    cfg.StrOpt(
        'host',
        help='Loki host, along with port and protocol. '
             'Defaults to http://localhost:3100',
        default='http://localhost:3100'),
    cfg.StrOpt(
        'tenant',
        help='The loki tenant to be used '
             'Defaults to tenant1.',
        default='tenant1'),
    cfg.StrOpt(
        'stream',
        help='The labels that are going to be used to define the Loki stream. '
             'Defaults to {service="cloudkitty"}.',
        default='{service="cloudkitty"}'),
    cfg.StrOpt(
        'buffer_size',
        help='The number of messages that will be grouped together before opening '
            'a Loki HTTP request.',
        default=10),
    cfg.StrOpt(
        'content_type',
        help='The http Content-Type that will be used to send info to Loki.'
             'Defaults to application/json. It can also be application/x-protobuf',
        default='application/json'),
    cfg.BoolOpt(
        'insecure',
        help='Set to true to allow insecure HTTPS '
                'connections to Loki',
        default=False),
    cfg.StrOpt(
        'cafile',
        help='Path of the CA certificate to trust for '
            'HTTPS connections.',
        default=None)
]

CONF.register_opts(loki_storage_opts, LOKI_STORAGE_GROUP)

class LokiStorage(v2_storage.BaseStorage):

    def __init__(self, *args, **kwargs):
        super(LokiStorage, self).__init__(*args, **kwargs)

        verify = not CONF.storage_loki.insecure
        if verify and CONF.storage_loki.cafile:
            verify = CONF.storage_loki.cafile

        self._conn = os_client.LokiClient(
            CONF.storage_loki.host,
            CONF.storage_loki.tenant,
            CONF.storage_loki.stream,
            CONF.storage_loki.content_type,
            CONF.storage_loki.buffer_size)

    def push(self, dataframes):
        for frame in dataframes:
            for type_, point in frame.iterpoints():
                start, end = self._local_to_utc(frame.start, frame.end)
                self._conn.add_point(point, type_, start, end)

    @staticmethod
    def _local_to_utc(*args):
        return [tzutils.local_to_utc(arg) for arg in args]

    def _build_dataframes(self, docs):
        dataframes = {}
        nb_points = 0
        for doc in docs:
            source = doc['_source']
            start = tzutils.dt_from_iso(source['start'])
            end = tzutils.dt_from_iso(source['end'])
            key = (start, end)
            if key not in dataframes.keys():
                dataframes[key] = dataframe.DataFrame(start=start, end=end)
            dataframes[key].add_point(
                self._doc_to_datapoint(source), source['type'])
            nb_points += 1

        output = list(dataframes.values())
        output.sort(key=lambda frame: (frame.start, frame.end))
        return output

    def retrieve(self, begin=None, end=None,
                 filters=None,
                 metric_types=None,
                 offset=0, limit=1000, paginate=True):
        begin, end = self._local_to_utc(begin or tzutils.get_month_start(),
                                        end or tzutils.get_next_month())
        total, docs = self._conn.retrieve(
            begin, end, filters, metric_types,
            offset=offset, limit=limit, paginate=paginate)
        return {
            'total': total,
            'dataframes': self._build_dataframes(docs),
        }

    def delete(self, begin=None, end=None, filters=None):
        LOG.info("Not implemented method.")
        raise exceptions.NotImplementedYet("delete")

    @staticmethod
    def _normalize_time(t):
        if isinstance(t, datetime.datetime):
            return tzutils.utc_to_local(t)
        return tzutils.dt_from_iso(t)

    def _doc_to_total_result(self, doc, start, end):
        output = {
            'begin': self._normalize_time(doc.get('start', start)),
            'end': self._normalize_time(doc.get('end', end)),
            'qty': doc['sum_qty']['value'],
            'rate': doc['sum_price']['value'],
        }
        # Means we had a composite aggregation
        if 'key' in doc.keys():
            for key, value in doc['key'].items():
                if key == 'begin' or key == 'end':
                    # OpenSearch returns ts in milliseconds
                    value = tzutils.dt_from_ts(value // 1000)
                output[key] = value
        return output

    def total(self, groupby=None, begin=None, end=None, metric_types=None,
              filters=None, custom_fields=None, offset=0, limit=1000,
              paginate=True):
        LOG.info("Not implemented method.")
        raise exceptions.NotImplementedYet("total")
