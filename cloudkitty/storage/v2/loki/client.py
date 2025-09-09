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
from datetime import datetime
import itertools

from oslo_log import log
import requests

from cloudkitty.storage.v2 import loki
from cloudkitty.storage.v2.loki import exceptions
from cloudkitty.utils import json

LOG = log.getLogger(__name__)


class LokiClient(object):
    """Class used to ease interaction with Loki.

    """

    def __init__(self, url, tenant, stream, content_type, buffer_size):
        if content_type != "application/json":
            raise exceptions.UnsupportedContentType(content_type)

        self._base_url = url.strip('/')
        self._stream = stream
        self._headers = {
            'X-Scope-OrgID': tenant,
            'Content-Type': content_type
        }
        self._buffer_size = buffer_size

    def _build_payload_json(self):
        payload = {
           "streams": [
                {
                    "stream": self._stream,
                    "values": [
                        self._points
                    ]
                }
            ]
        }
        return payload

    def push(self):
        """Send a message to Loki.
        """

        url = f"{self.base_url}/loki/api/v1/push"
        
        response = requests.post(url, json=self._build_payload_json(), headers=self._headers)
        if response.status_code == 204:
            print("Message pushed successfully.")
        else:
            print(f"Failed to push logs: {response.status_code} - {response.text}")

    def retrieve(self, query_string, start=None, end=None):
        """
        Retrieve messages from Loki.
        
        :param query_string: The Loki query string.
        :param start: Start time in nanoseconds (optional).
        :param end: End time in nanoseconds (optional).
        :return: The response from Loki.
        """
        url = f"{self.base_url}/loki/api/v1/query_range"
        params = {
            "query": query_string,
            "start": start,
            "end": end
        }
        response = requests.get(url, params=params, headers=self._headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to query logs: {response.status_code} - {response.text}")
            return None

    def add_point(self, point, type_, start, end):
        """Append a point to the client.

        :param point: DataPoint to append
        :type point: cloudkitty.dataframe.DataPoint
        :param type_: type of the DataPoint
        :type type_: str
        """
        self._points.append([
            str(int(datetime.now().timestamp() * 1_000_000_000)),
            {
                'start': start,
                'end': end,
                'type': type_,
                'unit': point.unit,
                'description': point.description,
                'qty': point.qty,
                'price': point.price,
                'groupby': point.groupby,
                'metadata': point.metadata,
            }
        ])
        if len(self._points) >= self._buffer_size:
            self.push()

    # def delete_by_query(self, begin=None, end=None, filters=None):
    #     """Does a POST request against ES's Delete By Query API.

    #     The POST request will be done against
    #     `/<index_name>/_delete_by_query`

    #     :param filters: Optional filters for documents to delete
    #     :type filters: list of dicts
    #     :rtype: requests.models.Response
    #     """
    #     url = '/'.join((self._url, self._index_name, '_delete_by_query'))
    #     must = self._build_must(begin, end, None, filters)
    #     data = (json.dumps({"query": {"bool": {"must": must}}})
    #             if must else None)
    #     return self._req(self._sess.post, url, data, None)

    # def total(self, begin, end, metric_types, filters, groupby,
    #           custom_fields=None, offset=0, limit=1000, paginate=True):

    #     if custom_fields:
    #         LOG.warning("'custom_fields' are not implemented yet for "
    #                     "OpenSearch. Therefore, the custom fields [%s] "
    #                     "informed by the user will be ignored.", custom_fields)
    #     if not paginate:
    #         offset = 0

    #     metric_types = [metric_types] if metric_types else None

    #     must = self._build_must(begin, end, metric_types, filters)
    #     should = self._build_should(filters)
    #     composite = self._build_composite(groupby) if groupby else None
    #     if composite:
    #         composite['size'] = self._chunk_size
    #     query = self._build_query(must, should, composite)

    #     if "aggs" not in query.keys():
    #         query["aggs"] = {
    #             "sum_price": {"sum": {"field": "price"}},
    #             "sum_qty": {"sum": {"field": "qty"}},
    #         }

    #     query['size'] = 0

    #     resp = self.search(query, scroll=False)

    #     # Means we didn't group, so length is 1
    #     if not composite:
    #         return 1, [resp["aggregations"]]

    #     after = resp["aggregations"]["sum_and_price"].get("after_key")
    #     chunk = resp["aggregations"]["sum_and_price"]["buckets"]

    #     total = len(chunk)

    #     output = chunk[offset:offset+limit if paginate else len(chunk)]
    #     offset = 0 if len(chunk) > offset else offset - len(chunk)

    #     # FIXME(peschk_l): We have to iterate over ALL buckets in order to get
    #     # the total length. If there is a way for composite aggregations to get
    #     # the total amount of buckets, please fix this
    #     while after:
    #         composite_query = query["aggs"]["sum_and_price"]["composite"]
    #         composite_query["size"] = self._chunk_size
    #         composite_query["after"] = after
    #         resp = self.search(query, scroll=False)
    #         after = resp["aggregations"]["sum_and_price"].get("after_key")
    #         chunk = resp["aggregations"]["sum_and_price"]["buckets"]
    #         if not chunk:
    #             break
    #         output += chunk[offset:offset+limit if paginate else len(chunk)]
    #         offset = 0 if len(chunk) > offset else offset - len(chunk)
    #         total += len(chunk)

    #     if paginate:
    #         output = output[offset:offset+limit]
    #     return total, output
