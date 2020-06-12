# Copyright 2016 MongoDB, Inc.
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

"""Elasticsearch implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
Elasticsearch.
"""
import base64
import logging
import threading
import time
import warnings
import os

import bson.json_util

try:
    __import__("elasticsearch")
except ImportError:
    raise ImportError(
        "Error: elasticsearch (https://pypi.python.org/pypi/elasticsearch) "
        "version 2.x or 5.x is not installed.\n"
        "Install with:\n"
        "  pip install elastic2-doc-manager[elastic2]\n"
        "or:\n"
        "  pip install elastic2-doc-manager[elastic5]\n"
    )

from elasticsearch import (
    Elasticsearch,
    exceptions as es_exceptions,
    connection as es_connection,
)
from elasticsearch.helpers import bulk, scan, streaming_bulk, BulkIndexError

import importlib_metadata

import prometheus_client
from prometheus_client import Summary, Counter

# import json
# from jsondiff import diff

from mongo_connector import errors
from mongo_connector.constants import DEFAULT_COMMIT_INTERVAL, DEFAULT_MAX_BULK
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

_HAS_AWS = True
try:
    from boto3 import session
    from requests_aws_sign import AWSV4Sign
except ImportError:
    _HAS_AWS = False

wrap_exceptions = exception_wrapper(
    {
        BulkIndexError: errors.OperationFailed,
        es_exceptions.ConnectionError: errors.ConnectionFailed,
        es_exceptions.TransportError: errors.OperationFailed,
        es_exceptions.NotFoundError: errors.OperationFailed,
        es_exceptions.RequestError: errors.OperationFailed,
    }
)

LOG = logging.getLogger(__name__)

DEFAULT_SEND_INTERVAL = 5
"""The default interval in seconds to send buffered operations."""

DEFAULT_AWS_REGION = "us-east-1"

__version__ = importlib_metadata.version("elastic2_doc_manager")

ERROR_TIME = Summary('dm_error_time', 'Errors thrown')
ERROR_CAUGHT = Counter('dm_error_caught', 'Error thrown', ['message', 'response'])

def convert_aws_args(aws_args):
    """Convert old style options into arguments to boto3.session.Session."""
    if not isinstance(aws_args, dict):
        raise errors.InvalidConfiguration(
            'Elastic DocManager config option "aws" must be a dict'
        )
    old_session_kwargs = dict(
        region="region_name",
        access_id="aws_access_key_id",
        secret_key="aws_secret_access_key",
    )
    new_kwargs = {}
    for arg in aws_args:
        if arg in old_session_kwargs:
            new_kwargs[old_session_kwargs[arg]] = aws_args[arg]
        else:
            new_kwargs[arg] = aws_args[arg]
    return new_kwargs


def create_aws_auth(aws_args):
    try:
        aws_session = session.Session(**convert_aws_args(aws_args))
    except TypeError as exc:
        raise errors.InvalidConfiguration(
            "Elastic DocManager unknown aws config option: %s" % (exc,)
        )
    return AWSV4Sign(
        aws_session.get_credentials(),
        aws_session.region_name or DEFAULT_AWS_REGION,
        "es",
    )

def __get_os_environ_or_default__(env_key, default_value, log_message=True):
    """ Returns the value for os.environ[environ_key], or default.

    Parameters
    ----------
    env_key : str
        The key used to access os.environ[].
    default_value
        The value to default to if os.environ[environ_key] is not found.
    log_message : bool
        If true, print whether the value was found or not.
    """
    try:
        result = os.environ[env_key]
        if not len(result):
            raise KeyError('{} is an empty string'.format(env_key))
        message = 'ENV VAR found {}={}'.format(env_key, result)
    except KeyError:
        result = default_value
        message = '-- did not find {0}; forced {0}={1}'.format(env_key, result)

    if log_message:
        LOG.info(message)

    return result


class AutoCommiter(threading.Thread):
    """Thread that periodically sends buffered operations to Elastic.

    :Parameters:
      - `docman`: The Elasticsearch DocManager.
      - `send_interval`: Number of seconds to wait before sending buffered
        operations to Elasticsearch. Set to None or 0 to disable.
      - `commit_interval`: Number of seconds to wait before committing
        buffered operations to Elasticsearch. Set to None or 0 to disable.
      - `sleep_interval`: Number of seconds to sleep.
    """

    def __init__(self, docman, send_interval, commit_interval, sleep_interval=1):
        super(AutoCommiter, self).__init__()
        self._docman = docman
        # Change `None` intervals to 0
        self._send_interval = send_interval if send_interval else 0
        self._commit_interval = commit_interval if commit_interval else 0
        self._should_auto_send = self._send_interval > 0
        self._should_auto_commit = self._commit_interval > 0
        self._sleep_interval = max(sleep_interval, 1)
        self._stopped = False
        self.daemon = True

    def join(self, timeout=None):
        self._stopped = True
        super(AutoCommiter, self).join(timeout=timeout)

    def run(self):
        """Periodically sends buffered operations and/or commit.
        """
        if not self._should_auto_commit and not self._should_auto_send:
            return
        last_send, last_commit = 0, 0
        while not self._stopped:
            if self._should_auto_commit:
                if last_commit > self._commit_interval:
                    self._docman.commit()
                    # commit also sends so reset both
                    last_send, last_commit = 0, 0
                    # Give a chance to exit the loop
                    if self._stopped:
                        break

            if self._should_auto_send:
                if last_send > self._send_interval:
                    self._docman.send_buffered_operations()
                    last_send = 0
            time.sleep(self._sleep_interval)
            last_send += self._sleep_interval
            last_commit += self._sleep_interval


class DocManager(DocManagerBase):
    """Elasticsearch implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    Elasticsearch.
    """

    def __init__(
        self,
        url,
        auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
        unique_key="_id",
        chunk_size=DEFAULT_MAX_BULK,
        meta_index_name="mongodb_meta",
        meta_type="mongodb_meta",
        attachment_field="content",
        **kwargs
    ):
        client_options = kwargs.get("clientOptions", {})
        if "aws" in kwargs:
            if not _HAS_AWS:
                raise errors.InvalidConfiguration(
                    "aws extras must be installed to sign Elasticsearch "
                    "requests. Install with: "
                    "pip install elastic2-doc-manager[aws]"
                )
            client_options["http_auth"] = create_aws_auth(kwargs["aws"])
            client_options["use_ssl"] = True
            client_options["verify_certs"] = False
            client_options["connection_class"] = es_connection.RequestsHttpConnection
        else:
            client_options["use_ssl"] = True
            client_options["verify_certs"] = False
            client_options["connection_class"] = es_connection.RequestsHttpConnection

        if type(url) is not list:
            url = [url]

        LOG.always('URL IN DOC MANAGER:')
        LOG.always(url)

        # self.elastic = Elasticsearch(hosts=url, **client_options)
        protocol = "http" if (os.environ.get('ELASTIC_SSL_ENABLED') == "false") else "https"
        username = os.environ.get('ELASTIC_USER')
        password = os.environ.get('ELASTIC_PASSWORD')
        hostname = os.environ.get('ELASTIC_HOST')
        port = os.environ.get('ELASTIC_PORT')

        timeout = int(__get_os_environ_or_default__('ELASTIC_TIMEOUT', 30))
        max_retries = int(__get_os_environ_or_default__('ELASTIC_MAX_RETRY', 20))
        retry_on_timeout = bool(int(__get_os_environ_or_default__('ELASTIC_RETRY_ON_TIMEOUT', True)))

        # We're not using sniffing now - we will fix it using Connection with credentials.
        sniff_on_start = bool(int(__get_os_environ_or_default__('ELASTIC_SNIFF_ON_START', True)))
        sniff_on_connection_fail = bool(int(__get_os_environ_or_default__('ELASTIC_SNIFF_ON_CONN_FAIL', True)))
        sniffer_timeout = int(__get_os_environ_or_default__('ELASTIC_SNIFFER_TIMEOUT', 60))

        if username and password:
            elastic_url = "{0}://{1}:{2}@{3}:{4}/".format(protocol, username, password, hostname, port)
        else:
            elastic_url = "{0}://{1}:{2}/".format(protocol, hostname, port)

        LOG.always('SELF-ASSEMBLED ELASTIC URL IN DOC MANAGER:')
        LOG.always(elastic_url)

        if os.environ.get('ELASTIC_SSL_ENABLED') == "false":
            use_ssl = False
        else:
            use_ssl = True

        # https://stackoverflow.com/questions/25908484/how-to-fix-read-timed-out-in-elasticsearch
        # es = Elasticsearch(timeout=30, max_retries=10, retry_on_timeout=True)
        # https://elasticsearch-py.readthedocs.io/en/master/#sniffing
        # Sniffing caused authentication issue - it appears it was using username/password to retry. We'll revisit
        # this later to check if sniff can be integrated in case needed. Disabling it for now. SEAR-392
        self.elastic = Elasticsearch(
            hosts=[elastic_url],
            verify_certs=False,
            use_ssl=use_ssl,
            timeout=timeout,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout
            # sniff_on_start=sniff_on_start,
            # sniff_on_connection_fail=sniff_on_connection_fail,
            # sniffer_timeout=sniffer_timeout
        )

        self.summary_title = 'dm_ingestion_time'
        self.counter_title = 'dm_ingest'
        self.REQUEST_TIME = Summary(self.summary_title, 'Bulk operations throughput')
        self.ingest_rate = Counter(self.counter_title, 'Number of documents ingested per bulk operation',
                                   ['collectionName'])

        self.doc_summary_title = 'new_doc_operation_time'
        self.doc_count_title = 'new_doc_operation'
        self.REQUEST_TIME_OP = Summary(self.doc_summary_title, 'Operations on documents for Elasticsearch')
        self.doc_operation_count = Counter(self.doc_count_title, 'Document operation', ['operation_type', 'index'])

        self._formatter = DefaultDocumentFormatter()
        self.BulkBuffer = BulkBuffer(self)

        # As bulk operation can be done in another thread
        # lock is needed to prevent access to BulkBuffer
        # while commiting documents to Elasticsearch
        # It is because BulkBuffer might get outdated
        # docs from Elasticsearch if bulk is still ongoing
        self.lock = threading.Lock()

        self.auto_commit_interval = auto_commit_interval
        self.auto_send_interval = kwargs.get("autoSendInterval", DEFAULT_SEND_INTERVAL)
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.has_attachment_mapping = False
        self.attachment_field = attachment_field
        self.auto_commiter = AutoCommiter(
            self, self.auto_send_interval, self.auto_commit_interval
        )
        self.auto_commiter.start()

        # with open('./config/mapping_resources_and_run_data.json', 'r') as mapping_config:
        #     try:
        #         # local_mapping = json.load(mapping_config)
        #         # local_mapping = str(local_mapping)
        #
        #         # try:
        #         #     es_mapping = self.elastic.indices.get_mapping(index='resources_and_run_data')
        #         #     es_mapping = es_mapping\
        #         #         .get('resources_and_run_data')\
        #         #         .get('mappings')\
        #         #         .get('resources_and_run_data')
        #         #
        #         #     # es_mapping = str(es_mapping)
        #         #
        #         #     # is_mapping_correct = local_mapping == es_mapping
        #         #     is_mapping_correct = diff(local_mapping, es_mapping)
        #         #
        #         #     LOG.always('*******************************************')
        #         #     LOG.always('LOCAL')
        #         #     LOG.always(local_mapping)
        #         #     LOG.always('*******************************************')
        #         #     LOG.always(' ')
        #         #     LOG.always(' ')
        #         #     LOG.always('*******************************************')
        #         #     LOG.always('ES')
        #         #     LOG.always(es_mapping)
        #         #     LOG.always('*******************************************')
        #         #
        #         #     LOG.always('*******************************************')
        #         #     LOG.always('diff')
        #         #     LOG.always(is_mapping_correct)
        #         #     LOG.always('*******************************************')
        #
        #             # if not is_mapping_correct:
        #
        #         except errors.ConnectionFailed:
        #             LOG.exception(
        #                 'Could not load mapping config on Elasticsearch'
        #             )
        #     except ValueError:
        #         LOG.exception(
        #             'Could not load mappings file'
        #         )
        #
        #         return

    def _index_and_mapping(self, namespace):
        """Helper method for getting the index and type from a namespace."""
        index, doc_type = namespace.split(".", 1)
        return index.lower(), doc_type

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commiter.join()
        self.auto_commit_interval = 0
        # Commit any remaining docs from buffer
        self.commit()

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        # Flush buffer before handle command
        self.commit()
        db = namespace.split(".", 1)[0]
        if doc.get("dropDatabase"):
            dbs = self.command_helper.map_db(db)
            for _db in dbs:
                self.elastic.indices.delete(index=_db.lower())

        if doc.get("renameCollection"):
            raise errors.OperationFailed(
                "elastic_doc_manager does not support renaming a mapping."
            )

        if doc.get("create"):
            db, coll = self.command_helper.map_collection(db, doc["create"])
            if db and coll:
                self.elastic.indices.put_mapping(
                    index=db.lower(), doc_type=coll, body={"_source": {"enabled": True}}
                )

        if doc.get("drop"):
            db, coll = self.command_helper.map_collection(db, doc["drop"])
            if db and coll:
                # This will delete the items in coll, but not get rid of the
                # mapping.
                warnings.warn(
                    "Deleting all documents of type %s on index %s."
                    "The mapping definition will persist and must be"
                    "removed manually." % (coll, db)
                )
                responses = streaming_bulk(
                    self.elastic,
                    (
                        dict(result, _op_type="delete")
                        for result in scan(
                            self.elastic, index=db.lower(), doc_type=coll
                        )
                    ),
                )
                for ok, resp in responses:
                    if not ok:
                        LOG.error(
                            "Error occurred while deleting ElasticSearch docum"
                            "ent during handling of 'drop' command: %r" % resp
                        )

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """

        index, doc_type = self._index_and_mapping(namespace)
        with self.lock:
            # Check if document source is stored in local buffer
            document = self.BulkBuffer.get_from_sources(
                index, doc_type, str(document_id)
            )

        LOG.debug('_________________________ UPDATING FILE')
        LOG.debug(update_spec)

        if document:
            # Document source collected from local buffer
            # Perform apply_update on it and then it will be
            # ready for commiting to Elasticsearch
            updated = self.apply_update(document, update_spec)
            # _id is immutable in MongoDB, so won't have changed in update
            updated["_id"] = document_id
            self.upsert(updated, namespace, timestamp, None, True)
        else:
            # Document source needs to be retrieved from Elasticsearch
            # before performing update. Pass update_spec to upsert function
            updated = {"_id": document_id}
            self.upsert(updated, namespace, timestamp, update_spec, False)
        # upsert() strips metadata, so only _id + fields in _source still here
        return updated

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp, update_spec=None, is_update=False):
        """Insert a document into Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)
        # No need to duplicate '_id' in source document
        doc_id = str(doc.pop("_id"))
        metadata = {"ns": namespace, "_ts": timestamp}

        action_source = self._formatter.format_document(doc)

        # Index the source document, using lowercase namespace as index name.
        action = {
            "_op_type": "index",
            "_index": index,
            "_type": doc_type,
            "_id": doc_id,
            "_source": action_source,
        }

        LOG.debug('_________________________ UPSERTING FILE')

        meta_action_source = bson.json_util.dumps(metadata)

        # Index document metadata with original namespace (mixed upper/lower).
        meta_action = {
            "_op_type": "index",
            "_index": self.meta_index_name,
            "_type": self.meta_type,
            "_id": doc_id,
            "_source": meta_action_source,
        }

        if is_update:
            action['_update'] = True
            meta_action['_update'] = True

        self.index(action, meta_action, doc, update_spec)

        # Leave _id, since it's part of the original document
        doc["_id"] = doc_id

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp, collectionName):
        """Insert multiple documents into Elasticsearch."""

        def docs_to_upsert():
            doc_count = 0
            doc = None
            for doc in docs:
                # Remove metadata and redundant _id
                index, doc_type = self._index_and_mapping(namespace)
                doc_id = str(doc.pop("_id"))
                routing = False

                if os.environ.get('JOIN_INDEX'):
                    if namespace == os.environ.get('JOIN_INDEX')+"."+os.environ.get('JOIN_INDEX'):
                        if doc.get(os.environ.get('CHILD_FIELD_1')) and doc.get(os.environ.get('CHILD_FIELD_2')):
                            routing = True
                            doc["data_join"] = {
                                "name": os.environ.get('JOIN_FIELD'),
                                "parent": doc.get(os.environ.get('JOIN_FIELD'))
                            }
                        else:
                            doc["data_join"] = { "name": "_id" }

                document_action = {
                    "_index": index,
                    "_type": doc_type,
                    "_id": doc_id,
                    "_source": self._formatter.format_document(doc),
                }

                document_meta = {
                    "_index": self.meta_index_name,
                    "_type": self.meta_type,
                    "_id": doc_id,
                    "_source": {"ns": namespace, "_ts": timestamp},
                }

                if routing is True:
                    document_meta["_routing"] = doc.get(os.environ.get('JOIN_FIELD'))
                    document_action["_routing"] = doc.get(os.environ.get('JOIN_FIELD'))

                yield document_action
                yield document_meta

                doc_count += 1
            if doc is None:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search"
                )

            LOG.always(" - - - - - COLLECTION")
            LOG.always(collectionName)
            LOG.always(" - - - - - # OF DOCS")
            LOG.always(doc_count)

        try:
            kw = {}
            if self.chunk_size > 0:
                kw["chunk_size"] = self.chunk_size

            kw["max_retries"] = 10

            ns, ns2 = namespace.split(".", 1)

            if collectionName:
                index_name, ns = collectionName.split(".", 1)

            @self.REQUEST_TIME.time()
            def process_request(metric):
                metric.inc()

            @ERROR_TIME.time()
            def error_catch(error):
                error.inc()

            responses = streaming_bulk(
                client=self.elastic, actions=docs_to_upsert(), **kw
            )

            for ok, resp in responses:
                try:
                    if not ok:
                        LOG.error('_ Could not bulk-upsert document. ERROR RESP: {r}'.format(r=resp))

                        error_catch(ERROR_CAUGHT.labels('Could not bulk-upsert document into ElasticSearch', resp))
                    else:
                        if resp.get('index').get('_type') != 'mongodb_meta':
                            process_request(self.ingest_rate.labels(ns))
                except Exception as e:
                    err_msg = 'ES doc manager bulk insert threw exception: {}'.format(e)
                    LOG.exception(err_msg)

            if self.auto_commit_interval == 0:
                self.commit()
        except errors.EmptyDocsError:
            # This can happen when mongo-connector starts up, there is no
            # config file, but nothing to dump
            pass

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        doc = f.get_metadata()
        doc_id = str(doc.pop("_id"))
        index, doc_type = self._index_and_mapping(namespace)

        # make sure that elasticsearch treats it like a file
        if not self.has_attachment_mapping:
            body = {"properties": {self.attachment_field: {"type": "attachment"}}}
            self.elastic.indices.put_mapping(index=index, doc_type=doc_type, body=body)
            self.has_attachment_mapping = True

        metadata = {"ns": namespace, "_ts": timestamp}

        doc = self._formatter.format_document(doc)
        doc[self.attachment_field] = base64.b64encode(f.read()).decode()

        action = {
            "_op_type": "index",
            "_index": index,
            "_type": doc_type,
            "_id": doc_id,
            "_source": doc,
        }
        meta_action = {
            "_op_type": "index",
            "_index": self.meta_index_name,
            "_type": self.meta_type,
            "_id": doc_id,
            "_source": bson.json_util.dumps(metadata),
        }

        LOG.debug('_________________________ INSERTING FILE')

        self.index(action, meta_action)

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Remove a document from Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)

        action = {
            "_op_type": "delete",
            "_index": index,
            "_type": doc_type,
            "_id": str(document_id),
        }

        meta_action = {
            "_op_type": "delete",
            "_index": self.meta_index_name,
            "_type": self.meta_type,
            "_id": str(document_id),
        }

        # When removing a runData doc, we need to get the routing field into our action data
        # This allows the parent+child relationship to successfully dissolve on removal
        # Without the _routing field, this operation will throw an exception
        if os.environ.get('JOIN_INDEX') and (index == os.environ.get('JOIN_INDEX')):
            try:
                hit = self.elastic.search(
                    index=index,
                    body={ "query": { "match": { "_id": str(document_id) } } },
                    size=1
                )["hits"]["hits"]

                for result in hit:
                    if result and result['_routing']:
                        action['_routing'] = result['_routing']
                        meta_action['_routing'] = result['_routing']
            except:
                LOG.error('EXCEPTION: COULD NOT FIND DOCUMENT IN ELASTICSEARCH FOR REMOVAL OPERATION')

        self.index(action, meta_action)

    @wrap_exceptions
    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results."""
        for hit in scan(
            self.elastic, query=kwargs.pop("body", None), scroll="10m", **kwargs
        ):
            hit["_source"]["_id"] = hit["_id"]
            yield hit["_source"]

    def search(self, start_ts, end_ts):
        """Query Elasticsearch for documents in a time range.

        This method is used to find documents that may be in conflict during
        a rollback event in MongoDB.
        """
        return self._stream_search(
            index=self.meta_index_name,
            body={"query": {"range": {"_ts": {"gte": start_ts, "lte": end_ts}}}},
        )

    def index(self, action, meta_action, doc_source=None, update_spec=None):
        if os.environ.get('JOIN_INDEX'):
            namespace = action["_type"]
            if namespace == os.environ.get('JOIN_INDEX'):
                if doc_source:
                    is_child1 = doc_source.get(os.environ.get('CHILD_FIELD_1')) and \
                                doc_source.get(os.environ.get('CHILD_FIELD_2'))
                    is_child2 = action['_source'].get(os.environ.get('CHILD_FIELD_1')) and \
                                action['_source'].get(os.environ.get('CHILD_FIELD_2'))

                    if is_child1 or is_child2:
                        action['_source']['data_join'] = {
                            "name": os.environ.get('JOIN_FIELD'),
                            "parent": action['_source'][os.environ.get('JOIN_FIELD')]
                        }
                        doc_source['data_join'] = {
                            "name": os.environ.get('JOIN_FIELD'),
                            "parent": doc_source[os.environ.get('JOIN_FIELD')]
                        }
                        action["_routing"] = doc_source.get(os.environ.get('JOIN_FIELD'))
                        meta_action["_routing"] = doc_source.get(os.environ.get('JOIN_FIELD'))
                    else:
                        action['_source']['data_join'] = { 'name': '_id' }
                        doc_source['data_join'] = { 'name': '_id' }

        with self.lock:
            self.BulkBuffer.add_upsert(action, meta_action, doc_source, update_spec)

        # Divide by two to account for meta actions
        if (
            len(self.BulkBuffer.action_buffer) / 2 >= self.chunk_size
            or self.auto_commit_interval == 0
        ):
            self.commit()

    def send_buffered_operations(self):
        """Send buffered operations to Elasticsearch.

        This method is periodically called by the AutoCommitThread.
        """
        with self.lock:
            @ERROR_TIME.time()
            def error_catch(error):
                error.inc()

            try:
                action_buffer = self.BulkBuffer.get_buffer()
                if action_buffer:
                    successes, errors = bulk(self.elastic, action_buffer)
                    LOG.debug(
                        "Bulk request finished, successfully sent %d " "operations",
                        successes,
                    )

                    LOG.debug(' ')
                    LOG.debug(' ')
                    LOG.debug('*****************************************')
                    LOG.debug(' ')
                    LOG.debug('SUCCESSES')
                    LOG.debug(successes)
                    LOG.debug(' ')
                    LOG.debug('ACTION BUFFER')
                    LOG.debug(action_buffer)
                    LOG.debug(' ')
                    LOG.debug('*****************************************')
                    LOG.debug(' ')
                    LOG.debug(' ')

                    if errors:
                        for error in errors:
                            error_catch(ERROR_CAUGHT.labels('Bulk request error', error))

                        LOG.error("Bulk request finished with errors: %r", errors)

                    # TODO: Add collection name as label
                    @self.REQUEST_TIME_OP.time()
                    def process_request(operation_type, index):
                        self.doc_operation_count.labels(operation_type, index).inc()

                    doc = action_buffer[0]
                    index = doc.get('_index')
                    operation_type = doc.get('_op_type')

                    if doc.get('_update'):
                        process_request('update', index)
                        LOG.debug('UPDATE!')
                    elif operation_type == 'index':
                        process_request('add', index)
                        LOG.debug('ADD!')
                    elif operation_type == 'delete':
                        process_request('remove', index)
                        LOG.debug('REMOVE!')

                    # LOG.always(
                    #     "Counter: Documents removed: %d, "
                    #     "inserted: %d, updated: %d so far" % (
                    #         op_remove, op_add, op_update))

            except es_exceptions.ElasticsearchException as e:
                error_catch(ERROR_CAUGHT.labels('Bulk request failed with exception', 'send_buffered_operations'))
                LOG.exception("Bulk request failed with exception {}".format(e))

    def commit(self):
        """Send buffered requests and refresh all indexes."""
        self.send_buffered_operations()
        retry_until_ok(self.elastic.indices.refresh, index="")

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document from Elasticsearch.

        This method is used to help define a time window within which documents
        may be in conflict after a MongoDB rollback.
        """
        try:
            result = self.elastic.search(
                index=self.meta_index_name,
                body={"query": {"match_all": {}}, "sort": [{"_ts": "desc"}]},
                size=1,
            )["hits"]["hits"]
            for r in result:
                r["_source"]["_id"] = r["_id"]
                return r["_source"]
        except es_exceptions.RequestError:
            # no documents so ES returns 400 because of undefined _ts mapping
            return None


class BulkBuffer(object):
    def __init__(self, docman):

        # Parent object
        self.docman = docman

        # Action buffer for bulk indexing
        self.action_buffer = []

        # Docs to update
        # Dict stores all documents for which firstly
        # source has to be retrieved from Elasticsearch
        # and then apply_update needs to be performed
        # Format: [ (doc, update_spec, action_buffer_index, get_from_ES) ]
        self.doc_to_update = []

        # Below dictionary contains ids of documents
        # which need to be retrieved from Elasticsearch
        # It prevents from getting same document multiple times from ES
        # Format: {"_index": {"_type": {"_id": True}}}
        self.doc_to_get = {}

        # Dictionary of sources
        # Format: {"_index": {"_type": {"_id": {"_source": actual_source}}}}
        self.sources = {}

    def add_upsert(self, action, meta_action, doc_source, update_spec):
        """
        Function which stores sources for "insert" actions
        and decide if for "update" action has to add docs to
        get source buffer
        """

        # Whenever update_spec is provided to this method
        # it means that doc source needs to be retrieved
        # from Elasticsearch. It means also that source
        # is not stored in local buffer
        if update_spec:
            self.bulk_index(action, meta_action)

            # -1 -> to get latest index number
            # -1 -> to get action instead of meta_action
            # Update document based on source retrieved from ES
            self.add_doc_to_update(action, update_spec, len(self.action_buffer) - 2)
        else:
            # Insert and update operations provide source
            # Store it in local buffer and use for comming updates
            # inside same buffer
            # add_to_sources will not be called for delete operation
            # as it does not provide doc_source
            if doc_source:
                self.add_to_sources(action, doc_source)
            self.bulk_index(action, meta_action)

    def add_doc_to_update(self, action, update_spec, action_buffer_index):
        """
        Prepare document for update based on Elasticsearch response.
        Set flag if document needs to be retrieved from Elasticsearch
        """

        doc = {
            "_index": action["_index"],
            "_type": action["_type"],
            "_id": action["_id"]
        }

        # If get_from_ES == True -> get document's source from Elasticsearch
        get_from_ES = self.should_get_id(action)
        self.doc_to_update.append((doc, update_spec, action_buffer_index, get_from_ES))

    def should_get_id(self, action):
        """
        Mark document to retrieve its source from Elasticsearch.
        Returns:
            True - if marking document for the first time in this bulk
            False - if document has been already marked
        """
        mapping_ids = self.doc_to_get.setdefault(action["_index"], {}).setdefault(
            action["_type"], set()
        )
        if action["_id"] in mapping_ids:
            # There is an update on this id already
            return False
        else:
            mapping_ids.add(action["_id"])
            return True

    def get_docs_sources_from_ES(self):
        """Get document sources using MGET elasticsearch API"""
        docs = [doc for doc, _, _, get_from_ES in self.doc_to_update if get_from_ES]
        if docs:
            documents = self.docman.elastic.mget(body={"docs": docs}, realtime=True)
            return iter(documents["docs"])
        else:
            return iter([])

    @wrap_exceptions
    def update_sources(self):
        """Update local sources based on response from Elasticsearch"""
        ES_documents = self.get_docs_sources_from_ES()

        @ERROR_TIME.time()
        def error_catch(error):
            error.inc()

        for doc, update_spec, action_buffer_index, get_from_ES in self.doc_to_update:
            if get_from_ES:
                # Update source based on response from ES
                ES_doc = next(ES_documents)
                if ES_doc["found"]:
                    source = ES_doc["_source"]
                else:
                    # Document not found in elasticsearch,
                    # Seems like something went wrong during replication
                    LOG.error(
                        "mGET: Document id: %s has not been found "
                        "in Elasticsearch. Due to that "
                        "following update failed: %s",
                        doc["_id"],
                        update_spec,
                    )

                    error_res = {
                        doc: doc,
                        update_spec: update_spec
                    }
                    error_catch(ERROR_CAUGHT.labels('Could not bulk-upsert document into ElasticSearch', error_res))

                    self.reset_action(action_buffer_index)
                    continue
            else:
                # Get source stored locally before applying update
                # as it is up-to-date
                source = self.get_from_sources(doc["_index"], doc["_type"], doc["_id"])
                if not source:
                    LOG.error(
                        "mGET: Document id: %s has not been found "
                        "in local sources. Due to that following "
                        "update failed: %s",
                        doc["_id"],
                        update_spec,
                    )

                    error_res = {
                        doc: doc,
                        update_spec: update_spec
                    }
                    error_catch(self.ERROR_CAUGHT.labels('mGET: Document id has not been found in local sources. Due to that following update failed', error_res))

                    self.reset_action(action_buffer_index)
                    continue

            updated = self.docman.apply_update(source, update_spec)

            # Remove _id field from source
            if "_id" in updated:
                del updated["_id"]

            # Everytime update locally stored sources to keep them up-to-date
            self.add_to_sources(doc, updated)

            current_doc = self.docman._formatter.format_document(updated)

            if os.environ.get('JOIN_INDEX') and (doc["_index"] == os.environ.get('JOIN_INDEX')):
                if current_doc.get(os.environ.get('CHILD_FIELD_1')) and \
                        current_doc.get(os.environ.get('CHILD_FIELD_2')):
                    current_doc["data_join"] = {
                        "name": os.environ.get('JOIN_FIELD'),
                        "parent": current_doc.get(os.environ.get('JOIN_FIELD'))
                    }
                    doc["_routing"] = current_doc.get(os.environ.get('JOIN_FIELD'))
                    self.action_buffer[action_buffer_index]["_routing"] = current_doc.get(
                        os.environ.get('JOIN_FIELD')
                    )
                else:
                    current_doc["data_join"] = { "name": "_id" }

            self.action_buffer[action_buffer_index][
                "_source"
            ] = current_doc

            doc['_update'] = True
            self.action_buffer[action_buffer_index]['_update'] = True

        # Remove empty actions if there were errors
        self.action_buffer = [
            each_action for each_action in self.action_buffer if each_action
        ]

    def reset_action(self, action_buffer_index):
        """Reset specific action as update failed"""
        self.action_buffer[action_buffer_index] = {}
        self.action_buffer[action_buffer_index + 1] = {}

    def add_to_sources(self, action, doc_source):
        """Store sources locally"""
        mapping = self.sources.setdefault(action["_index"], {}).setdefault(
            action["_type"], {}
        )
        mapping[action["_id"]] = doc_source

    def get_from_sources(self, index, doc_type, document_id):
        """Get source stored locally"""
        return self.sources.get(index, {}).get(doc_type, {}).get(document_id, {})

    def bulk_index(self, action, meta_action):
        self.action_buffer.append(action)
        self.action_buffer.append(meta_action)

    def clean_up(self):
        """Do clean-up before returning buffer"""
        self.action_buffer = []
        self.sources = {}
        self.doc_to_get = {}
        self.doc_to_update = []

    def get_buffer(self):
        """Get buffer which needs to be bulked to elasticsearch"""

        # Get sources for documents which are in Elasticsearch
        # and they are not in local buffer
        if self.doc_to_update:
            self.update_sources()

        ES_buffer = self.action_buffer
        self.clean_up()
        return ES_buffer
