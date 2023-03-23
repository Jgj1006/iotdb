# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import logging
import random
import struct
import time


from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport

from iotdb.utils.SessionDataSet import SessionDataSet
from .thrift.rpc.ttypes import (
    EndPoint,
    TSRawDataQueryReq,
    TSLastDataQueryReq,
    TSInsertStringRecordsOfOneDeviceReq,
)
from .thrift.rpc.TSIService import (
    Client,
    TSCreateTimeseriesReq,
    TSCreateAlignedTimeseriesReq,
    TSInsertRecordReq,
    TSInsertStringRecordReq,
    TSInsertTabletReq,
    TSExecuteStatementReq,
    TSOpenSessionReq,
    TSCreateMultiTimeseriesReq,
    TSCloseSessionReq,
    TSInsertTabletsReq,
    TSInsertRecordsReq,
    TSInsertRecordsOfOneDeviceReq,
)
from .thrift.rpc.ttypes import TSDeleteDataReq, TSProtocolVersion, TSSetTimeZoneReq

# for debug
# from IoTDBConstants import *
# from SessionDataSet import SessionDataSet
#
# from thrift.protocol import TBinaryProtocol, TCompactProtocol
# from thrift.transport import TSocket, TTransport
#
# from iotdb.rpc.TSIService import Client, TSCreateTimeseriesReq, TSInsertRecordReq, TSInsertTabletReq, \
#      TSExecuteStatementReq, TSOpenSessionReq, TSQueryDataSet, TSFetchResultsReq, TSCloseOperationReq, \
#      TSCreateMultiTimeseriesReq, TSCloseSessionReq, TSInsertTabletsReq, TSInsertRecordsReq
# from iotdb.rpc.ttypes import TSDeleteDataReq, TSProtocolVersion, TSSetTimeZoneReq
from .utils.IoTDBConstants import TSDataType

logger = logging.getLogger("IoTDB")


class Session(object):
    SUCCESS_CODE = 200
    MULTIPLE_ERROR = 506
    DEFAULT_FETCH_SIZE = 10000
    DEFAULT_USER = "root"
    DEFAULT_PASSWORD = "root"
    DEFAULT_ZONE_ID = time.strftime("%z")
    RETRY_NUM = 3

    def __init__(
        self,
        host,
        port,
        user=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        fetch_size=DEFAULT_FETCH_SIZE,
        zone_id=DEFAULT_ZONE_ID,
    ):
        self.__host = host
        self.__port = port
        self.__hosts = None
        self.__ports = None
        self.__default_endpoint = EndPoint(self.__host, self.__port)
        self.__user = user
        self.__password = password
        self.__fetch_size = fetch_size
        self.__is_close = True
        self.__transport = None
        self.__client = None
        self.protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
        self.__session_id = None
        self.__statement_id = None
        self.__zone_id = zone_id
        self.__enable_rpc_compression = None

    @classmethod
    def init_from_node_urls(
        cls,
        node_urls,
        user=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
        fetch_size=DEFAULT_FETCH_SIZE,
        zone_id=DEFAULT_ZONE_ID,
    ):
        if node_urls is None:
            raise RuntimeError("node urls is empty")
        session = Session(None, None, user, password, fetch_size, zone_id)
        session.__hosts = []
        session.__ports = []
        for node_url in node_urls:
            split = node_url.split(":")
            session.__hosts.append(split[0])
            session.__ports.append(split[1])
        session.__host = session.__hosts[0]
        session.__port = session.__ports[0]
        session.__default_endpoint = EndPoint(session.__host, session.__port)
        return session

    def open(self, enable_rpc_compression):
        if not self.__is_close:
            return
        self.__enable_rpc_compression = enable_rpc_compression
        if self.__hosts is None:
            self.init_connection(self.__default_endpoint)
        else:
            for i in range(0, len(self.__hosts)):
                self.__default_endpoint = EndPoint(self.__hosts[i], self.__ports[i])
                try:
                    self.init_connection(self.__default_endpoint)
                except Exception as e:
                    if not self.reconnect():
                        logger.error("Cluster has no nodes to connect")
                        raise e
                break

    def init_connection(self, endpoint):
        self.__transport = TTransport.TFramedTransport(
            TSocket.TSocket(endpoint.ip, endpoint.port)
        )

        if not self.__transport.isOpen():
            try:
                self.__transport.open()
            except TTransport.TTransportException as e:
                raise e

        if self.__enable_rpc_compression:
            self.__client = Client(TCompactProtocol.TCompactProtocol(self.__transport))
        else:
            self.__client = Client(TBinaryProtocol.TBinaryProtocol(self.__transport))

        open_req = TSOpenSessionReq(
            client_protocol=self.protocol_version,
            username=self.__user,
            password=self.__password,
            zoneId=self.__zone_id,
            configuration={"version": "V_0_13"},
        )

        try:
            open_resp = self.__client.openSession(open_req)
            Session.verify_success(open_resp.status)
            if self.protocol_version != open_resp.serverProtocolVersion:
                logger.exception(
                    "Protocol differ, Client version is {}, but Server version is {}".format(
                        self.protocol_version, open_resp.serverProtocolVersion
                    )
                )
                # version is less than 0.10
                if open_resp.serverProtocolVersion == 0:
                    raise TTransport.TException(message="Protocol not supported.")

            self.__session_id = open_resp.sessionId
            self.__statement_id = self.__client.requestStatementId(self.__session_id)

        except Exception as e:
            self.__transport.close()
            logger.exception("session closed because: ", exc_info=e)
            raise e

        if self.__zone_id is not None:
            self.set_time_zone(self.__zone_id)
        else:
            self.__zone_id = self.get_time_zone()

        self.__is_close = False

    def is_open(self):
        return not self.__is_close

    def close(self):
        if self.__is_close:
            return
        req = TSCloseSessionReq(self.__session_id)
        try:
            self.__client.closeSession(req)
        except TTransport.TException as e:
            logger.exception(
                "Error occurs when closing session at server. Maybe server is down. Error message: ",
                exc_info=e,
            )
        finally:
            self.__is_close = True
            if self.__transport is not None:
                self.__transport.close()

    def set_storage_group(self, group_name):
        """
        set one storage group
        :param group_name: String, storage group name (starts from root)
        """
        try:
            return Session.verify_success(
                self.__client.setStorageGroup(self.__session_id, group_name)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.setStorageGroup(self.__session_id, group_name)
                    )
                except TTransport.TException as e1:
                    logger.exception("create databases fails because: ", e1)
                    raise e1
            else:
                raise e

    def delete_storage_group(self, storage_group):
        """
        delete one storage group.
        :param storage_group: String, path of the target storage group.
        """
        groups = [storage_group]
        return self.delete_storage_groups(groups)

    def delete_storage_groups(self, storage_group_lst):
        """
        delete multiple storage groups.
        :param storage_group_lst: List, paths of the target storage groups.
        """
        try:
            return Session.verify_success(
                self.__client.deleteStorageGroups(self.__session_id, storage_group_lst)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.deleteStorageGroups(
                            self.__session_id, storage_group_lst
                        )
                    )
                except TTransport.TException as e1:
                    logger.exception("delete database fails because: ", e1)
                    raise e1
            else:
                raise e

    def create_time_series(
        self,
        ts_path,
        data_type,
        encoding,
        compressor,
        props=None,
        tags=None,
        attributes=None,
        alias=None,
    ):
        """
        create single time series
        :param ts_path: String, complete time series path (starts from root)
        :param data_type: TSDataType, data type for this time series
        :param encoding: TSEncoding, encoding for this time series
        :param compressor: Compressor, compressing type for this time series
        :param props: Dictionary, properties for time series
        :param tags: Dictionary, tag map for time series
        :param attributes: Dictionary, attribute map for time series
        :param alias: String, measurement alias for time series
        """
        data_type = data_type.value
        encoding = encoding.value
        compressor = compressor.value
        request = TSCreateTimeseriesReq(
            self.__session_id,
            ts_path,
            data_type,
            encoding,
            compressor,
            props,
            tags,
            attributes,
            alias,
        )
        try:
            return Session.verify_success(self.__client.createTimeseries(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createTimeseries(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("creating time series fails because: ", e1)
                    raise e1
            else:
                raise e

    def create_aligned_time_series(
        self, device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
    ):
        """
        create aligned time series
        :param device_id: String, device id for timeseries (starts from root)
        :param measurements_lst: List of String, measurement ids for time series
        :param data_type_lst: List of TSDataType, data types for time series
        :param encoding_lst: List of TSEncoding, encodings for time series
        :param compressor_lst: List of Compressor, compressing types for time series
        """
        data_type_lst = [data_type.value for data_type in data_type_lst]
        encoding_lst = [encoding.value for encoding in encoding_lst]
        compressor_lst = [compressor.value for compressor in compressor_lst]

        request = TSCreateAlignedTimeseriesReq(
            self.__session_id,
            device_id,
            measurements_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
        )
        try:
            return Session.verify_success(
                self.__client.createAlignedTimeseries(request)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createAlignedTimeseries(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("creating time series fails because: ", e1)
                    raise e1
            raise e

    def create_multi_time_series(
        self,
        ts_path_lst,
        data_type_lst,
        encoding_lst,
        compressor_lst,
        props_lst=None,
        tags_lst=None,
        attributes_lst=None,
        alias_lst=None,
    ):
        """
        create multiple time series
        :param ts_path_lst: List of String, complete time series paths (starts from root)
        :param data_type_lst: List of TSDataType, data types for time series
        :param encoding_lst: List of TSEncoding, encodings for time series
        :param compressor_lst: List of Compressor, compressing types for time series
        :param props_lst: List of Props Dictionary, properties for time series
        :param tags_lst: List of tag Dictionary, tag maps for time series
        :param attributes_lst: List of attribute Dictionary, attribute maps for time series
        :param alias_lst: List of alias, measurement alias for time series
        """
        data_type_lst = [data_type.value for data_type in data_type_lst]
        encoding_lst = [encoding.value for encoding in encoding_lst]
        compressor_lst = [compressor.value for compressor in compressor_lst]

        request = TSCreateMultiTimeseriesReq(
            self.__session_id,
            ts_path_lst,
            data_type_lst,
            encoding_lst,
            compressor_lst,
            props_lst,
            tags_lst,
            attributes_lst,
            alias_lst,
        )
        try:
            return Session.verify_success(self.__client.createMultiTimeseries(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.createMultiTimeseries(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("creating multi time series fails because: ", e1)
                    raise e1
            else:
                raise e

    def delete_time_series(self, paths_list):
        """
        delete multiple time series, including data and schema
        :param paths_list: List of time series path, which should be complete (starts from root)
        """
        try:
            return Session.verify_success(
                self.__client.deleteTimeseries(self.__session_id, paths_list)
            )
        except TTransport.TException:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.deleteTimeseries(self.__session_id, paths_list)
                    )
                except TTransport.TException as e1:
                    logger.exception("deleting time series fails because: ", e1)
                    raise e1

    def check_time_series_exists(self, path):
        """
        check whether a specific time series exists
        :param path: String, complete path of time series for checking
        :return Boolean value indicates whether it exists.
        """
        data_set = self.execute_query_statement("SHOW TIMESERIES {}".format(path))
        result = data_set.has_next()
        data_set.close_operation_handle()
        return result

    def delete_data(self, paths_list, end_time):
        """
        delete all data <= end_time in multiple time series
        :param paths_list: time series list that the data in.
        :param end_time: data with time stamp less than or equal to time will be deleted.
        """
        request = TSDeleteDataReq(
            self.__session_id, paths_list, -9223372036854775808, end_time
        )
        try:
            return Session.verify_success(self.__client.deleteData(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.deleteData(request))
                except TTransport.TException as e1:
                    logger.exception("data deletion fails because: ", e1)
                    raise e1
            else:
                raise e

    def delete_data_in_range(self, paths_list, start_time, end_time):
        """
        delete data >= start_time and data <= end_time in multiple timeseries
        :param paths_list: time series list that the data in.
        :param start_time: delete range start time.
        :param end_time: delete range end time.
        """
        request = TSDeleteDataReq(self.__session_id, paths_list, start_time, end_time)
        try:
            return Session.verify_success(self.__client.deleteData(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.deleteData(request))
                except TTransport.TException as e1:
                    logger.exception("data deletion fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_str_record(self, device_id, timestamp, measurements, string_values):
        """special case for inserting one row of String (TEXT) value"""
        if type(string_values) == str:
            string_values = [string_values]
        if type(measurements) == str:
            measurements = [measurements]
        request = self.gen_insert_str_record_req(
            device_id, timestamp, measurements, string_values
        )
        try:
            return Session.verify_success(self.__client.insertStringRecord(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertStringRecord(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_aligned_str_record(
        self, device_id, timestamp, measurements, string_values
    ):
        """special case for inserting one row of String (TEXT) value"""
        if type(string_values) == str:
            string_values = [string_values]
        if type(measurements) == str:
            measurements = [measurements]
        request = self.gen_insert_str_record_req(
            device_id, timestamp, measurements, string_values, True
        )
        try:
            return Session.verify_success(self.__client.insertStringRecord(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertStringRecord(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_record(self, device_id, timestamp, measurements, data_types, values):
        """
        insert one row of record into database, if you want improve your performance, please use insertTablet method
            for example a record at time=10086 with three measurements is:
                timestamp,     m1,    m2,     m3
                    10086,  125.3,  True,  text1
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        data_types = [data_type.value for data_type in data_types]
        request = self.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values
        )
        try:
            return Session.verify_success(self.__client.insertRecord(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertRecord(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_records(
        self, device_ids, times, measurements_lst, types_lst, values_lst
    ):
        """
        insert multiple rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        type_values_lst = []
        for types in types_lst:
            data_types = [data_type.value for data_type in types]
            type_values_lst.append(data_types)
        request = self.gen_insert_records_req(
            device_ids, times, measurements_lst, type_values_lst, values_lst
        )
        try:
            return Session.verify_success(self.__client.insertRecords(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertRecords(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_aligned_record(
        self, device_id, timestamp, measurements, data_types, values
    ):
        """
        insert one row of aligned record into database, if you want improve your performance, please use insertTablet method
            for example a record at time=10086 with three measurements is:
                timestamp,     m1,    m2,     m3
                    10086,  125.3,  True,  text1
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        data_types = [data_type.value for data_type in data_types]
        request = self.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values, True
        )
        try:
            return Session.verify_success(self.__client.insertRecord(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertRecord(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_aligned_records(
        self, device_ids, times, measurements_lst, types_lst, values_lst
    ):
        """
        insert multiple aligned rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        type_values_lst = []
        for types in types_lst:
            data_types = [data_type.value for data_type in types]
            type_values_lst.append(data_types)
        request = self.gen_insert_records_req(
            device_ids, times, measurements_lst, type_values_lst, values_lst, True
        )
        try:
            return Session.verify_success(self.__client.insertRecords(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertRecords(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def test_insert_record(
        self, device_id, timestamp, measurements, data_types, values
    ):
        """
        this method NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        """
        data_types = [data_type.value for data_type in data_types]
        request = self.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values
        )
        try:
            return Session.verify_success(self.__client.testInsertRecord(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.testInsertRecord(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("test insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def test_insert_records(
        self, device_ids, times, measurements_lst, types_lst, values_lst
    ):
        """
        this method NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        """
        type_values_lst = []
        for types in types_lst:
            data_types = [data_type.value for data_type in types]
            type_values_lst.append(data_types)
        request = self.gen_insert_records_req(
            device_ids, times, measurements_lst, type_values_lst, values_lst
        )
        try:
            return Session.verify_success(self.__client.testInsertRecords(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.testInsertRecords(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("test insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def gen_insert_record_req(
        self, device_id, timestamp, measurements, data_types, values, is_aligned=False
    ):
        if (len(values) != len(data_types)) or (len(values) != len(measurements)):
            raise RuntimeError(
                "length of data types does not equal to length of values!"
            )
        values_in_bytes = Session.value_to_bytes(data_types, values)
        return TSInsertRecordReq(
            self.__session_id,
            device_id,
            measurements,
            values_in_bytes,
            timestamp,
            is_aligned,
        )

    def gen_insert_str_record_req(
        self, device_id, timestamp, measurements, values, is_aligned=False
    ):
        if len(values) != len(measurements):
            raise RuntimeError(
                "length of measurements does not equal to length of values!"
            )
        return TSInsertStringRecordReq(
            self.__session_id, device_id, measurements, values, timestamp, is_aligned
        )

    def gen_insert_records_req(
        self,
        device_ids,
        times,
        measurements_lst,
        types_lst,
        values_lst,
        is_aligned=False,
    ):
        if (
            (len(device_ids) != len(measurements_lst))
            or (len(times) != len(types_lst))
            or (len(device_ids) != len(times))
            or (len(times) != len(values_lst))
        ):
            raise RuntimeError(
                "deviceIds, times, measurementsList and valuesList's size should be equal"
            )

        value_lst = []
        for values, data_types, measurements in zip(
            values_lst, types_lst, measurements_lst
        ):
            if (len(values) != len(data_types)) or (len(values) != len(measurements)):
                raise RuntimeError(
                    "deviceIds, times, measurementsList and valuesList's size should be equal"
                )
            values_in_bytes = Session.value_to_bytes(data_types, values)
            value_lst.append(values_in_bytes)

        return TSInsertRecordsReq(
            self.__session_id,
            device_ids,
            measurements_lst,
            value_lst,
            times,
            is_aligned,
        )

    def insert_tablet(self, tablet):
        """
        insert one tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        """
        request = self.gen_insert_tablet_req(tablet)
        try:
            return Session.verify_success(self.__client.insertTablet(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablet(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_tablets(self, tablet_lst):
        """
        insert multiple tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        """
        request = self.gen_insert_tablets_req(tablet_lst)
        try:
            return Session.verify_success(self.__client.insertTablets(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablets(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_aligned_tablet(self, tablet):
        """
        insert one aligned tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        """
        request = self.gen_insert_tablet_req(tablet, True)
        try:
            return Session.verify_success(self.__client.insertTablet(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablet(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_aligned_tablets(self, tablet_lst):
        """
        insert multiple aligned tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        """
        request = self.gen_insert_tablets_req(tablet_lst, True)
        try:
            return Session.verify_success(self.__client.insertTablets(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(self.__client.insertTablets(request))
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_records_of_one_device(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        # sort by timestamp
        sorted_zipped = sorted(
            zip(times_list, measurements_list, types_list, values_list)
        )
        result = zip(*sorted_zipped)
        times_list, measurements_list, types_list, values_list = [
            list(x) for x in result
        ]

        return self.insert_records_of_one_device_sorted(
            device_id, times_list, measurements_list, types_list, values_list
        )

    def insert_records_of_one_device_sorted(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        """
        Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
        executeBatch, we pack some insert request in batch and send them to server. If you want improve
        your performance, please see insertTablet method

        :param device_id: device id
        :param times_list: timestamps list
        :param measurements_list: measurements list
        :param types_list: types list
        :param values_list: values list
        :param have_sorted: have these list been sorted by timestamp
        """
        # check parameter
        size = len(times_list)
        if (
            size != len(measurements_list)
            or size != len(types_list)
            or size != len(values_list)
        ):
            raise RuntimeError(
                "insert records of one device error: types, times, measurementsList and valuesList's size should be equal"
            )

        # check sorted
        if not Session.check_sorted(times_list):
            raise RuntimeError(
                "insert records of one device error: timestamp not sorted"
            )

        request = self.gen_insert_records_of_one_device_request(
            device_id, times_list, measurements_list, values_list, types_list
        )
        try:
            return Session.verify_success(
                self.__client.insertRecordsOfOneDevice(request)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertRecordsOfOneDevice(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def insert_aligned_records_of_one_device(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        # sort by timestamp
        sorted_zipped = sorted(
            zip(times_list, measurements_list, types_list, values_list)
        )
        result = zip(*sorted_zipped)
        times_list, measurements_list, types_list, values_list = [
            list(x) for x in result
        ]

        return self.insert_aligned_records_of_one_device_sorted(
            device_id, times_list, measurements_list, types_list, values_list
        )

    def insert_aligned_records_of_one_device_sorted(
        self, device_id, times_list, measurements_list, types_list, values_list
    ):
        """
        Insert multiple aligned rows, which can reduce the overhead of network. This method is just like jdbc
        executeBatch, we pack some insert request in batch and send them to server. If you want to improve
        your performance, please see insertTablet method

        :param device_id: device id
        :param times_list: timestamps list
        :param measurements_list: measurements list
        :param types_list: types list
        :param values_list: values list
        """
        # check parameter
        size = len(times_list)
        if (
            size != len(measurements_list)
            or size != len(types_list)
            or size != len(values_list)
        ):
            raise RuntimeError(
                "insert records of one device error: types, times, measurementsList and valuesList's size should be equal"
            )

        # check sorted
        if not Session.check_sorted(times_list):
            raise RuntimeError(
                "insert records of one device error: timestamp not sorted"
            )

        request = self.gen_insert_records_of_one_device_request(
            device_id, times_list, measurements_list, values_list, types_list, True
        )

        # send request
        try:
            return Session.verify_success(
                self.__client.insertRecordsOfOneDevice(request)
            )
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.insertRecordsOfOneDevice(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def gen_insert_records_of_one_device_request(
        self,
        device_id,
        times_list,
        measurements_list,
        values_list,
        types_list,
        is_aligned=False,
    ):
        binary_value_list = []
        for values, data_types, measurements in zip(
            values_list, types_list, measurements_list
        ):
            data_types = [data_type.value for data_type in data_types]
            if (len(values) != len(data_types)) or (len(values) != len(measurements)):
                raise RuntimeError(
                    "insert records of one device error: deviceIds, times, measurementsList and valuesList's size should be equal"
                )
            values_in_bytes = Session.value_to_bytes(data_types, values)
            binary_value_list.append(values_in_bytes)

        return TSInsertRecordsOfOneDeviceReq(
            self.__session_id,
            device_id,
            measurements_list,
            binary_value_list,
            times_list,
            is_aligned,
        )

    def test_insert_tablet(self, tablet):
        """
         this method NOT insert data into database and the server just return after accept the request, this method
         should be used to test other time cost in client
        :param tablet: a tablet of data
        """
        try:
            request = self.gen_insert_tablet_req(tablet)
            return Session.verify_success(self.__client.testInsertTablet(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    return Session.verify_success(
                        self.__client.testInsertTablet(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("test insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def test_insert_tablets(self, tablet_list):
        """
         this method NOT insert data into database and the server just return after accept the request, this method
         should be used to test other time cost in client
        :param tablet_list: List of tablets
        """
        try:
            request = self.gen_insert_tablets_req(tablet_list)
            return Session.verify_success(self.__client.testInsertTablets(request))
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    return Session.verify_success(
                        self.__client.testInsertTablets(request)
                    )
                except TTransport.TException as e1:
                    logger.exception("test insert fails because: ", e1)
                    raise e1
            else:
                raise e

    def gen_insert_tablet_req(self, tablet, is_aligned=False):
        data_type_values = [data_type.value for data_type in tablet.get_data_types()]
        return TSInsertTabletReq(
            self.__session_id,
            tablet.get_device_id(),
            tablet.get_measurements(),
            tablet.get_binary_values(),
            tablet.get_binary_timestamps(),
            data_type_values,
            tablet.get_row_number(),
            is_aligned,
        )

    def gen_insert_tablets_req(self, tablet_lst, is_aligned=False):
        device_id_lst = []
        measurements_lst = []
        values_lst = []
        timestamps_lst = []
        type_lst = []
        size_lst = []
        for tablet in tablet_lst:
            data_type_values = [
                data_type.value for data_type in tablet.get_data_types()
            ]
            device_id_lst.append(tablet.get_device_id())
            measurements_lst.append(tablet.get_measurements())
            values_lst.append(tablet.get_binary_values())
            timestamps_lst.append(tablet.get_binary_timestamps())
            type_lst.append(data_type_values)
            size_lst.append(tablet.get_row_number())
        return TSInsertTabletsReq(
            self.__session_id,
            device_id_lst,
            measurements_lst,
            values_lst,
            timestamps_lst,
            type_lst,
            size_lst,
            is_aligned,
        )

    def execute_query_statement(self, sql, timeout=0):
        """
        execute query sql statement and returns SessionDataSet
        :param sql: String, query sql statement
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        """
        request = TSExecuteStatementReq(
            self.__session_id, sql, self.__statement_id, self.__fetch_size, timeout
        )
        try:
            resp = self.__client.executeQueryStatement(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeQueryStatement(request)
                except TTransport.TException as e1:
                    logger.exception("execution of query statement fails because: ", e1)
                    raise e1
            else:
                raise e
        Session.verify_success(resp.status)
        return SessionDataSet(
            sql,
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            self.__client,
            self.__statement_id,
            self.__session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp,
        )

    def execute_non_query_statement(self, sql):
        """
        execute non-query sql statement
        :param sql: String, non-query sql statement
        """
        request = TSExecuteStatementReq(self.__session_id, sql, self.__statement_id)
        try:
            resp = self.__client.executeUpdateStatement(request)
            status = resp.status
            return Session.verify_success(status)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeUpdateStatement(request)
                    status = resp.status
                    return Session.verify_success(status)
                except TTransport.TException as e1:
                    logger.exception(
                        "execution of non-query statement fails because: ", e1
                    )
                    raise e1
            else:
                raise e

    @staticmethod
    def value_to_bytes(data_types, values):
        format_str_list = [">"]
        values_tobe_packed = []
        for data_type, value in zip(data_types, values):
            if data_type == TSDataType.BOOLEAN.value:
                format_str_list.append("c")
                format_str_list.append("?")
                values_tobe_packed.append(bytes([TSDataType.BOOLEAN.value]))
                values_tobe_packed.append(value)
            elif data_type == TSDataType.INT32.value:
                format_str_list.append("c")
                format_str_list.append("i")
                values_tobe_packed.append(bytes([TSDataType.INT32.value]))
                values_tobe_packed.append(value)
            elif data_type == TSDataType.INT64.value:
                format_str_list.append("c")
                format_str_list.append("q")
                values_tobe_packed.append(bytes([TSDataType.INT64.value]))
                values_tobe_packed.append(value)
            elif data_type == TSDataType.FLOAT.value:
                format_str_list.append("c")
                format_str_list.append("f")
                values_tobe_packed.append(bytes([TSDataType.FLOAT.value]))
                values_tobe_packed.append(value)
            elif data_type == TSDataType.DOUBLE.value:
                format_str_list.append("c")
                format_str_list.append("d")
                values_tobe_packed.append(bytes([TSDataType.DOUBLE.value]))
                values_tobe_packed.append(value)
            elif data_type == TSDataType.TEXT.value:
                value_bytes = bytes(value, "utf-8")
                format_str_list.append("c")
                format_str_list.append("i")
                format_str_list.append(str(len(value_bytes)))
                format_str_list.append("s")
                values_tobe_packed.append(bytes([TSDataType.TEXT.value]))
                values_tobe_packed.append(len(value_bytes))
                values_tobe_packed.append(value_bytes)
            else:
                raise RuntimeError("Unsupported data type:" + str(data_type))
        format_str = "".join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

    def get_time_zone(self):
        if self.__zone_id is not None:
            return self.__zone_id
        try:
            resp = self.__client.getTimeZone(self.__session_id)
        except TTransport.TException as e:
            raise RuntimeError("Could not get time zone because: ", e)
        return resp.timeZone

    def set_time_zone(self, zone_id):
        request = TSSetTimeZoneReq(self.__session_id, zone_id)
        try:
            status = self.__client.setTimeZone(request)
            logger.debug(
                "setting time zone_id as {}, message: {}".format(
                    zone_id, status.message
                )
            )
        except TTransport.TException as e:
            raise RuntimeError("Could not set time zone because: ", e)
        self.__zone_id = zone_id

    @staticmethod
    def check_sorted(timestamps):
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i - 1]:
                return False
        return True

    @staticmethod
    def verify_success(status):
        """
        verify success of operation
        :param status: execution result status
        """
        if status.code == Session.MULTIPLE_ERROR:
            Session.verify_success_by_list(status.subStatus)
            return 0
        if status.code == Session.SUCCESS_CODE:
            return 0

        logger.error("error status is %s", status)
        raise RuntimeError(str(status.code) + ": " + status.message)

    @staticmethod
    def verify_success_by_list(status_list):
        """
        verify success of operation
        :param status_list: execution result status
        """
        message = str(Session.MULTIPLE_ERROR) + ": "
        for status in status_list:
            if status.code != Session.SUCCESS_CODE:
                message += status.message + "; "
        raise RuntimeError(message)

    def execute_raw_data_query(
        self, paths: list, start_time: int, end_time: int
    ) -> SessionDataSet:
        """
        execute query statement and returns SessionDataSet
        :param paths: String path list
        :param start_time: Query start time
        :param end_time: Query end time
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        """
        request = TSRawDataQueryReq(
            self.__session_id,
            paths,
            self.__fetch_size,
            startTime=start_time,
            endTime=end_time,
            statementId=self.__statement_id,
            enableRedirectQuery=False,
        )
        try:
            resp = self.__client.executeRawDataQuery(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeRawDataQuery(request)
                except TTransport.TException as e1:
                    logger.exception("execution of query statement fails because: ", e1)
                    raise e1
            else:
                raise e
        Session.verify_success(resp.status)
        return SessionDataSet(
            "",
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            self.__client,
            self.__statement_id,
            self.__session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp,
        )

    def execute_last_data_query(self, paths: list, last_time: int) -> SessionDataSet:
        """
        execute query statement and returns SessionDataSet
        :param paths: String path list
        :param last_time: Query last time
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        """
        request = TSLastDataQueryReq(
            self.__session_id,
            paths,
            self.__fetch_size,
            last_time,
            self.__statement_id,
            enableRedirectQuery=False,
        )
        try:
            resp = self.__client.executeLastDataQuery(request)
        except TTransport.TException as e:
            if self.reconnect():
                try:
                    request.sessionId = self.__session_id
                    request.statementId = self.__statement_id
                    resp = self.__client.executeLastDataQuery(request)
                except TTransport.TException as e1:
                    logger.exception("execution of query statement fails because: ", e1)
                    raise e1
            else:
                raise e
        Session.verify_success(resp.status)
        return SessionDataSet(
            "",
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            self.__client,
            self.__statement_id,
            self.__session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp,
        )

    def reconnect(self):
        if self.__hosts is None:
            return False
        connected = False
        for i in range(1, self.RETRY_NUM + 1):
            if self.__transport is not None:
                self.__transport.close()
                curr_host_index = random.randint(0, len(self.__hosts))
                try_host_num = 0
                for j in range(curr_host_index, len(self.__hosts)):
                    if try_host_num == len(self.__hosts):
                        break
                    self.__default_endpoint = EndPoint(self.__hosts[j], self.__ports[j])
                    if j == len(self.__hosts) - 1:
                        j = -1
                    try_host_num += 1
                    try:
                        self.init_connection(self.__default_endpoint)
                        connected = True
                    except TTransport.TException as e:
                        continue
                    break
            if connected:
                break
        return connected
