#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from apache_polaris.sdk.management import (
    ConnectionConfigInfo,
    IcebergRestConnectionConfigInfo,
    HadoopConnectionConfigInfo,
    HiveConnectionConfigInfo,
)


def test_iceberg_rest_connection_config_deserialization():
    data = {
        "connectionType": "ICEBERG_REST",
        "uri": "https://example.com/api/catalog",
        "remoteCatalogName": "my_remote_catalog",
        "properties": {"key": "val"},
    }
    config = ConnectionConfigInfo.from_dict(data)
    assert isinstance(config, IcebergRestConnectionConfigInfo)
    assert config.connection_type == "ICEBERG_REST"
    assert config.uri == "https://example.com/api/catalog"
    assert config.remote_catalog_name == "my_remote_catalog"
    assert config.properties == {"key": "val"}


def test_hadoop_connection_config_deserialization():
    data = {
        "connectionType": "HADOOP",
        "uri": "hdfs://namenode:8020/warehouse",
        "warehouse": "/user/hive/warehouse",
    }
    config = ConnectionConfigInfo.from_dict(data)
    assert isinstance(config, HadoopConnectionConfigInfo)
    assert config.connection_type == "HADOOP"
    assert config.uri == "hdfs://namenode:8020/warehouse"
    assert config.warehouse == "/user/hive/warehouse"


def test_hive_connection_config_deserialization():
    data = {
        "connectionType": "HIVE",
        "uri": "thrift://metastore:9083",
        "warehouse": "s3://my-bucket/hive/warehouse",
    }
    config = ConnectionConfigInfo.from_dict(data)
    assert isinstance(config, HiveConnectionConfigInfo)
    assert config.connection_type == "HIVE"
    assert config.uri == "thrift://metastore:9083"
    assert config.warehouse == "s3://my-bucket/hive/warehouse"
