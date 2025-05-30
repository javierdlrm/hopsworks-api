#
#   Copyright 2022 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import decimal
from datetime import date, datetime

import hopsworks_common
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from hopsworks_common.core.constants import HAS_POLARS
from hsfs import (
    feature,
    feature_group,
    feature_view,
    storage_connector,
    training_dataset,
    util,
)
from hsfs.client import exceptions
from hsfs.constructor import query
from hsfs.constructor.hudi_feature_group_alias import HudiFeatureGroupAlias
from hsfs.core import inode, job, online_ingestion
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS
from hsfs.engine import python
from hsfs.expectation_suite import ExpectationSuite
from hsfs.hopsworks_udf import udf
from hsfs.training_dataset_feature import TrainingDatasetFeature


if HAS_POLARS:
    import polars as pl
    from polars.testing import assert_frame_equal as polars_assert_frame_equal


hopsworks_common.connection._hsfs_engine_type = "python"


class TestPython:
    def test_sql(self, mocker):
        # Arrange
        mock_python_engine_sql_offline = mocker.patch(
            "hsfs.engine.python.Engine._sql_offline"
        )
        mock_python_engine_jdbc = mocker.patch("hsfs.engine.python.Engine._jdbc")

        python_engine = python.Engine()

        # Act
        python_engine.sql(
            sql_query=None,
            feature_store=None,
            online_conn=None,
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_python_engine_sql_offline.call_count == 1
        assert mock_python_engine_jdbc.call_count == 0

    def test_sql_online_conn(self, mocker):
        # Arrange
        mock_python_engine_sql_offline = mocker.patch(
            "hsfs.engine.python.Engine._sql_offline"
        )
        mock_python_engine_jdbc = mocker.patch("hsfs.engine.python.Engine._jdbc")

        python_engine = python.Engine()

        # Act
        python_engine.sql(
            sql_query=None,
            feature_store=None,
            online_conn=mocker.Mock(),
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_python_engine_sql_offline.call_count == 0
        assert mock_python_engine_jdbc.call_count == 1

    def test_jdbc(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch(
            "hsfs.core.util_sql.create_mysql_engine"
        )
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client._is_external")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=query, connector=None, dataframe_type="default", read_options={}
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_jdbc_dataframe_type_none(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.util_sql.create_mysql_engine")
        mocker.patch("hopsworks_common.client.get_instance")
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as fstore_except:
            python_engine._jdbc(
                sql_query=query, connector=None, dataframe_type=None, read_options={}
            )

        # Assert
        assert (
            str(fstore_except.value)
            == 'dataframe_type : None not supported. Possible values are "default", "pandas", "polars", "numpy" or "python"'
        )

    def test_jdbc_read_options(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch(
            "hsfs.core.util_sql.create_mysql_engine"
        )
        mocker.patch("hopsworks_common.client.get_instance")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=query,
            connector=None,
            dataframe_type="default",
            read_options={"external": ""},
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_read_none_data_format(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.read(
                storage_connector=None,
                data_format=None,
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_empty_data_format(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.read(
                storage_connector=None,
                data_format="",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_hopsfs_connector(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_hopsfs_connector_empty_dataframe(self, mocker):
        # Arrange

        # Setting list of empty dataframes as return value from _read_hopsfs
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs",
            return_value=[pd.DataFrame(), pd.DataFrame()],
        )

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        dataframe = python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert isinstance(dataframe, pd.DataFrame)
        assert len(dataframe) == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_hopsfs_connector_empty_dataframe_polars(self, mocker):
        # Arrange

        # Setting empty list as return value from _read_hopsfs
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs",
            return_value=[pl.DataFrame(), pl.DataFrame()],
        )

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        dataframe = python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="polars",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert isinstance(dataframe, pl.DataFrame)
        assert len(dataframe) == 0

    def test_read_s3_connector(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 0
        assert mock_python_engine_read_s3.call_count == 1

    def test_read_other_connector(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.JdbcConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.read(
                storage_connector=connector,
                data_format="csv",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert (
            str(e_info.value)
            == "JDBC Storage Connectors for training datasets are not supported yet for external environments."
        )
        assert mock_python_engine_read_hopsfs.call_count == 0
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_pandas_csv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_pandas(data_format="csv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_pandas_tsv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_pandas(data_format="tsv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_pandas_parquet(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        mock_obj = mocker.Mock()
        mock_obj.read.return_value = bytes()

        # Act
        python_engine._read_pandas(data_format="parquet", obj=mock_obj)

        # Assert
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 1

    def test_read_pandas_other(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._read_pandas(data_format="ocr", obj=None)

        # Assert
        assert (
            str(e_info.value)
            == "ocr training dataset format is not supported to read as pandas dataframe."
        )
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_csv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_polars(data_format="csv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_tsv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_polars(data_format="tsv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_parquet(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        mock_obj = mocker.Mock()
        mock_obj.read.return_value = bytes()

        # Act
        python_engine._read_polars(data_format="parquet", obj=mock_obj)

        # Assert
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 1

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_other(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._read_polars(data_format="ocr", obj=None)

        # Assert
        assert (
            str(e_info.value)
            == "ocr training dataset format is not supported to read as polars dataframe."
        )
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_hopsfs(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_remote.call_count == 1

    def test_read_hopsfs_remote(self, mocker):
        # Arrange
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        i = inode.Inode(attributes={"path": "test_path"})

        mock_dataset_api.return_value.list_files.return_value = (0, [i, i, i])
        mock_dataset_api.return_value.read_content.return_value.content = bytes()

        # Act
        python_engine._read_hopsfs_remote(location=None, data_format=None)

        # Assert
        assert mock_dataset_api.return_value.list_files.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 3

    def test_read_s3(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        mock_boto3_client.return_value.list_objects_v2.return_value = {
            "is_truncated": False,
            "Contents": [
                {"Key": "test", "Size": 1, "Body": ""},
                {"Key": "test1", "Size": 1, "Body": ""},
            ],
        }

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" not in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_s3_session_token(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, session_token="test_token"
        )

        mock_boto3_client.return_value.list_objects_v2.return_value = {
            "is_truncated": False,
            "Contents": [
                {"Key": "test", "Size": 1, "Body": ""},
                {"Key": "test1", "Size": 1, "Body": ""},
            ],
        }

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_s3_next_continuation_token(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        mock_boto3_client.return_value.list_objects_v2.side_effect = [
            {
                "is_truncated": True,
                "NextContinuationToken": "test_token",
                "Contents": [
                    {"Key": "test", "Size": 1, "Body": ""},
                    {"Key": "test1", "Size": 1, "Body": ""},
                ],
            },
            {
                "is_truncated": False,
                "Contents": [
                    {"Key": "test2", "Size": 1, "Body": ""},
                    {"Key": "test3", "Size": 1, "Body": ""},
                ],
            },
        ]

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" not in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 4

    def test_read_options(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.read_options(data_format=None, provided_options=None)

        # Assert
        assert result == {}

    def test_read_options_stream_source(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.read_stream(
                storage_connector=None,
                message_format=None,
                schema=None,
                options=None,
                include_metadata=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Streaming Sources are not supported for pure Python Environments."
        )

    def test_show(self, mocker):
        # Arrange
        mock_python_engine_sql = mocker.patch("hsfs.engine.python.Engine.sql")

        python_engine = python.Engine()

        # Act
        python_engine.show(sql_query=None, feature_store=None, n=None, online_conn=None)

        # Assert
        assert mock_python_engine_sql.call_count == 1

    def test_cast_columns(self, mocker):
        python_engine = python.Engine()
        d = {
            "string": ["s", "s"],
            "bigint": [1, 2],
            "int": [1, 2],
            "smallint": [1, 2],
            "tinyint": [1, 2],
            "float": [1.0, 2.2],
            "double": [1.0, 2.2],
            "timestamp": [1641340800000, 1641340800000],
            "boolean": ["False", None],
            "date": ["2022-01-27", "2022-01-28"],
            "binary": [b"1", b"2"],
            "array<string>": ["['123']", "['1234']"],
            "struc": ["{'label':'blue','index':45}", "{'label':'blue','index':46}"],
            "decimal": ["1.1", "1.2"],
        }
        df = pd.DataFrame(data=d)
        schema = [
            TrainingDatasetFeature("string", type="string"),
            TrainingDatasetFeature("bigint", type="bigint"),
            TrainingDatasetFeature("int", type="int"),
            TrainingDatasetFeature("smallint", type="smallint"),
            TrainingDatasetFeature("tinyint", type="tinyint"),
            TrainingDatasetFeature("float", type="float"),
            TrainingDatasetFeature("double", type="double"),
            TrainingDatasetFeature("timestamp", type="timestamp"),
            TrainingDatasetFeature("boolean", type="boolean"),
            TrainingDatasetFeature("date", type="date"),
            TrainingDatasetFeature("binary", type="binary"),
            TrainingDatasetFeature("array<string>", type="array<string>"),
            TrainingDatasetFeature("struc", type="struct<label:string,index:int>"),
            TrainingDatasetFeature("decimal", type="decimal"),
        ]
        cast_df = python_engine.cast_columns(df, schema)
        arrow_schema = pa.Schema.from_pandas(cast_df)
        expected = {
            "string": object,
            "bigint": pd.Int64Dtype(),
            "int": pd.Int32Dtype(),
            "smallint": pd.Int16Dtype(),
            "tinyint": pd.Int8Dtype(),
            "float": np.dtype("float32"),
            "double": np.dtype("float64"),
            "timestamp": np.dtype("datetime64[ns]"),
            "boolean": object,
            "date": np.dtype(date),
            "binary": object,
            "array<string>": object,
            "struc": object,
            "decimal": np.dtype(decimal.Decimal),
        }
        assert pa.types.is_string(arrow_schema.field("string").type)
        assert pa.types.is_boolean(arrow_schema.field("boolean").type)
        assert pa.types.is_binary(arrow_schema.field("binary").type)
        assert pa.types.is_list(arrow_schema.field("array<string>").type)
        assert pa.types.is_struct(arrow_schema.field("struc").type)
        for col in cast_df.columns:
            assert cast_df[col].dtype == expected[col]

    def test_register_external_temporary_table(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.register_external_temporary_table(
            external_fg=None, alias=None
        )

        # Assert
        assert result is None

    def test_register_hudi_temporary_table(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.register_hudi_temporary_table(
            hudi_fg_alias=None,
            feature_store_id=None,
            feature_store_name=None,
            read_options=None,
        )

        # Assert
        assert result is None

    def test_register_hudi_temporary_table_time_travel(self):
        # Arrange
        python_engine = python.Engine()
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )
        q = HudiFeatureGroupAlias(
            fg.to_dict(),
            "fg",
            left_feature_group_end_timestamp="20220101",
            left_feature_group_start_timestamp="20220101",
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.register_hudi_temporary_table(
                hudi_fg_alias=q,
                feature_store_id=None,
                feature_store_name=None,
                read_options=None,
            )

        # Assert
        assert str(e_info.value) == (
            "Incremental queries are not supported in the python client."
            + " Read feature group without timestamp to retrieve latest snapshot or switch to "
            + "environment with Spark Engine."
        )

    def test_register_hudi_temporary_table_time_travel_sub_query(self):
        # Arrange
        python_engine = python.Engine()
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )
        q = HudiFeatureGroupAlias(
            fg.to_dict(),
            "fg",
            left_feature_group_end_timestamp="20220101",
            left_feature_group_start_timestamp="20220101",
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.register_hudi_temporary_table(
                hudi_fg_alias=q,
                feature_store_id=None,
                feature_store_name=None,
                read_options=None,
            )

        # Assert
        assert str(e_info.value) == (
            "Incremental queries are not supported in the python client."
            + " Read feature group without timestamp to retrieve latest snapshot or switch to "
            + "environment with Spark Engine."
        )

    def test_profile_pandas(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_pandas_with_null_column(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, None], "col3": [None, None]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_profile_polars(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_profile_polars_with_null_column(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, None], "col3": [None, None]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_relevant_columns(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.return_value = {
            "dataType": "Integral",
            "test_key": "test_value",
        }

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=["col1"],
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 1

    def test_profile_relevant_columns_diff_dtypes(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=["col1", "col3"],
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 2

    def test_convert_pandas_statistics(self):
        # Arrange
        python_engine = python.Engine()

        stat = {
            "25%": 25,
            "50%": 50,
            "75%": 75,
            "mean": 50,
            "count": 100,
            "max": 10,
            "std": 33,
            "min": 1,
        }

        # Act
        result = python_engine._convert_pandas_statistics(stat=stat, dataType="Integer")

        # Assert
        assert result == {
            "dataType": "Integer",
            "approxPercentiles": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                25,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                50,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                75,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            "maximum": 10,
            "mean": 50,
            "minimum": 1,
            "stdDev": 33,
            "sum": 5000,
            "count": 100,
        }

    def test_validate(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.validate(dataframe=None, expectations=None, log_activity=True)

        # Assert
        assert (
            str(e_info.value)
            == "Deequ data validation is only available with Spark Engine. Use validate_with_great_expectations"
        )

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_validate_with_great_expectations(self, mocker):
        # Arrange
        mock_ge_from_pandas = mocker.patch("great_expectations.from_pandas")

        python_engine = python.Engine()

        # Act
        python_engine.validate_with_great_expectations(
            dataframe=None, expectation_suite=None, ge_validate_kwargs={}
        )

        # Assert
        assert mock_ge_from_pandas.call_count == 1

    @pytest.mark.skipif(
        HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is installed.",
    )
    def test_validate_with_great_expectations_raise_module_not_found(self):
        # Arrange
        python_engine = python.Engine()
        suite = ExpectationSuite(
            expectation_suite_name="test_suite",
            expectations=[],
            meta={},
            run_validation=True,
        )

        # Act
        with pytest.raises(ModuleNotFoundError):
            python_engine.validate_with_great_expectations(
                dataframe=None, expectation_suite=suite, ge_validate_kwargs={}
            )

    def test_set_job_group(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.set_job_group(group_id=None, description=None)

        # Assert
        assert result is None

    def test_convert_to_default_dataframe_pandas(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains upper case letters in feature names: `['Col1']`. Feature names are sanitized to lower case in the feature store."
        )

    def test_convert_to_default_dataframe_pandas_with_spaces(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        d = {"col 1": [1, 2], "co 2 co": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        assert result.columns.values.tolist() == ["col_1", "co_2_co"]
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains feature names with spaces: `['col 1', 'co 2 co']`. "
            "Feature names are sanitized to use underscore '_' in the feature store."
        )

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_convert_to_default_dataframe_polars(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        df = pl.DataFrame(
            [
                pl.Series("Col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "Date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone="Africa/Abidjan"),
                ),
            ]
        )

        # Resulting dataframe
        expected_converted_df = pl.DataFrame(
            [
                pl.Series("col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone=None),
                ),
            ]
        )

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        polars_assert_frame_equal(result, expected_converted_df)
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains upper case letters in feature names: `['Col1', 'Date']`. Feature names are sanitized to lower case in the feature store."
        )

    def test_parse_schema_feature_group_pandas(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.type_systems.convert_pandas_dtype_to_offline_type")

        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.parse_schema_feature_group(
            dataframe=df, time_travel_format=None
        )

        # Assert
        assert len(result) == 2
        assert result[0].name == "col1"
        assert result[1].name == "col2"

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_parse_schema_feature_group_polars(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.type_systems.convert_pandas_dtype_to_offline_type")

        python_engine = python.Engine()

        df = pl.DataFrame(
            [
                pl.Series("col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone="Africa/Abidjan"),
                ),
            ]
        )

        # Act
        result = python_engine.parse_schema_feature_group(
            dataframe=df, time_travel_format=None
        )

        # Assert
        assert len(result) == 3
        assert result[0].name == "col1"
        assert result[1].name == "col2"
        assert result[2].name == "date"

    def test_parse_schema_training_dataset(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.parse_schema_training_dataset(dataframe=None)

        # Assert
        assert (
            str(e_info.value)
            == "Training dataset creation from Dataframes is not supported in Python environment. Use HSFS Query object instead."
        )

    def test_save_dataframe(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_python_engine_write_dataframe_kafka.call_count == 0
        assert mock_python_engine_legacy_save_dataframe.call_count == 1

    def test_save_dataframe_stream(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=True,
        )

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_python_engine_write_dataframe_kafka.call_count == 1
        assert mock_python_engine_legacy_save_dataframe.call_count == 0

    def test_legacy_save_dataframe(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_get_url = mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_api.return_value._start.return_value = (
            hopsworks_common.execution.Execution(job=mocker.Mock())
        )
        mocker.patch("hsfs.engine.python.Engine._get_app_options")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")

        python_engine = python.Engine()

        mock_fg_api.return_value.ingestion.return_value.job = job.Job(
            1, "test_job", None, None, None, None
        )

        # Act
        python_engine.legacy_save_dataframe(
            feature_group=mocker.Mock(),
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options={},
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_fg_api.return_value.ingestion.call_count == 1
        assert mock_dataset_api.return_value.upload_feature_group.call_count == 1
        assert mock_execution_api.return_value._start.call_count == 1
        assert mock_get_url.call_count == 1

    def test_get_training_data(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_python_engine_prepare_transform_split_df = mocker.patch(
            "hsfs.engine.python.Engine._prepare_transform_split_df"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.get_training_data(
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            query_obj=mocker.Mock(),
            read_options=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 0

    def test_get_training_data_splits(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_python_engine_prepare_transform_split_df = mocker.patch(
            "hsfs.engine.python.Engine._prepare_transform_split_df"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.get_training_data(
            training_dataset_obj=td,
            feature_view_obj=None,
            query_obj=mocker.Mock(),
            read_options=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 1

    def test_split_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels=None
        )

        # Assert
        assert str(result_df) == "   Col1  col2\n0     1     3\n1     2     4"
        assert str(result_df_split) == "None"

    def test_split_labels_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels=None
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert result_df_split is None

    def test_split_labels_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="pandas", labels=None
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert result_df_split is None

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_split_labels_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pl.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="polars", labels=None
        )

        # Assert
        assert isinstance(result_df, pl.DataFrame) or isinstance(
            result_df, pl.dataframe.frame.DataFrame
        )
        assert result_df_split is None

    def test_split_labels_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pd.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="python", labels=None
        )

        # Assert
        assert isinstance(result_df, list)
        assert result_df_split is None

    def test_split_labels_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pd.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="numpy", labels=None
        )

        # Assert
        assert isinstance(result_df, np.ndarray)
        assert result_df_split is None

    def test_split_labels_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels="col1"
        )

        # Assert
        assert str(result_df) == "   col2\n0     3\n1     4"
        assert str(result_df_split) == "0    1\n1    2\nName: col1, dtype: int64"

    def test_split_labels_labels_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(result_df_split, pd.Series)

    def test_split_labels_labels_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="pandas", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(result_df_split, pd.Series)

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_split_labels_labels_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pl.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="polars", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pl.DataFrame) or isinstance(
            result_df, pl.dataframe.frame.DataFrame
        )
        assert isinstance(result_df_split, pl.Series)

    def test_split_labels_labels_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="python", labels="col1"
        )

        # Assert
        assert isinstance(result_df, list)
        assert isinstance(result_df_split, list)

    def test_split_labels_labels_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="numpy", labels="col1"
        )

        # Assert
        assert isinstance(result_df, np.ndarray)
        assert isinstance(result_df_split, np.ndarray)

    def test_prepare_transform_split_df_random_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_random_split = mocker.patch(
            "hsfs.engine.python.Engine._random_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        mock_python_engine_random_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_random_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_td_features(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
            features=[f, f1, f2],
        )

        q = query.Query(left_feature_group=fg, left_features=[])

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_query_features(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_query_features_fully_qualified_name(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {
            "col1": [1, 2],
            "col2": [3, 4],
            "test_fs_test_1_event_time": [1000000000, 2000000000],
        }
        df = pd.DataFrame(data=d)

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
            featurestore_name="test_fs",
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(
            name="event_time", type="str", use_fully_qualified_name=True
        )

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_random_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["test_split1", "test_split2"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_size_precision_1(self, mocker):
        # In python sum([0.6, 0.3, 0.1]) != 1.0 due to floating point precision.
        # This test checks if different split ratios can be handled.
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2] * 10, "col2": [3, 4] * 10}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.6, "validation": 0.3, "test": 0.1},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["train", "validation", "test"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_size_precision_2(self, mocker):
        # This test checks if the method can handle split ratio with high precision.
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2] * 10, "col2": [3, 4] * 10}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 1 / 3, "validation": 1 - 1 / 3 - 0.1, "test": 0.1},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["train", "validation", "test"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_bad_percentage(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.2},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert (
            str(e_info.value)
            == "Sum of split ratios should be 1 and each values should be in range (0, 1)"
        )

    def test_time_series_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [], "col2": [], "event_time": []}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_time_series_split_drop_event_time(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [], "col2": [], "event_time": []}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}
        expected["train"] = expected["train"].drop(["event_time"], axis=1)
        expected["test"] = expected["test"].drop(["event_time"], axis=1)

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=True,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_time_series_split_event_time(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_convert_to_unix_timestamp_pandas(self):
        # Act
        result = util.convert_event_time_to_timestamp(
            event_time=pd.Timestamp("2017-01-01")
        )

        # Assert
        assert result == 1483228800000.0

    def test_convert_to_unix_timestamp_str(self, mocker):
        # Arrange
        mock_util_get_timestamp_from_date_string = mocker.patch(
            "hopsworks_common.util.get_timestamp_from_date_string"
        )

        mock_util_get_timestamp_from_date_string.return_value = 1483225200000

        # Act
        result = util.convert_event_time_to_timestamp(
            event_time="2017-01-01 00-00-00-000"
        )

        # Assert
        assert result == 1483225200000

    def test_convert_to_unix_timestamp_int(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=1483225200)

        # Assert
        assert result == 1483225200000

    def test_convert_to_unix_timestamp_datetime(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=datetime(2022, 9, 18))

        # Assert
        assert result == 1663459200000

    def test_convert_to_unix_timestamp_date(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=date(2022, 9, 18))

        # Assert
        assert result == 1663459200000

    def test_convert_to_unix_timestamp_pandas_datetime(self):
        # Act
        result = util.convert_event_time_to_timestamp(
            event_time=pd.Timestamp("2022-09-18")
        )

        # Assert
        assert result == 1663459200000

    def test_write_training_dataset(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.util.get_job_url")
        mock_python_engine_wait_for_job = mocker.patch(
            "hopsworks_common.engine.execution_engine.ExecutionEngine.wait_until_finished"
        )

        python_engine = python.Engine()

        # Act
        with pytest.raises(Exception) as e_info:
            python_engine.write_training_dataset(
                training_dataset=None,
                dataset=None,
                user_write_options={},
                save_mode=None,
                feature_view_obj=None,
                to_df=False,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Currently only query based training datasets are supported by the Python engine"
        )
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 0
        assert mock_td_api.return_value.compute.call_count == 0
        assert mock_python_engine_wait_for_job.call_count == 0

    def test_write_training_dataset_query_td(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_job = mocker.patch("hsfs.core.job.Job")

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mock_td_api.return_value.compute.return_value = mock_job
        mocker.patch("hsfs.util.get_job_url")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(fg, fg.features)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.write_training_dataset(
            training_dataset=td,
            dataset=q,
            user_write_options={},
            save_mode=None,
            feature_view_obj=None,
            to_df=False,
        )

        # Assert
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 0
        assert mock_td_api.return_value.compute.call_count == 1
        assert mock_job._wait_for_job.call_count == 1

    def test_write_training_dataset_query_fv(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_job = mocker.patch("hsfs.core.job.Job")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_api.return_value.compute_training_dataset.return_value = mock_job

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.util.get_job_url")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(fg, fg.features)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=q,
            featurestore_id=99,
            labels=[],
        )

        # Act
        python_engine.write_training_dataset(
            training_dataset=td,
            dataset=q,
            user_write_options={},
            save_mode=None,
            feature_view_obj=fv,
            to_df=False,
        )

        # Assert
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 1
        assert mock_td_api.return_value.compute.call_count == 0
        assert mock_job._wait_for_job.call_count == 1

    def test_return_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="default"
        )

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"

    def test_return_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="pandas"
        )

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_return_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="pandas"
        )

        # Assert
        assert isinstance(result, pl.DataFrame) or isinstance(
            result, pl.dataframe.frame.DataFrame
        )
        assert df.equals(result)

    def test_return_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="numpy"
        )

        # Assert
        assert str(result) == "[[1 3]\n [2 4]]"

    def test_return_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="python"
        )

        # Assert
        assert result == [[1, 3], [2, 4]]

    def test_return_dataframe_type_other(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._return_dataframe_type(dataframe=df, dataframe_type="other")

        # Assert
        assert (
            str(e_info.value)
            == "Dataframe type `other` not supported on this platform."
        )

    def test_is_spark_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.is_spark_dataframe(dataframe=None)

        # Assert
        assert result is False

    def test_save_stream_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.save_stream_dataframe(
                feature_group=None,
                dataframe=None,
                query_name=None,
                output_mode=None,
                await_termination=None,
                timeout=None,
                write_options=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Stream ingestion is not available on Python environments, because it requires Spark as engine."
        )

    def test_update_table_schema(self, mocker):
        # Arrange
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        python_engine = python.Engine()

        mock_fg_api.return_value.update_table_schema.return_value.job = job.Job(
            1, "test_job", None, None, None, None
        )

        # Act
        result = python_engine.update_table_schema(feature_group=None)

        # Assert
        assert result is None
        assert mock_fg_api.return_value.update_table_schema.call_count == 1

    def test_get_app_options(self, mocker):
        # Arrange
        mock_ingestion_job_conf = mocker.patch(
            "hsfs.core.ingestion_job_conf.IngestionJobConf"
        )

        python_engine = python.Engine()

        # Act
        python_engine._get_app_options(user_write_options={"spark": 1, "test": 2})

        # Assert
        assert mock_ingestion_job_conf.call_count == 1
        assert mock_ingestion_job_conf.call_args[1]["write_options"] == {"test": 2}

    def test_add_file(self):
        # Arrange
        python_engine = python.Engine()

        file = None

        # Act
        result = python_engine.add_file(file=file)

        # Assert
        assert result == file

    def test_apply_transformation_function_udf_default_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_apply_transformation_function_udf_pandas_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="pandas")
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_apply_transformation_function_udf_python_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="python")
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.parametrize("execution_mode", ["default", "pandas", "python"])
    def test_apply_transformation_function_udf_transformation_context(
        self, mocker, execution_mode
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode=execution_mode)
        def plus_one(col1, context):
            return col1 + context["test"]

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=df,
            transformation_context={"test": 10},
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 11
        assert result["plus_one_tf_name_"][1] == 12

    def test_apply_transformation_function_multiple_output_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"])
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_output_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="python")
        def plus_two(col1):
            return col1 + 1, col1 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_output_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="pandas")
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_input_output_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], mode="pandas")
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], mode="python")
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1", "col2"])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1", "col2"], mode="python")
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1", "col2"], mode="pandas")
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="python")
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="pandas")
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_default_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_python_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="python")
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_pandas_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="pandas")
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_get_unique_values(self):
        # Arrange
        python_engine = python.Engine()

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        result = python_engine.get_unique_values(
            feature_dataframe=df, feature_name="col1"
        )

        # Assert
        assert len(result) == 3
        assert 1 in result
        assert 2 in result
        assert 3 in result

    def test_apply_transformation_function_missing_feature_on_demand_transformations(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            transformation_functions=[add_one("missing_col1")],
            id=11,
            stream=False,
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as exception:
            python_engine._apply_transformation_function(
                transformation_functions=fg.transformation_functions, dataset=df
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, specified in the on-demand transformation function 'add_one' are not present in the dataframe being inserted into the feature group. "
            "Please verify that the correct feature names are used in the transformation function and that these features exist in the dataframe being inserted."
        )

    def test_apply_transformation_function_missing_feature_model_dependent_transformations(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[add_one("missing_col1")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as exception:
            python_engine._apply_transformation_function(
                transformation_functions=fv.transformation_functions, dataset=df
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, specified in the model-dependent transformation function 'add_one' are not present in the feature view. "
            "Please verify that the correct features are specified in the transformation function."
        )

    def test_materialization_kafka(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            return_value=" tests_offsets",
        )
        mocker.patch(
            "hsfs.core.job_api.JobApi.last_execution",
            return_value=["", ""],
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults",
            await_termination=False,
        )

    def test_materialization_kafka_first_job_execution(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            return_value="tests_offsets",
        )
        mocker.patch(
            "hsfs.core.job_api.JobApi.last_execution",
            return_value=[],
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults -initialCheckPointString tests_offsets",
            await_termination=False,
        )

    def test_materialization_kafka_skip_offsets(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            return_value="tests_offsets",
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={
                "start_offline_materialization": True,
                "skip_offsets": True,
            },
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults -initialCheckPointString tests_offsets",
            await_termination=False,
        )

    def test_materialization_kafka_topic_doesnt_exist(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            side_effect=["", "tests_offsets"],
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults -initialCheckPointString tests_offsets",
            await_termination=False,
        )

    def test_test(self, mocker):
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "topic_name"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        assert fg.materialization_job.config == {"defaultArgs": "defaults"}
