#
#   Copyright 2024 Hopsworks AB
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
import pytest
from hsfs.core import monitoring_window_config as mwc


class TestMonitoringWindowConfig:
    def test_window_based_on_training_dataset_version(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.TRAINING_DATASET,
            training_dataset_version=1,
        )

        # Act
        # forbidden update for training dataset
        with pytest.raises(AttributeError):
            window_config.time_offset = "1d"
        with pytest.raises(AttributeError):
            window_config.window_length = "1h"
        with pytest.raises(AttributeError):
            window_config.row_percentage = 0.2

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.TRAINING_DATASET
        assert window_config.training_dataset_version == 1
        assert window_config.time_offset is None
        assert window_config.window_length is None
        # training dataset statistics are always computed on the whole dataset
        assert window_config.row_percentage == 1.0

    def test_window_based_on_rolling_time(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1d",
            window_length="1h",
            row_percentage=0.2,
        )

        # Act
        # forbidden update for rolling time
        with pytest.raises(AttributeError):
            window_config.training_dataset_version = 1

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.ROLLING_TIME
        assert window_config.training_dataset_version is None
        assert window_config.time_offset == "1d"
        assert window_config.window_length == "1h"
        assert window_config.row_percentage == 0.2

    def test_window_based_on_all_time(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ALL_TIME,
            row_percentage=0.2,
        )

        # Act
        # forbidden update for all time
        with pytest.raises(AttributeError):
            window_config.training_dataset_version = 1
        with pytest.raises(AttributeError):
            window_config.time_offset = "1d"
        with pytest.raises(AttributeError):
            window_config.window_length = "1h"

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.ALL_TIME
        assert window_config.training_dataset_version is None
        assert window_config.time_offset is None
        assert window_config.window_length is None
        assert window_config.row_percentage == 0.2

    def test_window_config_type_list_str(self):
        # Arrange
        window_config_type_list = mwc.WindowConfigType._list_str()

        # Assert
        assert set(window_config_type_list) == {
            "ALL_TIME",
            "ROLLING_TIME",
            "TRAINING_DATASET",
            "EVENT_TIME_RANGE",
        }

    def test_window_based_on_event_time_range(self):
        # Arrange
        window_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.EVENT_TIME_RANGE,
            start_event_time="2024-01-01 00:00:00",
            end_event_time="2024-01-01 01:00:00",
            row_percentage=0.5,
        )

        # Act
        the_dict = window_config.to_dict()

        # Assert
        assert window_config.window_config_type == mwc.WindowConfigType.EVENT_TIME_RANGE
        assert window_config.start_event_time == 1704067200000
        assert window_config.end_event_time == 1704070800000
        assert window_config.row_percentage == 0.5
        assert window_config.time_offset is None
        assert window_config.window_length is None
        assert the_dict["windowConfigType"] == mwc.WindowConfigType.EVENT_TIME_RANGE
        assert the_dict["startEventTime"] == 1704067200000
        assert the_dict["endEventTime"] == 1704070800000
        assert the_dict["rowPercentage"] == 0.5
        assert "timeOffset" not in the_dict

    def test_event_time_range_round_trips_through_response_json(self):
        # Arrange
        response = {
            "id": 7,
            "windowConfigType": "EVENT_TIME_RANGE",
            "startEventTime": 1704067200000,
            "endEventTime": 1704070800000,
            "rowPercentage": 1.0,
        }

        # Act
        window_config = mwc.MonitoringWindowConfig.from_response_json(response)

        # Assert
        assert window_config.id == 7
        assert window_config.window_config_type == mwc.WindowConfigType.EVENT_TIME_RANGE
        assert window_config.start_event_time == 1704067200000
        assert window_config.end_event_time == 1704070800000
        assert window_config.row_percentage == 1.0

    def test_event_time_range_requires_ordered_bounds(self):
        with pytest.raises(
            ValueError, match="both start_event_time and end_event_time"
        ):
            mwc.MonitoringWindowConfig(
                window_config_type=mwc.WindowConfigType.EVENT_TIME_RANGE,
                start_event_time=1704067200000,
            )
        with pytest.raises(ValueError, match="after start_event_time"):
            mwc.MonitoringWindowConfig(
                window_config_type=mwc.WindowConfigType.EVENT_TIME_RANGE,
                start_event_time=1704070800000,
                end_event_time=1704067200000,
            )

    def test_event_time_bounds_rejected_on_other_window_types(self):
        with pytest.raises(ValueError, match="only be set for EVENT_TIME_RANGE"):
            mwc.MonitoringWindowConfig(
                window_config_type=mwc.WindowConfigType.ROLLING_TIME,
                time_offset="1d",
                start_event_time=1704067200000,
                end_event_time=1704070800000,
            )
