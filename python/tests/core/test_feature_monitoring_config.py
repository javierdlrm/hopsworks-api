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

from datetime import datetime, timezone

from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.feature_monitoring_config import FeatureMonitoringType
from hsfs.core.job_schedule import JobSchedule
from hsfs.core.monitoring_window_config import WindowConfigType


class TestFeatureMonitoringConfig:

    # via feature group

    def test_from_response_json_scheduled_stats_det_rolling_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["scheduled_stats_detection_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_rolling(config, fg_id=13)

    def test_from_response_json_scheduled_stats_det_all_time_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["scheduled_stats_detection_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_all_time(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_and_spec_value_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_rolling_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling_and_spec_value(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_and_spec_value_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_all_time_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time_and_spec_value(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_spec_value_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_specific_value"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_spec_value(config, fg_id=13)

    # via feature view

    def test_from_response_json_scheduled_stats_det_rolling_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["scheduled_stats_detection_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_rolling(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_scheduled_stats_det_all_time_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["scheduled_stats_detection_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_all_time(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_td_version_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_training_dataset"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_td_version(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_all_time_ref_td_version_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_all_time_reference_training_dataset"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_all_time_ref_td_version(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_rolling_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_all_time_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_td_version_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ][
            "stats_comparison_detection_rolling_reference_training_dataset_and_specific_value"
        ][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_td_version_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_specific_value"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_all_time_ref_td_version_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ][
            "stats_comparison_detection_all_time_reference_training_dataset_and_specific_value"
        ][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_all_time_ref_td_version_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_all_time_ref_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_all_time_reference_specific_value"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_all_time_ref_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    # assert utils

    def assert_scheduled_stats_det_rolling(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.SCHEDULED_STATISTICS,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

    def assert_scheduled_stats_det_all_time(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.SCHEDULED_STATISTICS,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

    def assert_stats_comp_det_rolling_ref_rolling(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_rolling(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_rolling_ref_all_time(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_all_time(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_rolling_ref_td_version(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_all_time_ref_td_version(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_rolling_ref_rolling_and_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_rolling(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_rolling_ref_all_time_and_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_all_time(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_rolling_ref_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_ref_stats=False, with_spec_value=True)

    def assert_stats_comp_det_rolling_ref_td_version_and_spec_value(
        self, config, fv_name, fv_version
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_all_time_ref_td_version_and_spec_value(
        self, config, fv_name, fv_version
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_all_time_ref_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_ref_stats=False, with_spec_value=True)

    # -- assert fm config

    def assert_fm_config(
        self, config, type=None, fg_id=None, fv_name=None, fv_version=None
    ):
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_group_id == fg_id
        assert config._feature_view_name == fv_name
        assert config._feature_view_version == fv_version
        assert config._href[-2:] == "32"
        assert config._name == "unit_test_config"
        assert config.enabled is True
        assert config._feature_monitoring_type == type
        assert (
            config.job_name
            == "fg_or_fv_name_version_fm_config_name_run_feature_monitoring"
        )

        assert isinstance(config._job_schedule, JobSchedule)
        assert config._job_schedule.id == 222
        assert config._job_schedule.cron_expression == "0 0 * ? * * *"
        assert config._job_schedule.enabled is True
        assert config._job_schedule.start_date_time == datetime.fromtimestamp(
            1676457000000 / 1000, tz=timezone.utc
        )

    # -- assert windows

    def assert_det_rolling(self, config):
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._detection_window_config.training_dataset_version is None

    def assert_det_all_time(self, config):
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ALL_TIME
        )
        assert config._detection_window_config.time_offset is None
        assert config._detection_window_config.window_length is None
        assert config._detection_window_config.training_dataset_version is None

    def assert_ref_rolling(self, config):
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._reference_window_config.time_offset == "1w"
        assert config._reference_window_config.window_length == "1d"
        assert config._detection_window_config.training_dataset_version is None

    def assert_ref_all_time(self, config):
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.ALL_TIME
        )
        assert config._reference_window_config.time_offset is None
        assert config._reference_window_config.window_length is None
        assert config._detection_window_config.training_dataset_version is None

    def assert_ref_td_version(self, config):
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.TRAINING_DATASET
        )
        assert config._reference_window_config.time_offset is None
        assert config._reference_window_config.window_length is None
        assert config._reference_window_config.training_dataset_version == 33

    # -- assert feat stats and stats configs

    def assert_feature_stats(self, config):
        assert len(config._feature_statistics_configs) == 1

        fsc = config._feature_statistics_configs[0]
        assert fsc.id == 332
        assert fsc.feature_name == "monitored_feature"

        assert (
            len(config.get_feature_names()) == 1
            and "monitored_feature" in config.get_feature_names()
        )

    def assert_stats_configs(self, config, with_ref_stats=True, with_spec_value=False):
        feat_stats_configs = config._feature_statistics_configs

        stats_configs = feat_stats_configs[0].statistics_comparison_configs
        assert len(stats_configs) == 2 if with_ref_stats and with_spec_value else 1

        if with_ref_stats:
            self.assert_stats_config(stats_configs[0])
            if not with_spec_value:
                assert len(stats_configs) == 1
            else:
                assert len(stats_configs) == 2
                self.assert_stats_config(stats_configs[1], spec_value=6.6)
        elif with_spec_value:
            assert len(stats_configs) == 1
            self.assert_stats_config(stats_configs[0], spec_value=6.6)

    def assert_stats_config(self, stats_config, spec_value=None):
        assert stats_config.threshold == 1
        assert stats_config.strict is True
        assert stats_config.relative is False
        assert stats_config.metric == "MEAN"
        assert stats_config.specific_value == spec_value
