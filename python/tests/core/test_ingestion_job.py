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


from hsfs.core import ingestion_job, job


class TestIngestionJob:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["ingestion_job"]["get"]["response"]

        # Act
        mocker.patch("hopsworks_common.client.get_instance")
        ij = ingestion_job.IngestionJob.from_response_json(json)

        # Assert
        assert ij.data_path == "test_data_path"
        assert isinstance(ij.job, job.Job)
