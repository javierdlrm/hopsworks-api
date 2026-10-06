#
#   Copyright 2026 Hopsworks AB
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

import json

import pytest
from hsml.deployment_candidate import DeploymentCandidate


class TestDeploymentCandidate:
    def test_from_response_json(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_candidate"][
            "get_candidate_running_test"
        ]["response"]

        candidate = DeploymentCandidate.from_response_json(json_dict)

        assert candidate.version == 3
        assert candidate.traffic_percentage == 20
        assert candidate.state == "RUNNING_TEST"
        assert candidate.status == "Running"
        assert candidate.condition.type == "READY"
        assert candidate.condition.status is True
        assert candidate.condition.reason == "Candidate is ready"
        assert candidate.available_predictor_instances == 2
        assert candidate.requested_predictor_instances == 2
        assert candidate.available_transformer_instances == 1
        assert candidate.requested_transformer_instances == 1
        assert candidate.revision == 7

    def test_minimal_json_and_unknown_keys(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_candidate"]["get_candidate_minimal"][
            "response"
        ]

        candidate = DeploymentCandidate.from_response_json(
            {**json_dict, "somethingNew": 1}
        )

        assert candidate.version == 4
        assert candidate.traffic_percentage == 0
        assert candidate.condition is None
        assert candidate.status is None
        assert candidate.is_running() is False

    def test_to_dict_round_trips_the_wire_format(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_candidate"][
            "get_candidate_running_test"
        ]["response"]

        candidate = DeploymentCandidate.from_response_json(json_dict)

        assert candidate.to_dict() == json_dict
        assert json.loads(candidate.json()) == json_dict

    @pytest.mark.parametrize(
        ("status", "or_idle", "or_updating", "expected"),
        [
            ("Running", False, False, True),
            ("Idle", True, False, True),
            ("Idle", False, False, False),
            ("Updating", False, True, True),
            ("Updating", True, False, False),
            ("Failed", True, True, False),
            ("Starting", True, True, False),
        ],
    )
    def test_is_running(self, status, or_idle, or_updating, expected):
        candidate = DeploymentCandidate(version=1, status=status)

        assert candidate.is_running(or_idle, or_updating) is expected

    def test_describe_prints_json(self, backend_fixtures, capsys):
        json_dict = backend_fixtures["deployment_candidate"][
            "get_candidate_running_test"
        ]["response"]

        DeploymentCandidate.from_response_json(json_dict).describe()

        assert json.loads(capsys.readouterr().out)["traffic_percent"] == 20
