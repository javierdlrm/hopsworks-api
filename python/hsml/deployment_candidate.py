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

from __future__ import annotations

import json
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hsml.constants import PREDICTOR_STATE
from hsml.predictor_state_condition import PredictorStateCondition


@public
class DeploymentCandidate:
    """The candidate configuration served next to the live one of a deployment.

    A candidate takes a share of the deployment's traffic, so a new configuration can be compared with the live one on real requests.
    It is a snapshot of the state reported by the last response the deployment received, so it is not refreshed by itself.
    """

    STATE_RUNNING_TEST = "RUNNING_TEST"
    STATE_ROLLING_OUT = "ROLLING_OUT"

    def __init__(
        self,
        version: int,
        traffic_percent: int = 0,
        state: str | None = None,
        status: str | None = None,
        condition: PredictorStateCondition | dict | None = None,
        available_instances: int | None = None,
        requested_instances: int | None = None,
        available_transformer_instances: int | None = None,
        requested_transformer_instances: int | None = None,
        revision: int | None = None,
        **kwargs: Any,
    ) -> None:
        self._version = version
        self._traffic_percent = traffic_percent
        self._state = state
        self._status = status
        self._condition = util._get_obj_from_json(condition, PredictorStateCondition)
        self._available_predictor_instances = available_instances
        self._requested_predictor_instances = requested_instances
        self._available_transformer_instances = available_transformer_instances
        self._requested_transformer_instances = requested_transformer_instances
        self._revision = revision

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> DeploymentCandidate:
        return cls(**humps.decamelize(json_dict))

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self._version,
            "trafficPercent": self._traffic_percent,
            "state": self._state,
            "status": self._status,
            "condition": (
                self._condition.to_dict()["condition"] if self._condition else None
            ),
            "availableInstances": self._available_predictor_instances,
            "requestedInstances": self._requested_predictor_instances,
            "availableTransformerInstances": self._available_transformer_instances,
            "requestedTransformerInstances": self._requested_transformer_instances,
            "revision": self._revision,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    @public
    def describe(self) -> None:
        """Print a JSON description of the candidate."""
        util._pretty_print(self)

    @public
    def is_running(self, or_idle: bool = True, or_updating: bool = True) -> bool:
        """Check whether the candidate is ready to handle inference requests.

        Parameters:
            or_idle: Whether the idle status is considered as running.
            or_updating: Whether the updating status is considered as running.

        Returns:
            Whether the candidate is ready, according to the last response received.
        """
        return (
            self._status == PREDICTOR_STATE.STATUS_RUNNING
            or (or_idle and self._status == PREDICTOR_STATE.STATUS_IDLE)
            or (or_updating and self._status == PREDICTOR_STATE.STATUS_UPDATING)
        )

    @public
    @property
    def version(self) -> int:
        """Version number of the candidate configuration."""
        return self._version

    @public
    @property
    def traffic_percentage(self) -> int:
        """Share of the deployment's traffic, in percent, that is sent to the candidate."""
        return self._traffic_percent

    @public
    @property
    def state(self) -> str | None:
        """Phase of the candidate, `RUNNING_TEST` while it is tested and `ROLLING_OUT` while it replaces the live configuration."""
        return self._state

    @public
    @property
    def status(self) -> str | None:
        """Status of the candidate, with the same values as the status of the deployment."""
        return self._status

    @public
    @property
    def condition(self) -> PredictorStateCondition | None:
        """Condition of the current status of the candidate."""
        return self._condition

    @public
    @property
    def available_predictor_instances(self) -> int | None:
        """Available predictor instances of the candidate."""
        return self._available_predictor_instances

    @public
    @property
    def requested_predictor_instances(self) -> int | None:
        """Requested predictor instances of the candidate."""
        return self._requested_predictor_instances

    @public
    @property
    def available_transformer_instances(self) -> int | None:
        """Available transformer instances of the candidate."""
        return self._available_transformer_instances

    @public
    @property
    def requested_transformer_instances(self) -> int | None:
        """Requested transformer instances of the candidate."""
        return self._requested_transformer_instances

    @public
    @property
    def revision(self) -> int | None:
        """Revision of the candidate."""
        return self._revision

    def __repr__(self):
        return (
            f"DeploymentCandidate(version: {self._version!r}, "
            f"traffic_percentage: {self._traffic_percent!r}, state: {self._state!r}, "
            f"status: {self._status!r})"
        )
