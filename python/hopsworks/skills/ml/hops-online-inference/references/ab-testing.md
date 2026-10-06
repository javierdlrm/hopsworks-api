# A/B testing with a candidate

A candidate is a second configuration of a running deployment (new model version, resources, predictor) served next to the live version with a share of the requests.
It exists only on Standard-mode deployments (`knative_mode=False`) served by KServe.

```python
deployment.predictor.model_version = 2          # edit the deployment as usual
candidate = deployment.create_candidate(traffic_percentage=20)
# The edits became the candidate at the next version number; `deployment` describes the live version again.
deployment.update_candidate_traffic(50)         # 0 to 99
deployment.rollout_candidate()                  # promote: the candidate becomes the active version
deployment.delete_candidate()                   # or discard it
```

- `create_candidate` starts the candidate with no traffic and sets `traffic_percentage` once it runs; with `await_running=0` it does not wait and the traffic stays 0.
- `deployment.candidate` is `None` without a candidate.
- `get_version(n).candidate` marks the current candidate and `.activated` is False for a discarded one, whose number is kept and never reused.
- While a candidate exists, `save`, `save(new_version=True)`, `rollback` and `stop` are refused: promote or discard it first.
- `get_logs`, `read_logs` and `tail_logs` take `variant="candidate"` to read the candidate's pods; the default `"primary"` reads the live version.
- A candidate can carry a different schema; the active schema switches on rollout (see [deployment-schema.md](deployment-schema.md)).
- Declare the `deployment_version` extra logging column to tell the candidate's logged rows from the live version's.
