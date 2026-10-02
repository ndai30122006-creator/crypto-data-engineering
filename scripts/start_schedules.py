"""Start/check schedules through the running webserver's workspace and instance.

Avoid CLI-local repository origins differing from the daemon's gRPC workspace.
Run: python scripts/start_schedules.py [--check] [--url http://localhost:3000]
"""

import argparse

import httpx

START = """mutation($selector: ScheduleSelector!) {
  startSchedule(scheduleSelector: $selector) {
    __typename ... on ScheduleStateResult { scheduleState { name status } }
  }
}"""
CHECK = """query($selector: RepositorySelector!) {
  repositoryOrError(repositorySelector: $selector) {
    __typename ... on Repository { schedules { name scheduleState { status } } }
  }
}"""


def schedules(client, *, check=False):
    repository = {"repositoryLocationName": "crypto-data-platform", "repositoryName": "__repository__"}
    if not check:
        for name in ("news_job_schedule", "market_job_schedule"):
            response = client.post("/graphql", json={"query": START,
                "variables": {"selector": {**repository, "scheduleName": name}}})
            response.raise_for_status()
            data = response.json()
            result = data.get("data", {}).get("startSchedule", {})
            if data.get("errors") or result.get("__typename") != "ScheduleStateResult":
                raise RuntimeError("schedule start failed; inspect webserver workspace")
    response = client.post("/graphql", json={"query": CHECK, "variables": {"selector": repository}})
    response.raise_for_status()
    data = response.json()
    result = data.get("data", {}).get("repositoryOrError", {})
    if data.get("errors") or result.get("__typename") != "Repository":
        raise RuntimeError("schedule query failed; inspect webserver workspace")
    states = {s["name"]: s["scheduleState"]["status"] for s in result["schedules"]}
    if any(states.get(name) != "RUNNING" for name in ("news_job_schedule", "market_job_schedule")):
        raise RuntimeError(f"schedules not running: {states}")
    return states


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:3000")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    with httpx.Client(base_url=args.url, timeout=15) as client:
        print(schedules(client, check=args.check))


if __name__ == "__main__":
    main()
