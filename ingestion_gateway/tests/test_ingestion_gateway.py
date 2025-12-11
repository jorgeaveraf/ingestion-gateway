import os
from pathlib import Path
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from ingestion_gateway.core.config import get_settings


class _StubAirflowClient:
    def __init__(self, dag_state: str = "success") -> None:
        self.triggered: list[Dict[str, Any]] = []
        self.dag_state = dag_state

    async def trigger_dag(self, dag_id: str, dag_run) -> Dict[str, Any]:
        self.triggered.append({"dag_id": dag_id, "dag_run": dag_run.model_dump()})
        return {"dag_run_id": dag_run.dag_run_id, "state": "queued"}

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "state": self.dag_state,
            "end_date": "2023-01-01T00:00:00+00:00",
        }

    async def close(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _clear_settings_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("SHARED_INPUT_FOLDER", str(tmp_path / "shared"))
    monkeypatch.setenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "1")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def app_with_stub(tmp_path, monkeypatch):
    from ingestion_gateway.core import deps
    from ingestion_gateway.app import app

    stub_client = _StubAirflowClient()

    async def override_client():
        yield stub_client

    app.dependency_overrides[deps.get_airflow_client] = override_client
    yield app, stub_client
    app.dependency_overrides = {}


def test_ingest_part1_creates_run_folder_and_triggers(app_with_stub: tuple):
    app, stub_client = app_with_stub
    client = TestClient(app)
    response = client.post(
        "/ingest/part1",
        data={"week_year": 2023, "week_num": 10, "notify_email": "demo@example.com"},
        files={"files": ("data.csv", "a,b\n1,2\n", "text/csv")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    dag_run_id = payload["dag_run_id"]
    shared_root = Path(get_settings().shared_input_folder)
    stored_file = shared_root / "runs" / dag_run_id / "data.csv"
    assert stored_file.exists()
    assert stub_client.triggered, "Airflow trigger was not called"
    assert stub_client.triggered[0]["dag_run"]["conf"]["input_subdir"] == f"runs/{dag_run_id}"


def test_ingest_rejects_non_csv(app_with_stub: tuple):
    app, _ = app_with_stub
    client = TestClient(app)
    response = client.post(
        "/ingest/part2",
        data={"week_year": 2023, "week_num": 12, "notify_email": "demo@example.com"},
        files={"files": ("data.txt", "not csv", "text/plain")},
    )
    assert response.status_code == 400
    assert "must have a .csv extension" in response.json()["detail"]


def test_poll_endpoint_returns_outputs(monkeypatch, tmp_path):
    shared = tmp_path / "shared"
    run_id = "gw_test"
    outputs_dir = shared / "runs" / run_id / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    output_file = outputs_dir / "result.csv"
    output_file.write_text("x,y\n1,2\n")

    monkeypatch.setenv("SHARED_INPUT_FOLDER", str(shared))
    get_settings.cache_clear()

    from ingestion_gateway.core import deps
    from ingestion_gateway.app import app
    stub_client = _StubAirflowClient()

    async def override_client():
        yield stub_client

    app.dependency_overrides[deps.get_airflow_client] = override_client
    client = TestClient(app)

    response = client.get(f"/poll/{run_id}", params={"dag": "part1"})
    assert response.status_code == 200
    data = response.json()
    assert data["state"] == "success"
    assert str(output_file) in data["outputs"]

    app.dependency_overrides = {}
