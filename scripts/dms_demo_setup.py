"""Connect the dedicated RDS/Cloud SQL demo to GCP DMS. No application cutover."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import google.auth
import psycopg
from dms_demo_data import connections
from google.auth.transport.requests import AuthorizedSession

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime/dms-cloud"
PARENT = "projects/foxtrot-513413/locations/asia-northeast3"
BASE = "https://datamigration.googleapis.com/v1/"
OWNER = "foxtrot"


def assert_empty_destination() -> None:
    _, target, _ = connections()
    with psycopg.connect(**{**target, "dbname": "postgres"}, autocommit=True) as conn:
        databases = conn.execute(
            "SELECT datname FROM pg_database WHERE NOT datistemplate "
            "AND datname NOT IN ('postgres','cloudsqladmin')"
        ).fetchall()
        tables = conn.execute(
            "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE c.relkind IN ('r','p') AND n.nspname !~ '^pg_' "
            "AND n.nspname <> 'information_schema' AND NOT EXISTS "
            "(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass "
            "AND d.objid=c.oid AND d.deptype='e') LIMIT 1"
        ).fetchall()
    if databases or tables:
        raise RuntimeError("Destination contains user databases or tables; refusing demotion")


def create(session, collection: str, resource_id: str, body: dict) -> dict:
    name = PARENT + "/" + collection + "/" + resource_id
    existing = session.get(BASE + name, timeout=20)
    if existing.status_code != 404:
        existing.raise_for_status()
        value = existing.json()
        if value.get("labels", {}).get("foxtrot-owner") != OWNER:
            raise RuntimeError("Existing resource belongs to another owner")
        return {"resource": name, "state": value.get("state"), "existing": True}
    parameter = {"connectionProfiles": "connectionProfileId", "migrationJobs": "migrationJobId"}
    request_id = str(uuid5(NAMESPACE_URL, name))
    intent = {
        "resource": name,
        "requestId": request_id,
        "at": datetime.now(UTC).isoformat(),
        "phase": "SUBMITTING",
    }
    path = RUNTIME / (resource_id + "-create.json")
    path.write_text(json.dumps(intent, indent=2))
    response = session.post(
        BASE + PARENT + "/" + collection,
        params={parameter[collection]: resource_id, "requestId": request_id},
        json={**body, "labels": {"foxtrot-owner": OWNER, "purpose": "dms-demo"}},
        timeout=30,
    )
    response.raise_for_status()
    operation = response.json()
    intent.update(phase="PENDING", operation=operation["name"])
    path.write_text(json.dumps(intent, indent=2))
    return intent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["profiles", "job", "demote"])
    args = parser.parse_args()
    credentials, _ = google.auth.default(
        quota_project_id="foxtrot-513413", scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(credentials)
    source_id, target_id, job_id = (
        "foxtrot-rds-demo-20261104",
        "foxtrot-cloudsql-demo-20261104",
        "foxtrot-rds-to-gcp-20261104",
    )
    binding = {
        "job": PARENT + "/migrationJobs/" + job_id,
        "source": PARENT + "/connectionProfiles/" + source_id,
        "destination": PARENT + "/connectionProfiles/" + target_id,
        "owner": OWNER,
    }
    if args.command == "profiles":
        source = json.loads((RUNTIME / "aws-source.json").read_text())
        secrets = json.loads((RUNTIME / "generated-secrets.json").read_text())
        if source["DBInstanceIdentifier"] != "foxtrot-dms-demo-20261104" or not source.get(
            "Endpoint"
        ):
            raise RuntimeError("The dedicated RDS endpoint is not available")
        result = [
            create(
                session,
                "connectionProfiles",
                source_id,
                {
                    "provider": "RDS",
                    "role": "SOURCE",
                    "postgresql": {
                        "host": source["Endpoint"]["Address"],
                        "port": 5432,
                        "username": "foxtrot_migrate",
                        "password": secrets["migration_password"],
                        "ssl": {
                            "type": "SERVER_ONLY",
                            "caCertificate": (RUNTIME / "rds-ca.pem").read_text(),
                        },
                    },
                },
            ),
            create(
                session,
                "connectionProfiles",
                target_id,
                {
                    "provider": "CLOUDSQL",
                    "role": "DESTINATION",
                    "postgresql": {
                        "cloudSqlId": "foxtrot-dms-demo-20261104",
                        "username": "postgres",
                        "password": secrets["target_password"],
                        "ssl": {"type": "REQUIRED"},
                    },
                },
            ),
        ]
    elif args.command == "job":
        result = create(
            session,
            "migrationJobs",
            job_id,
            {
                "source": binding["source"],
                "destination": binding["destination"],
                "type": "CONTINUOUS",
                "displayName": "Foxtrot RDS to Cloud SQL demo",
                "staticIpConnectivity": {},
                "performanceConfig": {"dumpParallelLevel": "MIN"},
            },
        )
        (RUNTIME / "job.json").write_text(json.dumps(binding, indent=2))
    else:
        # Only this newly provisioned, empty destination may be demoted.
        # Record intent and retain operation ID; never retry this mutation automatically.
        destination = session.get(BASE + binding["destination"], timeout=20)
        destination.raise_for_status()
        profile = destination.json()
        if (
            profile.get("labels", {}).get("foxtrot-owner") != OWNER
            or profile.get("postgresql", {}).get("cloudSqlId") != "foxtrot-dms-demo-20261104"
        ):
            raise RuntimeError("Destination identity mismatch")
        path = RUNTIME / "demote-operation.json"
        if path.exists():
            raise RuntimeError("Demotion was already submitted; inspect its operation instead")
        assert_empty_destination()
        path.write_text(json.dumps({"phase": "SUBMITTING", "at": datetime.now(UTC).isoformat()}))
        response = session.post(BASE + binding["job"] + ":demoteDestination", json={}, timeout=30)
        response.raise_for_status()
        result = response.json()
        path.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
