"""Public observation projection. Never expose DSNs, SQL rows or binding credentials."""

from copy import deepcopy


def public_state(record: dict) -> dict:
    state = deepcopy(record["state"])
    internal = record["internal"]
    observed = internal.get("observedDetails", {})
    source, target = observed.get("source", {}), observed.get("target", {})
    evidence = internal.get("validationEvidence")
    if evidence and evidence.get("transitionId") != state.get("transitionId"):
        evidence = None
    state["details"] = {
        "database": {
            "observedAt": state["observed"].get("observedAt") if observed else None,
            "observationAvailable": not internal.get("observationUnavailable", False)
            and bool(observed),
            "proxyPaused": observed.get("paused"),
            "sourceWriterSessions": source.get("sessions"),
            "targetWriterSessions": target.get("sessions"),
            "sourcePreparedTransactions": source.get("prepared"),
            "targetPreparedTransactions": target.get("prepared"),
            "targetReachable": observed.get("targetReachable"),
            "accessEvidence": observed.get("accessEvidence"),
            "verification": evidence,
            "lastCutover": {
                k: v for k, v in (internal.get("lastCutover") or {}).items() if k != "steps"
            }
            or None,
        }
    }
    return state
