import os


def rca_actual_target(inputs):
    """
    Actual LLM-powered RCA evaluation target.

    Uses synthetic evaluation inputs and bypasses MCP.
    Requires explicit opt-in before making OpenAI requests.
    """

    if os.getenv("DATAPILOT_ALLOW_LIVE_RCA_EVAL") != "true":
        raise RuntimeError(
            "Actual RCA evaluation is disabled. "
            "Explicitly enable DATAPILOT_ALLOW_LIVE_RCA_EVAL=true "
            "before running the evaluation."
        )

    from agents.rca.rca_agent import run_rca

    dq_result = inputs.get("dq_result")
    rca_evidence = inputs.get("rca_evidence")

    if not isinstance(dq_result, dict):
        raise ValueError("Evaluation input requires dq_result.")

    if not isinstance(rca_evidence, dict):
        raise ValueError("Evaluation input requires rca_evidence.")

    # Copy the evidence so the original LangSmith dataset input is unchanged.
    normalized_evidence = dict(rca_evidence)

    # Explicitly represent missing Silver evidence.
    if normalized_evidence.get("silver") is None:
        normalized_evidence["silver"] = {
            "layer": "silver",
            "status": "UNAVAILABLE",
            "reason": (
                "Silver evidence is unavailable in this synthetic "
                "evaluation scenario."
            ),
        }

    return run_rca(
        dq_result=dq_result,
        rca_evidence=normalized_evidence
    )