import unittest
from types import SimpleNamespace
from agents.rca.rca_evaluators import evidence_grounding
from agents.rca.rca_agent import (
    validate_dq_result,
    validate_mcp_evidence,
    validate_additional_mcp_evidence,
    validate_rca_result,
    determine_expected_classification,
)


class TestRCAEvaluation(unittest.TestCase):

    def setUp(self):
        self.dataset = "olist_order_payments"
        self.column = "payment_installments"
        self.minimum = 1
        self.maximum = 100

        self.dq_result = {
            "dataset": self.dataset,
            "status": "FAIL",
            "checks": [
                {
                    "check": "range_check:payment_installments",
                    "status": "FAIL",
                    "range": "1-100",
                }
            ],
        }

        self.bronze_result = {
            "dataset": self.dataset,
            "layer": "bronze",
            "column": self.column,
            "path": "synthetic/bronze/data",
            "status": "FAIL",
            "minimum": self.minimum,
            "maximum": self.maximum,
            "invalid_count": 2,
            "sample_invalid_values": [0, 101],
        }

        self.silver_result = {
            "dataset": self.dataset,
            "layer": "silver",
            "column": self.column,
            "path": "synthetic/silver/data",
            "status": "PASS",
            "minimum": self.minimum,
            "maximum": self.maximum,
            "invalid_count": 0,
            "sample_invalid_values": [],
        }

        self.invalid_records = {
            "dataset": self.dataset,
            "layer": "bronze",
            "column": self.column,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "status": "FAIL",
            "invalid_count": 2,
            "records_returned": 2,
            "records": [
                {"synthetic_record": "invalid_1"},
                {"synthetic_record": "invalid_2"},
            ],
        }

        self.remediation_audit = {
            "read_only": True,
            "count": 1,
            "records": [
                {
                    "dataset": self.dataset,
                    "action": "QUARANTINE_RECORDS",
                    "execution_mode": "DRY_RUN",
                }
            ],
        }

        self.valid_rca_result = {
            "dataset": self.dataset,
            "dq_status": "FAIL",
            "classification": "UPSTREAM_DATA_QUALITY",
            "root_cause": "Synthetic upstream range violation.",
            "evidence": ["Synthetic Bronze evidence indicates invalid values."],
            "recommendation": "Review invalid source records.",
            "remediation": {
                "action": "QUARANTINE_RECORDS",
                "requires_approval": True,
                "reason": "Quarantine requires explicit approval.",
            },
            "next_actions": ["Review the proposed remediation."],
            "confidence": "MEDIUM",
        }

    # ---------------------------------------------------------
    # DQ response validation
    # ---------------------------------------------------------

    def test_valid_dq_result(self):
        self.assertTrue(
            validate_dq_result(self.dq_result, self.dataset)
        )

    def test_dq_dataset_mismatch_rejected(self):
        with self.assertRaises(ValueError):
            validate_dq_result(self.dq_result, "another_dataset")

    def test_dq_invalid_status_rejected(self):
        result = dict(self.dq_result)
        result["status"] = "UNKNOWN"

        with self.assertRaises(ValueError):
            validate_dq_result(result, self.dataset)

    def test_dq_failed_range_without_range_rejected(self):
        result = {
            "dataset": self.dataset,
            "status": "FAIL",
            "checks": [
                {
                    "check": "range_check:payment_installments",
                    "status": "FAIL",
                }
            ],
        }

        with self.assertRaises(ValueError):
            validate_dq_result(result, self.dataset)

    # ---------------------------------------------------------
    # Bronze and Silver evidence validation
    # ---------------------------------------------------------

    def test_valid_bronze_silver_evidence(self):
        self.assertTrue(
            validate_mcp_evidence(
                self.dq_result,
                self.bronze_result,
                self.silver_result,
                expected_column=self.column,
                expected_minimum=self.minimum,
                expected_maximum=self.maximum,
            )
        )

    def test_bronze_dataset_mismatch_rejected(self):
        bronze = dict(self.bronze_result)
        bronze["dataset"] = "another_dataset"

        with self.assertRaises(ValueError):
            validate_mcp_evidence(
                self.dq_result, bronze, self.silver_result,
                self.column, self.minimum, self.maximum
            )

    def test_silver_layer_mismatch_rejected(self):
        silver = dict(self.silver_result)
        silver["layer"] = "bronze"

        with self.assertRaises(ValueError):
            validate_mcp_evidence(
                self.dq_result, self.bronze_result, silver,
                self.column, self.minimum, self.maximum
            )

    def test_wrong_bronze_column_rejected(self):
        bronze = dict(self.bronze_result)
        bronze["column"] = "another_column"

        with self.assertRaises(ValueError):
            validate_mcp_evidence(
                self.dq_result, bronze, self.silver_result,
                self.column, self.minimum, self.maximum
            )

    def test_wrong_silver_range_rejected(self):
        silver = dict(self.silver_result)
        silver["minimum"] = 0

        with self.assertRaises(ValueError):
            validate_mcp_evidence(
                self.dq_result, self.bronze_result, silver,
                self.column, self.minimum, self.maximum
            )

    # ---------------------------------------------------------
    # Additional MCP evidence validation
    # ---------------------------------------------------------

    def test_valid_additional_mcp_evidence(self):
        self.assertTrue(
            validate_additional_mcp_evidence(
                self.dataset,
                self.column,
                self.minimum,
                self.maximum,
                self.invalid_records,
                self.remediation_audit,
            )
        )

    def test_invalid_record_count_mismatch_rejected(self):
        records = dict(self.invalid_records)
        records["records_returned"] = 1

        with self.assertRaises(ValueError):
            validate_additional_mcp_evidence(
                self.dataset, self.column, self.minimum, self.maximum,
                records, self.remediation_audit
            )

    def test_invalid_record_dataset_mismatch_rejected(self):
        records = dict(self.invalid_records)
        records["dataset"] = "another_dataset"

        with self.assertRaises(ValueError):
            validate_additional_mcp_evidence(
                self.dataset, self.column, self.minimum, self.maximum,
                records, self.remediation_audit
            )

    def test_non_read_only_audit_rejected(self):
        audit = dict(self.remediation_audit)
        audit["read_only"] = False

        with self.assertRaises(ValueError):
            validate_additional_mcp_evidence(
                self.dataset, self.column, self.minimum, self.maximum,
                self.invalid_records, audit
            )

    def test_audit_count_mismatch_rejected(self):
        audit = dict(self.remediation_audit)
        audit["count"] = 2

        with self.assertRaises(ValueError):
            validate_additional_mcp_evidence(
                self.dataset, self.column, self.minimum, self.maximum,
                self.invalid_records, audit
            )

    # ---------------------------------------------------------
    # RCA output and decision-safety validation
    # ---------------------------------------------------------

    def test_valid_approval_gated_rca_result(self):
        self.assertTrue(
            validate_rca_result(self.valid_rca_result, self.dq_result)
        )

    def test_rca_dataset_mismatch_rejected(self):
        result = dict(self.valid_rca_result)
        result["dataset"] = "another_dataset"

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)

    def test_rca_dq_status_mismatch_rejected(self):
        result = dict(self.valid_rca_result)
        result["dq_status"] = "PASS"

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)

    def test_inconclusive_high_confidence_rejected(self):
        result = dict(self.valid_rca_result)
        result["classification"] = "RCA_INCONCLUSIVE"
        result["confidence"] = "HIGH"
        result["remediation"] = {
            "action": "MANUAL_REVIEW",
            "requires_approval": True,
            "reason": "Manual review required.",
        }

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)

    def test_inconclusive_automated_remediation_rejected(self):
        result = dict(self.valid_rca_result)
        result["classification"] = "RCA_INCONCLUSIVE"
        result["confidence"] = "LOW"

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)

    def test_no_action_requires_justification(self):
        result = dict(self.valid_rca_result)
        result["remediation"] = {
            "action": "NO_ACTION",
            "requires_approval": False,
            "reason": "   ",
        }

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)

    def test_remediation_without_approval_rejected(self):
        result = dict(self.valid_rca_result)
        result["remediation"] = {
            "action": "QUARANTINE_RECORDS",
            "requires_approval": False,
            "reason": "Synthetic remediation test.",
        }

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)

    def test_missing_required_rca_field_rejected(self):
        result = dict(self.valid_rca_result)
        result.pop("evidence")

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result)
    

    def test_deterministic_classification_matrix(self):
        scenarios = [
            ("FAIL", "FAIL", "FAIL", "UPSTREAM_DATA_QUALITY"),
            ("FAIL", "PASS", "FAIL", "TRANSFORMATION_DATA_QUALITY"),
            ("FAIL", "FAIL", "PASS", "RCA_INCONCLUSIVE"),
            ("FAIL", "PASS", "PASS", "RCA_INCONCLUSIVE"),
            ("FAIL", "FAIL", "UNAVAILABLE", "RCA_INCONCLUSIVE"),
            ("FAIL", "UNAVAILABLE", "FAIL", "RCA_INCONCLUSIVE"),
            ("PASS", "FAIL", "FAIL", "RCA_INCONCLUSIVE"),
        ]

        for dq_status, bronze_status, silver_status, expected in scenarios:
            with self.subTest(
                dq=dq_status,
                bronze=bronze_status,
                silver=silver_status
            ):
                dq_result = {
                    "dataset": self.dataset,
                    "status": dq_status,
                }

                evidence = {
                    "bronze": {"status": bronze_status},
                    "silver": {"status": silver_status},
                }

                actual = determine_expected_classification(
                    dq_result,
                    evidence
                )

                self.assertEqual(actual, expected)

    def test_evidence_matching_classification_accepted(self):
        evidence = {
            "bronze": {"status": "FAIL"},
            "silver": {"status": "FAIL"},
        }

        self.assertTrue(
            validate_rca_result(
                self.valid_rca_result,
                self.dq_result,
                evidence
            )
        )

    def test_evidence_conflicting_classification_rejected(self):
        result = dict(self.valid_rca_result)
        result["classification"] = "TRANSFORMATION_DATA_QUALITY"

        evidence = {
            "bronze": {"status": "FAIL"},
            "silver": {"status": "FAIL"},
        }

        with self.assertRaises(ValueError):
            validate_rca_result(
                result,
                self.dq_result,
                evidence
            )

    def test_unavailable_silver_requires_manual_review(self):
        result = dict(self.valid_rca_result)
        result["classification"] = "RCA_INCONCLUSIVE"
        result["confidence"] = "LOW"
        result["remediation"] = {
            "action": "MANUAL_REVIEW",
            "requires_approval": True,
            "reason": "Silver evidence is unavailable.",
        }
        evidence = {
            "bronze": {"status": "FAIL"},
            "silver": {"status": "UNAVAILABLE"},
        }

        self.assertTrue(
            validate_rca_result(
                result,
                self.dq_result,
                evidence
            )
        )
    
    def test_upstream_classification_requires_quarantine(self):
        result = dict(self.valid_rca_result)

        evidence = {
            "bronze": {"status": "FAIL"},
            "silver": {"status": "FAIL"},
        }

        result["remediation"] = {
            "action": "TRANSFORMATION_FIX",
            "requires_approval": True,
            "reason": "Test remediation.",
        }

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result, evidence)

    def test_transformation_classification_requires_transformation_fix(self):
        result = dict(self.valid_rca_result)
        result["classification"] = "TRANSFORMATION_DATA_QUALITY"
        result["remediation"] = {
            "action": "QUARANTINE_RECORDS",
            "requires_approval": True,
            "reason": "Test remediation.",
        }

        evidence = {
            "bronze": {"status": "PASS"},
            "silver": {"status": "FAIL"},
        }

        with self.assertRaises(ValueError):
            validate_rca_result(result, self.dq_result, evidence)

    def test_inconclusive_allows_manual_review(self):
        result = dict(self.valid_rca_result)
        result["classification"] = "RCA_INCONCLUSIVE"
        result["confidence"] = "LOW"
        result["remediation"] = {
            "action": "MANUAL_REVIEW",
            "requires_approval": True,
            "reason": "Insufficient evidence.",
        }

        evidence = {
            "bronze": {"status": "FAIL"},
            "silver": {"status": "PASS"},
        }

        self.assertTrue(
            validate_rca_result(result, self.dq_result, evidence)
        )
    

class TestEvidenceGrounding(unittest.TestCase):

    def evaluate(self, actual_evidence, expected_facts, inputs):
        run = SimpleNamespace(
            outputs={"evidence": actual_evidence}
        )

        example = SimpleNamespace(
            outputs={
                "expected_evidence_facts": expected_facts
            },
            inputs=inputs,
        )

        return evidence_grounding(run, example)["score"]

    def test_equivalent_wording_is_accepted(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
                "silver": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
            },
        }

        actual = [
            "Bronze status is FAIL with invalid_count 2.",
            "Silver status is FAIL with invalid_count 2.",
        ]

        expected = [
            "Bronze contains two invalid records.",
            "Silver contains two invalid records.",
        ]

        self.assertTrue(
            self.evaluate(actual, expected, inputs)
        )

    def test_facts_split_across_statements_are_accepted(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {
                    "status": "PASS",
                    "invalid_count": 0,
                },
                "silver": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
            },
        }

        actual = [
            "The Bronze inspection passed.",
            "The Bronze invalid_count is zero.",
            "Silver inspection failed.",
            "Two invalid records were found in Silver.",
        ]

        expected = [
            "Bronze contains zero invalid records.",
            "Silver contains two invalid records.",
        ]

        self.assertTrue(
            self.evaluate(actual, expected, inputs)
        )

    def test_missing_expected_fact_is_rejected(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
                "silver": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
            },
        }

        actual = [
            "Bronze contains two invalid records."
        ]

        expected = [
            "Bronze contains two invalid records.",
            "Silver contains two invalid records.",
        ]

        self.assertFalse(
            self.evaluate(actual, expected, inputs)
        )

    def test_contradictory_invalid_count_is_rejected(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
                "silver": {
                    "status": "FAIL",
                    "invalid_count": 2,
                },
            },
        }

        actual = [
            "Bronze contains 99 invalid records.",
            "Silver contains two invalid records.",
        ]

        expected = [
            "Bronze contains two invalid records.",
            "Silver contains two invalid records.",
        ]

        self.assertFalse(
            self.evaluate(actual, expected, inputs)
        )

    def test_historical_remediation_requires_audit_evidence(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {
                    "status": "FAIL",
                    "invalid_count": 3,
                },
                "silver": {
                    "status": "FAIL",
                    "invalid_count": 3,
                },
                "remediation_audit": {
                    "count": 0,
                    "records": [],
                    "read_only": True,
                },
            },
        }

        actual = [
            "Bronze contains three invalid records.",
            "Silver contains three invalid records.",
            "Historical remediation was committed and verified.",
        ]

        expected = [
            "Bronze contains three invalid records.",
            "Silver contains three invalid records.",
        ]

        self.assertFalse(
            self.evaluate(actual, expected, inputs)
        )

    
    def test_contradictory_bronze_status_is_rejected(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {"status": "FAIL", "invalid_count": 2},
                "silver": {"status": "PASS", "invalid_count": 0},
            },
        }

        actual = [
            "Bronze inspection passed.",
            "Bronze contains two invalid records.",
            "Silver inspection passed.",
            "Silver contains zero invalid records.",
        ]

        expected = [
            "Bronze contains two invalid records.",
            "Silver contains zero invalid records.",
        ]

        self.assertFalse(self.evaluate(actual, expected, inputs))

    def test_contradictory_silver_status_is_rejected(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {"status": "PASS", "invalid_count": 0},
                "silver": {"status": "PASS", "invalid_count": 0},
            },
        }

        actual = [
            "Bronze inspection passed with zero invalid records.",
            "Silver inspection failed with zero invalid records.",
        ]

        expected = [
            "Bronze contains zero invalid records.",
            "Silver contains zero invalid records.",
        ]

        self.assertFalse(self.evaluate(actual, expected, inputs))

    def test_dq_failure_claim_when_source_passes_is_rejected(self):
        inputs = {
            "dq_result": {"status": "PASS"},
            "rca_evidence": {
                "bronze": {"status": "PASS", "invalid_count": 0},
                "silver": {"status": "PASS", "invalid_count": 0},
            },
        }

        actual = [
            "The DQ check failed.",
            "Bronze contains zero invalid records.",
            "Silver contains zero invalid records.",
        ]

        expected = [
            "Bronze contains zero invalid records.",
            "Silver contains zero invalid records.",
        ]

        self.assertFalse(self.evaluate(actual, expected, inputs))

    def test_historical_success_cannot_claim_current_incident_resolved(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {"status": "FAIL", "invalid_count": 3},
                "silver": {"status": "FAIL", "invalid_count": 3},
                "remediation_audit": {
                    "count": 1,
                    "records": [{
                        "execution_status": "COMMITTED_AND_VERIFIED"
                    }],
                },
            },
        }

        actual = [
            "Bronze contains three invalid records.",
            "Silver contains three invalid records.",
            "Historical remediation was committed and verified.",
            "The current incident is resolved.",
        ]

        expected = [
            "Bronze contains three invalid records.",
            "Silver contains three invalid records.",
            "Historical remediation does not prove the current incident is resolved.",
        ]

        self.assertFalse(self.evaluate(actual, expected, inputs))
    
    def test_current_resolution_claim_rejected_when_layers_still_fail(self):
        inputs = {
            "dq_result": {"status": "FAIL"},
            "rca_evidence": {
                "bronze": {"status": "FAIL", "invalid_count": 3},
                "silver": {"status": "FAIL", "invalid_count": 3},
                "remediation_audit": {
                    "count": 1,
                    "records": [{
                        "execution_status": "COMMITTED_AND_VERIFIED"
                    }],
                },
            },
        }

        actual = [
            "Bronze contains three invalid records.",
            "Silver contains three invalid records.",
            "Historical remediation was committed and verified.",
            "The current incident is resolved.",
        ]

        expected = [
            "Bronze contains three invalid records.",
            "Silver contains three invalid records.",
        ]

        self.assertFalse(self.evaluate(actual, expected, inputs))

if __name__ == "__main__":
    unittest.main(verbosity=2)
