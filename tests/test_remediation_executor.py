
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


EXECUTOR_PATH = (
    Path(__file__).resolve().parents[1]
    / "agents"
    / "remediation"
    / "executor.py"
)

spec = importlib.util.spec_from_file_location(
    "remediation_executor_under_test",
    EXECUTOR_PATH,
)
executor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(executor)


class TestRemediationExecutor(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.root = Path(self.temp_dir.name)
        self.original_remediation_dir = executor.REMEDIATION_DIR
        executor.REMEDIATION_DIR = self.root
        self.addCleanup(
            setattr,
            executor,
            "REMEDIATION_DIR",
            self.original_remediation_dir,
        )

        self.approval = {
            "dataset": "olist_order_payments",
            "action": "QUARANTINE_RECORDS",
            "status": "APPROVED",
            "execution_authorized": True,
        }
        self.plan = {
            "dataset": "olist_order_payments",
            "action": "QUARANTINE_RECORDS",
            "requires_approval": True,
            "status": "PENDING_APPROVAL",
            "execution_mode": "DRY_RUN",
        }
        self.dq_result = {
            "dataset": "olist_order_payments",
            "validated": True,
            "invalid_count": 2,
        }
        self.spark_report = {
            "dataset": "olist_order_payments",
            "status": "VALIDATED",
            "mode": "DRY_RUN",
            "invalid_count": 2,
            "s3_write_performed": False,
            "source_modified": False,
        }

    def mock_inputs(self):
        def fake_load(path):
            if "approvals" in str(path):
                return self.approval
            return self.plan

        return patch.object(executor, "load_json", side_effect=fake_load)

    def mock_validations(self):
        return (
            patch.object(executor, "validate_action"),
            patch.object(executor, "validate_approval"),
            patch.object(executor, "validate_remediation_plan"),
            patch.object(
                executor,
                "validate_dq_failure",
                return_value=self.dq_result,
            ),
        )

    def test_successful_dry_run_writes_history_and_summary(self):
        with self.mock_inputs(), \
             self.mock_validations()[0], \
             self.mock_validations()[1], \
             self.mock_validations()[2], \
             self.mock_validations()[3], \
             patch.object(
                 executor,
                 "run_quarantine_dry_run",
                 return_value=self.spark_report,
             ):
            result = executor.execute(
                "olist_order_payments",
                "QUARANTINE_RECORDS",
            )

        execution = result["execution"]
        self.assertEqual(execution["status"], "VALIDATED")
        self.assertFalse(execution["executed"])
        self.assertFalse(execution["s3_write_performed"])

        history = self.root / "history" / f"{execution['execution_id']}.json"
        summary = self.root / "olist_order_payments_execution.json"

        self.assertTrue(history.is_file())
        self.assertTrue(summary.is_file())

    def test_validation_failure_prevents_spark(self):
        with self.mock_inputs(), \
             patch.object(
                 executor,
                 "validate_action",
                 side_effect=RuntimeError("approval/action rejected"),
             ), \
             patch.object(
                 executor,
                 "run_quarantine_dry_run",
             ) as spark_mock:
            with self.assertRaisesRegex(
                RuntimeError,
                "approval/action rejected",
            ):
                executor.execute(
                    "olist_order_payments",
                    "QUARANTINE_RECORDS",
                )

        spark_mock.assert_not_called()

        history_dir = self.root / "history"
        self.assertTrue(history_dir.is_dir())
        self.assertEqual(len(list(history_dir.glob("*.json"))), 1)

    def test_spark_failure_is_audited(self):
        with self.mock_inputs(), \
             self.mock_validations()[0], \
             self.mock_validations()[1], \
             self.mock_validations()[2], \
             self.mock_validations()[3], \
             patch.object(
                 executor,
                 "run_quarantine_dry_run",
                 side_effect=RuntimeError("simulated Spark failure"),
             ):
            with self.assertRaisesRegex(
                RuntimeError,
                "simulated Spark failure",
            ):
                executor.execute(
                    "olist_order_payments",
                    "QUARANTINE_RECORDS",
                )

        history_files = list((self.root / "history").glob("*.json"))
        self.assertEqual(len(history_files), 1)

        import json
        with history_files[0].open(encoding="utf-8") as file:
            record = json.load(file)

        self.assertEqual(record["execution"]["status"], "FAILED")
        self.assertFalse(record["execution"]["s3_write_performed"])

    def test_unsupported_dataset_is_rejected_before_validation(self):
        with patch.object(executor, "validate_action") as validation_mock, \
             patch.object(executor, "run_quarantine_dry_run") as spark_mock:
            with self.assertRaisesRegex(
                RuntimeError, "Dataset is not enabled for remediation"
            ):
                executor.execute("unsupported_dataset", "QUARANTINE_RECORDS")

        validation_mock.assert_not_called()
        spark_mock.assert_not_called()

    def test_unapproved_request_is_audited_without_spark(self):
        self.approval["status"] = "PENDING_APPROVAL"

        with self.mock_inputs(), \
             patch.object(executor, "validate_action"), \
             patch.object(executor, "validate_approval", wraps=executor.validate_approval), \
             patch.object(executor, "run_quarantine_dry_run") as spark_mock:
            with self.assertRaisesRegex(RuntimeError, "Remediation is not approved"):
                executor.execute("olist_order_payments", "QUARANTINE_RECORDS")

        spark_mock.assert_not_called()

        history_files = list((self.root / "history").glob("*.json"))
        self.assertEqual(len(history_files), 1)

        import json
        with history_files[0].open(encoding="utf-8") as file:
            record = json.load(file)

        self.assertEqual(record["execution"]["status"], "FAILED")
        self.assertFalse(record["execution"]["s3_write_performed"])

    def test_summary_failure_reports_history_and_cleans_temp_file(self):
        with self.mock_inputs(), \
             self.mock_validations()[0], \
             self.mock_validations()[1], \
             self.mock_validations()[2], \
             self.mock_validations()[3], \
             patch.object(
                 executor,
                 "run_quarantine_dry_run",
                 return_value=self.spark_report,
             ), \
             patch.object(
                 executor.os,
                 "replace",
                 side_effect=OSError("simulated publication failure"),
             ):
            with self.assertRaisesRegex(
                RuntimeError,
                "latest-execution summary publication failed",
            ) as raised:
                executor.execute(
                    "olist_order_payments",
                    "QUARANTINE_RECORDS",
                )

        self.assertIn(
            "per-execution history was recorded",
            str(raised.exception),
        )

        history_files = list((self.root / "history").glob("*.json"))
        self.assertEqual(len(history_files), 1)

        remaining_temps = list(self.root.glob(".*.tmp"))
        self.assertEqual(remaining_temps, [])

    def test_execution_history_collision_does_not_overwrite_existing_record(self):
        import json

        execution_id = "quarantine-collision-test"
        history_dir = self.root / "history"
        history_dir.mkdir(parents=True)

        history_path = history_dir / f"{execution_id}.json"
        original_record = {
            "execution": {"execution_id": execution_id},
            "marker": "original-record",
        }

        with history_path.open("w", encoding="utf-8") as file:
            json.dump(original_record, file)

        replacement_record = {
            "execution": {"execution_id": execution_id},
            "marker": "replacement-record",
        }
        with self.assertRaises(FileExistsError):
            executor.write_execution_history(replacement_record)

        with history_path.open(encoding="utf-8") as file:
            saved_record = json.load(file)

        self.assertEqual(saved_record, original_record)
        self.assertEqual(
            list(history_dir.glob(".*.tmp")),
            [],
            "Temporary files should be cleaned up after a collision",
        )

    def test_disabled_quarantine_config_is_rejected(self):
        import textwrap

        config_dir = self.root / "config"
        quarantine_dir = config_dir / "quarantine"
        quarantine_dir.mkdir(parents=True)

        config_path = quarantine_dir / "olist_order_payments.yaml"
        config_path.write_text(
            textwrap.dedent("""
                dataset: olist_order_payments
                quarantine:
                  enabled: false
                  catalog: glue_catalog
                  database: datapilot
                  table: olist_order_payments_quarantine
                  format: iceberg
                  source_layer: silver
                  target_layer: quarantine
                  keys:
                    - order_id
                    - payment_sequential
                  validate_source_duplicates: true
            """).lstrip(),
            encoding="utf-8",
        )

        with patch.object(executor, "CONFIG_DIR", config_dir):
            with self.assertRaisesRegex(
                RuntimeError, "Invalid quarantine configuration for 'enabled'"
            ):
                executor.validate_quarantine_config("olist_order_payments")

    def test_unexpected_quarantine_table_is_rejected(self):
        import textwrap

        config_dir = self.root / "config"
        quarantine_dir = config_dir / "quarantine"
        quarantine_dir.mkdir(parents=True)

        config_path = quarantine_dir / "olist_order_payments.yaml"
        config_path.write_text(
            textwrap.dedent("""
                dataset: olist_order_payments
                quarantine:
                  enabled: true
                  catalog: glue_catalog
                  database: datapilot
                  table: unexpected_table
                  format: iceberg
                  source_layer: silver
                  target_layer: quarantine
                  keys:
                    - order_id
                    - payment_sequential
                  validate_source_duplicates: true
            """).lstrip(),
            encoding="utf-8",
        )

        with patch.object(executor, "CONFIG_DIR", config_dir):
            with self.assertRaisesRegex(
                RuntimeError, "Invalid quarantine configuration for 'table'"
            ):
                executor.validate_quarantine_config("olist_order_payments")

    def test_quarantine_config_failure_is_audited_without_spark(self):
        with self.mock_inputs(), \
             patch.object(executor, "validate_action"), \
             patch.object(executor, "validate_approval"), \
             patch.object(executor, "validate_remediation_plan"), \
             patch.object(
                 executor,
                 "validate_quarantine_config",
                 side_effect=RuntimeError("invalid quarantine configuration"),
             ), \
             patch.object(executor, "run_quarantine_dry_run") as spark_mock:
            with self.assertRaisesRegex(
                RuntimeError, "invalid quarantine configuration"
            ):
                executor.execute(
                    "olist_order_payments",
                    "QUARANTINE_RECORDS",
                )

        spark_mock.assert_not_called()

        history_files = list((self.root / "history").glob("*.json"))
        self.assertEqual(len(history_files), 1)

        import json
        with history_files[0].open(encoding="utf-8") as file:
            record = json.load(file)

        self.assertEqual(record["execution"]["status"], "FAILED")
        self.assertFalse(record["execution"]["s3_write_performed"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
