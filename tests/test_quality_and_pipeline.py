import json
import unittest

from src.batch_pipeline import run_batch_pipeline
from src.config_loader import get_repo_root, load_pipeline_config
from src.quality_rules import (
    clean_billing_record,
    clean_customer_org_record,
    clean_nps_survey_record,
    clean_resource_record,
    clean_support_ticket_record,
    clean_user_record,
    compute_cost_anomaly_stats_by_group,
    evaluate_usage_event_quality,
    impute_event_unit,
    score_event_cost_anomaly,
)
from src.schemas import SCHEMA_CONTRACTS
from src.serving_cassandra import run_serving_cassandra
from src.streaming_pipeline import run_streaming_pipeline


class TestCloudProviderPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo_root = get_repo_root()
        cls.config = load_pipeline_config()

    def test_schema_v1_v2_fields(self):
        events_contract = dict((col, (dtype, nullable)) for col, dtype, nullable in SCHEMA_CONTRACTS["usage_events"])
        self.assertIn("carbon_kg", events_contract)
        self.assertIn("genai_tokens", events_contract)
        self.assertTrue(events_contract["carbon_kg"][1])
        self.assertTrue(events_contract["genai_tokens"][1])

    def test_unit_imputation_from_metric(self):
        self.assertEqual(impute_event_unit("cpu_hours", None), ("hours", True))
        self.assertEqual(impute_event_unit("requests", ""), ("count", True))
        self.assertEqual(impute_event_unit("storage_gb_hours", None), ("gb_hours", True))
        self.assertEqual(impute_event_unit("cpu_hours", "hours"), ("hours", False))

    def test_negative_cost_event_goes_to_quarantine(self):
        bad_event = {
            "event_id": "evt_test_neg",
            "timestamp": "2025-07-20T10:00:00Z",
            "org_id": "org_001",
            "resource_id": "res_0001",
            "service": "compute",
            "region": "us-east",
            "metric": "cpu_hours",
            "value": 10.5,
            "unit": None,
            "cost_usd_increment": -4.25,
            "schema_version": 2,
            "carbon_kg": 0.02,
            "genai_tokens": None,
        }
        is_valid, reason, _ = evaluate_usage_event_quality(bad_event)
        self.assertFalse(is_valid)
        self.assertEqual(reason, "NEGATIVE_COST_INCREMENT")

        good_event = dict(bad_event, cost_usd_increment=12.50, value=None)
        is_valid_ok, reason_ok, cleaned_ok = evaluate_usage_event_quality(good_event)
        self.assertTrue(is_valid_ok)
        self.assertIsNone(reason_ok)
        self.assertEqual(cleaned_ok["unit"], "hours")
        self.assertTrue(cleaned_ok["is_unit_imputed"])
        self.assertTrue(cleaned_ok["is_value_null"])

    def test_resources_keeps_prefixes_and_unpacks_tags(self):
        raw_res = {
            "resource_id": "res_abc12345",
            "org_id": "org_xyz987",
            "service": "GenAI",
            "region": "US-EAST",
            "created_at": "2025-06-15T08:30:00Z",
            "state": "RUNNING",
            "tags_json": '{"env": "prod", "pii": true, "cost_center": "CC-10", "backup": "daily"}',
        }
        cleaned = clean_resource_record(raw_res)
        self.assertTrue(cleaned["resource_id"].startswith("res_"))
        self.assertTrue(cleaned["org_id"].startswith("org_"))
        self.assertEqual(cleaned["service"], "genai")
        self.assertEqual(cleaned["env"], "prod")
        self.assertTrue(cleaned["pii"])
        self.assertEqual(cleaned["costcenter"], "CC-10")
        self.assertEqual(cleaned["team"], "Sin Asignar")
        self.assertTrue(cleaned["backup"])

    def test_billing_usd_conversion_and_credits(self):
        raw_bill = {
            "invoice_id": "inv_01",
            "org_id": "org_001",
            "month": "2025-08-01",
            "subtotal": "-200.0",
            "credits": "",
            "taxes": "0.0",
            "currency": "EUR",
            "exchange_rate_to_usd": "1.10",
        }
        cleaned = clean_billing_record(raw_bill)
        self.assertEqual(cleaned["credits"], 0.0)
        self.assertTrue(cleaned["is_credits_imputed"])
        self.assertEqual(cleaned["subtotal_usd"], -220.0)
        self.assertTrue(cleaned["is_credit_note"])

    def test_support_tickets_csat_rules(self):
        open_ticket = {
            "ticket_id": "tkt_1",
            "org_id": "org_1",
            "category": "Billing ",
            "severity": "HIGH",
            "created_at": "2025-07-01",
            "resolved_at": "",
            "csat": "4.0",
            "sla_breached": "False",
        }
        c_open, r_open = clean_support_ticket_record(open_ticket)
        self.assertIsNone(c_open["csat"])
        self.assertEqual(r_open, "CSAT_ON_UNRESOLVED_TICKET")

        out_of_range_ticket = dict(open_ticket, resolved_at="2025-07-02", csat="7.0")
        c_oor, r_oor = clean_support_ticket_record(out_of_range_ticket)
        self.assertIsNone(c_oor["csat"])
        self.assertEqual(r_oor, "CSAT_OUT_OF_RANGE")

    def test_users_login_before_created_at(self):
        raw_user = {
            "user_id": "user_01",
            "org_id": "org_01",
            "email": "Test@Example.com ",
            "role": "Admin",
            "active": "True",
            "created_at": "2025-07-15",
            "last_login": "2025-06-10",
        }
        cleaned, inconsistent = clean_user_record(raw_user)
        self.assertTrue(inconsistent)
        self.assertIsNone(cleaned["last_login"])
        self.assertEqual(cleaned["email"], "test@example.com")

    def test_nps_empty_survey_and_org_nps_range(self):
        is_valid, reason, _ = clean_nps_survey_record(
            {"org_id": "org_064", "survey_date": "2025-07-09", "nps_score": "", "comment": ""}
        )
        self.assertFalse(is_valid)
        self.assertEqual(reason, "EMPTY_SURVEY_SCORE_AND_COMMENT")

        c_org, invalid_nps = clean_customer_org_record(
            {"org_id": "org_001", "org_name": "Acme", "plan_tier": "pro", "nps_score": "101"}
        )
        self.assertTrue(invalid_nps)
        self.assertIsNone(c_org["nps_score"])
        self.assertEqual(c_org["plan"], "pro")

    def test_mad_spike_detection(self):
        sample_events = [
            {"service": "compute", "metric": "cpu_hours", "cost_usd_increment": 1.0 + (i % 3) * 0.1}
            for i in range(30)
        ]
        sample_events.append({"service": "compute", "metric": "cpu_hours", "cost_usd_increment": 50.0})
        stats = compute_cost_anomaly_stats_by_group(sample_events)
        spike_score = score_event_cost_anomaly(50.0, stats[("compute", "cpu_hours")])
        normal_score = score_event_cost_anomaly(1.1, stats[("compute", "cpu_hours")])
        self.assertTrue(spike_score["is_mad_spike"])
        self.assertFalse(normal_score["is_mad_spike"])

    def test_referential_integrity_across_sources(self):
        ref_file = self.repo_root / "evidence" / "integridad_referencial_resumen.json"
        self.assertTrue(ref_file.exists())
        data = json.loads(ref_file.read_text(encoding="utf-8"))
        for ds_name, ds_metrics in data["org_id_integrity_by_dataset"].items():
            self.assertEqual(ds_metrics["match_pct"], 100.0, f"Fallo en {ds_name}")
            self.assertEqual(ds_metrics["orphan_org_ids"], 0, f"Huérfanos en {ds_name}")
        self.assertEqual(data["resource_id_integrity_stream_vs_resources_limpio"]["match_pct"], 100.0)

    def test_batch_and_streaming_totals_match(self):
        batch_rep = run_batch_pipeline()
        stream_rep = run_streaming_pipeline()

        self.assertTrue(batch_rep["idempotency_verification"]["is_idempotent"])
        self.assertTrue(stream_rep["checkpoint_fault_tolerance_and_idempotency"]["is_checkpoint_idempotent"])
        self.assertEqual(batch_rep["silver_and_quarantine_counts"]["usage_events_silver_valid"], 42984)
        self.assertEqual(batch_rep["silver_and_quarantine_counts"]["usage_events_quarantined_negative_cost"], 216)

        cal_stream = stream_rep["watermark_profiles_comparison"]["calibrated_replay_full_1440h_60d"]
        self.assertEqual(cal_stream["accepted_valid_events"], 42984)
        self.assertAlmostEqual(
            batch_rep["gold_marts_summary"]["total_valid_cost_usd"],
            cal_stream["accepted_cost_usd"],
            places=4,
        )

    def test_cassandra_partition_sizing_and_upsert(self):
        srv_rep = run_serving_cassandra()
        self.assertTrue(srv_rep["idempotency_upsert_verification"]["is_idempotent"])
        sizing = {t["table_name"]: t for t in srv_rep["tables_partition_sizing"]}
        self.assertEqual(sizing["cloud_analytics.finops_cost_by_org_day"]["total_rows"], 11050)
        self.assertLessEqual(sizing["cloud_analytics.finops_cost_by_org_day"]["max_rows_per_partition"], 200)


if __name__ == "__main__":
    unittest.main()
