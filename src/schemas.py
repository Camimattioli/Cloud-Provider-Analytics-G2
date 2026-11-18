# Esquemas explícitos para leer las 8 fuentes en PySpark sin usar inferSchema.
# En usage_events se incluyen carbon_kg y genai_tokens como opcionales (nullable=True)
# para leer juntos los eventos v1 y v2.

SCHEMA_CONTRACTS = {
    "usage_events": [
        ("event_id", "string", False),
        ("timestamp", "string", False),
        ("org_id", "string", False),
        ("resource_id", "string", False),
        ("service", "string", True),
        ("region", "string", True),
        ("metric", "string", True),
        ("value", "double", True),
        ("unit", "string", True),
        ("cost_usd_increment", "double", True),
        ("schema_version", "integer", True),
        ("carbon_kg", "double", True),
        ("genai_tokens", "long", True),
        ("_corrupt_record", "string", True),
    ],
    "customers_orgs": [
        ("org_id", "string", False),
        ("org_name", "string", True),
        ("industry", "string", True),
        ("hq_region", "string", True),
        ("plan_tier", "string", True),
        ("is_enterprise", "boolean", True),
        ("signup_date", "string", True),
        ("sales_rep", "string", True),
        ("lifecycle_stage", "string", True),
        ("marketing_source", "string", True),
        ("nps_score", "double", True),
    ],
    "billing_monthly": [
        ("invoice_id", "string", False),
        ("org_id", "string", False),
        ("month", "string", False),
        ("subtotal", "double", True),
        ("credits", "double", True),
        ("taxes", "double", True),
        ("currency", "string", True),
        ("exchange_rate_to_usd", "double", True),
    ],
    "resources": [
        ("resource_id", "string", False),
        ("org_id", "string", False),
        ("service", "string", True),
        ("region", "string", True),
        ("created_at", "string", True),
        ("state", "string", True),
        ("tags_json", "string", True),
    ],
    "users": [
        ("user_id", "string", False),
        ("org_id", "string", False),
        ("email", "string", True),
        ("role", "string", True),
        ("active", "boolean", True),
        ("created_at", "string", True),
        ("last_login", "string", True),
    ],
    "support_tickets": [
        ("ticket_id", "string", False),
        ("org_id", "string", False),
        ("category", "string", True),
        ("severity", "string", True),
        ("created_at", "string", True),
        ("resolved_at", "string", True),
        ("csat", "double", True),
        ("sla_breached", "boolean", True),
    ],
    "marketing_touches": [
        ("touch_id", "string", False),
        ("org_id", "string", False),
        ("campaign", "string", True),
        ("channel", "string", True),
        ("timestamp", "string", True),
        ("clicked", "boolean", True),
        ("converted", "boolean", True),
    ],
    "nps_surveys": [
        ("org_id", "string", False),
        ("survey_date", "string", False),
        ("nps_score", "double", True),
        ("comment", "string", True),
    ],
}


def get_pyspark_schema(dataset_name):
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    if dataset_name not in SCHEMA_CONTRACTS:
        raise KeyError(f"No hay esquema definido para: {dataset_name}")

    type_map = {
        "string": StringType(),
        "double": DoubleType(),
        "integer": IntegerType(),
        "long": LongType(),
        "boolean": BooleanType(),
    }

    fields = [
        StructField(col_name, type_map[col_type], nullable)
        for col_name, col_type, nullable in SCHEMA_CONTRACTS[dataset_name]
    ]
    return StructType(fields)
