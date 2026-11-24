import sqlite3
from pathlib import Path


class SQLiteObservabilityStore:
    def __init__(self, db_path=None):
        project_root = Path(__file__).resolve().parents[2]
        self.db_path = Path(
            db_path
            or project_root
            / "data"
            / "observability"
            / "datapilot_observability.db"
        )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize_database()

    def get_connection(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize_database(self):
        with self.get_connection() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS request_metrics (
                    request_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    llm_model TEXT,
                    embedding_model TEXT,
                    embedding_latency_ms REAL,
                    pinecone_query_latency_ms REAL,
                    retrieval_latency_ms REAL,
                    llm_generation_ms REAL,
                    total_request_latency_ms REAL,
                    embedding_tokens INTEGER,
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER,
                    embedding_cost_usd REAL,
                    llm_input_cost_usd REAL,
                    llm_output_cost_usd REAL,
                    estimated_cost_usd REAL,
                    currency TEXT
                )
            """)

            # Backward-compatible migration for existing local databases.
            existing_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(request_metrics)"
                ).fetchall()
            }
            migrations = {
                "embedding_tokens": "INTEGER",
                "embedding_cost_usd": "REAL",
                "llm_input_cost_usd": "REAL",
                "llm_output_cost_usd": "REAL",
                "currency": "TEXT",
            }
            for column, column_type in migrations.items():
                if column not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE request_metrics "
                        f"ADD COLUMN {column} {column_type}"
                    )

            connection.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_request_metrics_timestamp
                ON request_metrics(timestamp)
            """)

    def record_request(self, metrics: dict):
        required_fields = {"request_id", "timestamp", "status"}
        missing_fields = required_fields - metrics.keys()
        if missing_fields:
            raise ValueError(
                f"Missing required metrics: {sorted(missing_fields)}"
            )

        allowed_fields = {
            "request_id", "timestamp", "status", "llm_model",
            "embedding_model", "embedding_latency_ms",
            "pinecone_query_latency_ms", "retrieval_latency_ms",
            "llm_generation_ms", "total_request_latency_ms",
            "embedding_tokens", "prompt_tokens", "completion_tokens",
            "total_tokens", "embedding_cost_usd", "llm_input_cost_usd",
            "llm_output_cost_usd", "estimated_cost_usd", "currency",
        }
        filtered_metrics = {
            key: value for key, value in metrics.items()
            if key in allowed_fields
        }
        columns = list(filtered_metrics.keys())
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)
        query = (
            f"INSERT INTO request_metrics ({column_names}) "
            f"VALUES ({placeholders})"
        )
        with self.get_connection() as connection:
            connection.execute(query, list(filtered_metrics.values()))

    def get_request(self, request_id: str):
        with self.get_connection() as connection:
            row = connection.execute(
                "SELECT * FROM request_metrics WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_recent_requests(self, limit: int = 20):
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Limit must be a positive integer")
        with self.get_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM request_metrics
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
