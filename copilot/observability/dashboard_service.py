from datetime import datetime, timedelta, timezone

from copilot.observability.sqlite_store import SQLiteObservabilityStore


class ObservabilityDashboardService:
    """Read-only aggregate queries for the local observability dashboard."""

    def __init__(self, store=None):
        self.store = store or SQLiteObservabilityStore()

    def _cutoff(self, days: int) -> str:
        if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= 90:
            raise ValueError("days must be an integer between 1 and 90")
        return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    def get_summary(self, days: int = 30) -> dict:
        cutoff = self._cutoff(days)

        with self.store.get_connection() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_requests,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_requests,
                    SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) AS unsuccessful_requests,
                    ROUND(AVG(total_request_latency_ms), 2) AS avg_request_latency_ms,
                    ROUND(MIN(total_request_latency_ms), 2) AS min_request_latency_ms,
                    ROUND(MAX(total_request_latency_ms), 2) AS max_request_latency_ms,
                    ROUND(AVG(embedding_latency_ms), 2) AS avg_embedding_latency_ms,
                    ROUND(AVG(pinecone_query_latency_ms), 2) AS avg_pinecone_query_latency_ms,
                    ROUND(AVG(retrieval_latency_ms), 2) AS avg_retrieval_latency_ms,
                    ROUND(AVG(llm_generation_ms), 2) AS avg_llm_generation_ms,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS llm_total_tokens,
                    COALESCE(SUM(embedding_tokens), 0) AS embedding_tokens,
                    ROUND(COALESCE(SUM(estimated_cost_usd), 0), 10) AS total_estimated_cost_usd,
                    COUNT(estimated_cost_usd) AS priced_requests,
                    ROUND(AVG(estimated_cost_usd), 10) AS avg_cost_per_priced_request_usd,
                    COALESCE(SUM(embedding_cost_usd), 0) AS total_embedding_cost_usd,
                    COALESCE(SUM(llm_input_cost_usd), 0) AS total_llm_input_cost_usd,
                    COALESCE(SUM(llm_output_cost_usd), 0) AS total_llm_output_cost_usd
                FROM request_metrics
                WHERE timestamp >= ?
                """,
                (cutoff,),
            ).fetchone()

        result = dict(row)
        total = result["total_requests"] or 0
        successful = result["successful_requests"] or 0
        result["success_rate_percent"] = (
            round(successful * 100.0 / total, 2) if total else 0.0
        )
        result["period_days"] = days
        result["currency"] = "USD"
        return result

    def get_daily_trend(self, days: int = 30) -> list[dict]:
        cutoff = self._cutoff(days)

        with self.store.get_connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    SUBSTR(timestamp, 1, 10) AS date,
                    COUNT(*) AS requests,
                    ROUND(AVG(total_request_latency_ms), 2) AS avg_latency_ms,
                    ROUND(COALESCE(SUM(estimated_cost_usd), 0), 10) AS estimated_cost_usd,
                    COALESCE(SUM(total_tokens), 0) AS llm_tokens
                FROM request_metrics
                WHERE timestamp >= ?
                GROUP BY SUBSTR(timestamp, 1, 10)
                ORDER BY date ASC
                """,
                (cutoff,),
            ).fetchall()

        return [dict(row) for row in rows]

    def get_recent_requests(self, limit: int = 20) -> list[dict]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")

        with self.store.get_connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    request_id,
                    timestamp,
                    status,
                    llm_model,
                    embedding_model,
                    embedding_latency_ms,
                    pinecone_query_latency_ms,
                    retrieval_latency_ms,
                    llm_generation_ms,
                    total_request_latency_ms,
                    embedding_tokens,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    embedding_cost_usd,
                    llm_input_cost_usd,
                    llm_output_cost_usd,
                    estimated_cost_usd,
                    currency
                FROM request_metrics
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(row) for row in rows]

    def get_top_cost_requests(self, days: int = 30, limit: int = 10) -> list[dict]:
        cutoff = self._cutoff(days)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")

        with self.store.get_connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    request_id,
                    timestamp,
                    llm_model,
                    prompt_tokens,
                    completion_tokens,
                    embedding_tokens,
                    estimated_cost_usd
                FROM request_metrics
                WHERE timestamp >= ? AND estimated_cost_usd IS NOT NULL
                ORDER BY estimated_cost_usd DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()

        return [dict(row) for row in rows]
