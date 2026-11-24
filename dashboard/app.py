
import os
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode

import streamlit as st
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

API_BASE_URL = os.getenv(
    "DATAPILOT_API_URL",
    "http://localhost:8002"
)

st.set_page_config(
    page_title="DataPilot AI | Observability",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# API CLIENT
# ============================================================

def api_get(endpoint, params=None):
    url = f"{API_BASE_URL}{endpoint}"

    if params:
        url += "?" + urlencode(params)

    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))

    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"API HTTP error {exc.code}: {exc.reason}"
        ) from exc

    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Cannot connect to FastAPI at {API_BASE_URL}: "
            f"{exc.reason}"
        ) from exc

    except TimeoutError as exc:
        raise RuntimeError("API request timed out") from exc


# ============================================================
# FORMATTING
# ============================================================

def number(value, decimals=2):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:,.{decimals}f}"


def currency(value):
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.6f}"


def latency(value):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:,.2f} ms"


def dataframe_from_records(records):
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("📊 DataPilot AI")
st.sidebar.caption("AI Data Engineering Platform")
st.sidebar.divider()

period_days = st.sidebar.selectbox(
    "Reporting Period",
    options=[7, 30, 60, 90],
    index=1,
    format_func=lambda value: f"Last {value} days",
)

request_limit = st.sidebar.selectbox(
    "Request History",
    options=[20, 50, 100],
    index=2,
)

st.sidebar.divider()

if st.sidebar.button(
    "🔄 Refresh Dashboard",
    use_container_width=True,
):
    st.rerun()

st.sidebar.divider()

st.sidebar.caption("Backend Configuration")
st.sidebar.code(API_BASE_URL)

st.sidebar.caption(
    "Data source: FastAPI + SQLite"
)


# ============================================================
# HEADER
# ============================================================

st.title("📊 DataPilot AI Observability Dashboard")

st.caption(
    "Monitor API performance, RAG pipeline latency, "
    "token consumption, estimated costs and request health."
)

st.divider()


# ============================================================
# LOAD API DATA
# ============================================================

try:
    summary = api_get(
        "/api/v1/observability/summary",
        {"days": period_days},
    )

    trends = api_get(
        "/api/v1/observability/trends",
        {"days": period_days},
    )

    requests_data = api_get(
        "/api/v1/observability/requests",
        {"limit": request_limit},
    )

    top_cost_data = api_get(
        "/api/v1/observability/top-cost",
        {"days": period_days, "limit": 10},
    )

except Exception as exc:
    st.error(f"Dashboard data loading failed: {exc}")
    st.info(
        "Verify that FastAPI is running on port 8002 "
        "and all observability endpoints are available."
    )
    st.stop()


requests_df = dataframe_from_records(requests_data)
trends_df = dataframe_from_records(trends)
top_cost_df = dataframe_from_records(top_cost_data)


# ============================================================
# PREPARE DATA
# ============================================================

if not requests_df.empty:
    requests_df["timestamp"] = pd.to_datetime(
        requests_df["timestamp"],
        errors="coerce",
        utc=True,
    )

    numeric_columns = [
        "total_request_latency_ms",
        "embedding_latency_ms",
        "pinecone_query_latency_ms",
        "retrieval_latency_ms",
        "llm_generation_ms",
        "embedding_tokens",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "embedding_cost_usd",
        "llm_input_cost_usd",
        "llm_output_cost_usd",
        "estimated_cost_usd",
    ]

    for column in numeric_columns:
        if column in requests_df.columns:
            requests_df[column] = pd.to_numeric(
                requests_df[column],
                errors="coerce",
            )

if not trends_df.empty:
    trends_df["date"] = pd.to_datetime(
        trends_df["date"],
        errors="coerce",
    )
    trends_df = trends_df.sort_values("date")

if not top_cost_df.empty:
    top_cost_df["estimated_cost_usd"] = pd.to_numeric(
        top_cost_df["estimated_cost_usd"],
        errors="coerce",
    )


# ============================================================
# 1. SYSTEM OVERVIEW
# ============================================================

st.header("1. System Overview")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Requests",
    f"{summary.get('total_requests', 0):,}",
)

col2.metric(
    "Success Rate",
    f"{number(summary.get('success_rate_percent', 0))}%",
)

col3.metric(
    "Average Latency",
    latency(summary.get("avg_request_latency_ms")),
)

col4.metric(
    "Estimated Total Cost",
    currency(summary.get("total_estimated_cost_usd")),
)

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Successful Requests",
    summary.get("successful_requests") or 0,
)

col2.metric(
    "Failed Requests",
    summary.get("unsuccessful_requests") or 0,
)

col3.metric(
    "Minimum Latency",
    latency(summary.get("min_request_latency_ms")),
)

col4.metric(
    "Maximum Latency",
    latency(summary.get("max_request_latency_ms")),
)


# ============================================================
# 2. PERFORMANCE ANALYTICS
# ============================================================

st.divider()
st.header("2. Performance Analytics")

st.subheader("RAG Pipeline Latency Breakdown")

latency_data = {
    "Component": [
        "Embedding",
        "Pinecone Query",
        "Retrieval",
        "LLM Generation",
    ],
    "Average Latency (ms)": [
        summary.get("avg_embedding_latency_ms"),
        summary.get("avg_pinecone_query_latency_ms"),
        summary.get("avg_retrieval_latency_ms"),
        summary.get("avg_llm_generation_ms"),
    ],
}

latency_df = pd.DataFrame(latency_data)
latency_df["Average Latency (ms)"] = pd.to_numeric(
    latency_df["Average Latency (ms)"],
    errors="coerce",
)

latency_df = latency_df.dropna()

if not latency_df.empty:
    st.bar_chart(
        latency_df.set_index("Component"),
        y="Average Latency (ms)",
    )
else:
    st.info("No latency breakdown is available.")

st.subheader("Daily Performance Trends")

if not trends_df.empty:
    trend_col1, trend_col2 = st.columns(2)

    with trend_col1:
        st.markdown("**Daily Request Volume**")
        st.line_chart(
            trends_df.set_index("date")[["requests"]]
        )

    with trend_col2:
        st.markdown("**Average Request Latency**")
        st.line_chart(
            trends_df.set_index("date")[["avg_latency_ms"]]
        )

    st.markdown("**Daily Trend Data**")

    trend_display = trends_df.copy()
    trend_display["date"] = trend_display["date"].dt.strftime(
        "%Y-%m-%d"
    )

    st.dataframe(
        trend_display,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No daily performance trend data available.")


# ============================================================
# 3. COST ANALYTICS
# ============================================================

st.divider()
st.header("3. Cost Analytics")

cost_col1, cost_col2, cost_col3 = st.columns(3)

cost_col1.metric(
    "Embedding Cost",
    currency(
        summary.get("total_embedding_cost_usd")
    ) if summary.get("total_embedding_cost_usd") is not None
    else "Not available",
)

cost_col2.metric(
    "LLM Input Cost",
    currency(
        summary.get("total_llm_input_cost_usd")
    ) if summary.get("total_llm_input_cost_usd") is not None
    else "Not available",
)

cost_col3.metric(
    "LLM Output Cost",
    currency(
        summary.get("total_llm_output_cost_usd")
    ) if summary.get("total_llm_output_cost_usd") is not None
    else "Not available",
)

st.caption(
    "Component cost totals are displayed only if the summary API "
    "provides them. Otherwise, the request-level breakdown below "
    "is calculated from the returned request history."
)

if not requests_df.empty:
    component_columns = [
        "embedding_cost_usd",
        "llm_input_cost_usd",
        "llm_output_cost_usd",
    ]

    available_cost_columns = [
        column
        for column in component_columns
        if column in requests_df.columns
    ]

    if available_cost_columns:
        cost_breakdown = requests_df[
            available_cost_columns
        ].sum(min_count=1)

        cost_labels = {
            "embedding_cost_usd": "Embedding",
            "llm_input_cost_usd": "LLM Input",
            "llm_output_cost_usd": "LLM Output",
        }

        cost_chart_df = pd.DataFrame({
            "Component": [
                cost_labels[column]
                for column in cost_breakdown.index
            ],
            "Cost (USD)": cost_breakdown.values,
        }).dropna()

        if not cost_chart_df.empty:
            st.subheader(
                "Cost Breakdown — Returned Request History"
            )

            st.bar_chart(
                cost_chart_df.set_index("Component"),
                y="Cost (USD)",
            )

            st.caption(
                f"Based on the latest {len(requests_df)} "
                "requests returned by the API, not necessarily "
                "the entire reporting period."
            )

if not top_cost_df.empty:
    st.subheader("Top 10 Expensive Requests")

    st.dataframe(
        top_cost_df,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No requests have calculated costs yet.")

if not trends_df.empty and "estimated_cost_usd" in trends_df:
    st.subheader("Daily Estimated Cost Trend")

    cost_trend = trends_df[
        ["date", "estimated_cost_usd"]
    ].dropna()

    if not cost_trend.empty:
        st.line_chart(
            cost_trend.set_index("date")
        )


# ============================================================
# 4. TOKEN ANALYTICS
# ============================================================

st.divider()
st.header("4. Token Usage Analytics")

token_col1, token_col2, token_col3, token_col4 = st.columns(4)

token_col1.metric(
    "Embedding Tokens",
    f"{summary.get('embedding_tokens', 0):,}",
)

token_col2.metric(
    "LLM Input Tokens",
    f"{summary.get('prompt_tokens', 0):,}",
)

token_col3.metric(
    "LLM Output Tokens",
    f"{summary.get('completion_tokens', 0):,}",
)

token_col4.metric(
    "Total LLM Tokens",
    f"{summary.get('llm_total_tokens', 0):,}",
)

if not trends_df.empty:
    token_trend = trends_df[
        ["date", "llm_tokens"]
    ].dropna()

    if not token_trend.empty:
        st.subheader("Daily LLM Token Consumption")

        st.bar_chart(
            token_trend.set_index("date")
        )


# ============================================================
# 5. REQUEST EXPLORER
# ============================================================

st.divider()
st.header("5. Request Explorer")

if not requests_df.empty:

    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        statuses = sorted(
            requests_df["status"].dropna().unique().tolist()
        ) if "status" in requests_df else []

        selected_status = st.multiselect(
            "Request Status",
            options=statuses,
            default=statuses,
        )

    with filter_col2:
        models = sorted(
            requests_df["llm_model"].dropna().unique().tolist()
        ) if "llm_model" in requests_df else []

        selected_models = st.multiselect(
            "LLM Model",
            options=models,
            default=models,
        )

    with filter_col3:
        max_latency = st.number_input(
            "Minimum latency filter (ms)",
            min_value=0.0,
            value=0.0,
            step=100.0,
        )

    filtered_df = requests_df.copy()

    if "status" in filtered_df:
        filtered_df = filtered_df[
            filtered_df["status"].isin(selected_status)
        ]

    if "llm_model" in filtered_df:
        filtered_df = filtered_df[
            filtered_df["llm_model"].isin(selected_models)
            | filtered_df["llm_model"].isna()
        ]

    if "total_request_latency_ms" in filtered_df:
        filtered_df = filtered_df[
            filtered_df["total_request_latency_ms"].fillna(-1)
            >= max_latency
        ]

    st.metric(
        "Matching Requests",
        len(filtered_df),
    )

    display_columns = [
        "request_id",
        "timestamp",
        "status",
        "llm_model",
        "embedding_model",
        "total_request_latency_ms",
        "embedding_latency_ms",
        "pinecone_query_latency_ms",
        "retrieval_latency_ms",
        "llm_generation_ms",
        "prompt_tokens",
        "completion_tokens",
        "estimated_cost_usd",
        "currency",
    ]

    available_columns = [
        column
        for column in display_columns
        if column in filtered_df.columns
    ]

    st.dataframe(
        filtered_df[available_columns],
        use_container_width=True,
        hide_index=True,
    )

    csv_data = filtered_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "⬇️ Download Filtered Requests CSV",
        data=csv_data,
        file_name="datapilot_observability_requests.csv",
        mime="text/csv",
    )

else:
    st.info("No request history is available.")


# ============================================================
# 6. PERFORMANCE INSIGHTS AND DATA QUALITY
# ============================================================

st.divider()
st.header("6. Performance Insights and Data Quality")

if not requests_df.empty:

    total_history = len(requests_df)

    required_latency_fields = [
        "embedding_latency_ms",
        "pinecone_query_latency_ms",
        "retrieval_latency_ms",
        "llm_generation_ms",
    ]

    available_latency_fields = [
        column
        for column in required_latency_fields
        if column in requests_df.columns
    ]

    if available_latency_fields:
        fully_instrumented = requests_df[
            available_latency_fields
        ].notna().all(axis=1)

        # A recorded zero is not automatically treated as missing.
        # The presence of all four values indicates completeness.
        instrumented_count = int(fully_instrumented.sum())
    else:
        instrumented_count = 0

    priced_count = (
        int(requests_df["estimated_cost_usd"].notna().sum())
        if "estimated_cost_usd" in requests_df
        else 0
    )

    quality_col1, quality_col2, quality_col3 = st.columns(3)

    quality_col1.metric(
        "Requests in History",
        total_history,
    )

    quality_col2.metric(
        "Fully Instrumented Requests",
        instrumented_count,
    )

    quality_col3.metric(
        "Requests with Cost Data",
        priced_count,
    )

    st.subheader("Data Completeness")

    st.write(
        f"Latency instrumentation coverage: "
        f"{instrumented_count}/{total_history}"
    )

    st.progress(
        instrumented_count / total_history
        if total_history else 0
    )

    st.write(
        f"Cost calculation coverage: "
        f"{priced_count}/{total_history}"
    )

    st.progress(
        priced_count / total_history
        if total_history else 0
    )

    st.subheader("Slow Request Analysis")

    if "total_request_latency_ms" in requests_df:
        slow_requests = requests_df.dropna(
            subset=["total_request_latency_ms"]
        ).sort_values(
            "total_request_latency_ms",
            ascending=False,
        ).head(5)

        if not slow_requests.empty:
            st.markdown("**Top 5 slowest requests**")

            slow_columns = [
                "request_id",
                "timestamp",
                "total_request_latency_ms",
                "retrieval_latency_ms",
                "llm_generation_ms",
                "llm_model",
            ]

            slow_columns = [
                column
                for column in slow_columns
                if column in slow_requests.columns
            ]

            st.dataframe(
                slow_requests[slow_columns],
                use_container_width=True,
                hide_index=True,
            )

    st.subheader("Latency Bottleneck")

    if not latency_df.empty:
        bottleneck = latency_df.sort_values(
            "Average Latency (ms)",
            ascending=False,
        ).iloc[0]

        st.info(
            f"Highest average measured stage latency: "
            f"{bottleneck['Component']} "
            f"({bottleneck['Average Latency (ms)']:.2f} ms)."
        )

    if priced_count < total_history:
        st.warning(
            f"{total_history - priced_count} requests in the "
            "returned history do not have calculated cost data. "
            "Missing values are not treated as zero."
        )

    if instrumented_count < total_history:
        st.warning(
            f"{total_history - instrumented_count} requests do not "
            "contain all four latency-stage measurements. "
            "Check whether they used a different API execution path."
        )

else:
    st.info("Not enough request data to generate insights.")


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "DataPilot AI | Observability Dashboard | "
    "FastAPI + SQLite + Streamlit"
)

st.caption(
    "Cost figures are estimates based on configured model pricing. "
    "They are not provider billing records."
)
