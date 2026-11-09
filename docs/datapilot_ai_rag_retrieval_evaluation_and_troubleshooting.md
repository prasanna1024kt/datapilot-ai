# DataPilot AI --- RAG Retrieval Evaluation, Troubleshooting and Improvements

**Document type:** Engineering implementation and troubleshooting
record\
**Component:** DataPilot AI Copilot --- Retrieval-Augmented Generation
(RAG)\
**Evaluation milestone:** Multi-Query Retrieval v1\
**Evaluation date:** 9 November 2026\
**Status:** Retrieval evaluation completed successfully; production
strategy selection remains open

------------------------------------------------------------------------

## 1. Purpose

This document records the RAG retrieval evaluation work completed for
DataPilot AI, including:

-   Evaluation design, metrics and configuration
-   Retrieval problems observed during testing
-   Troubleshooting and corrective actions
-   Similarity threshold experiments
-   Similarity, Maximal Marginal Relevance (MMR), and Multi-Query
    strategy comparison
-   Results, interpretation, limitations and recommended next actions

The purpose is to make the work reproducible and explain the engineering
decisions made while improving retrieval quality.

## 2. Scope and current implementation

The knowledge base contains 14 Markdown documents covering architecture,
agents, pipelines, operations and MCP tools. The evaluation dataset
contains 16 questions (RAG001--RAG016), each with expected source
documents.

The retriever uses:

-   OpenAI embeddings: `text-embedding-3-small`
-   Embedding dimension: 1536
-   Vector database: Pinecone
-   Pinecone index: `datapilot-ai-knowledge`
-   Namespace: `datapilot-ai`
-   Candidate retrieval size: 20 chunks
-   Final document result size: 5

The retrieval evaluator compares three strategies:

1.  **Similarity:** vector similarity ranking.
2.  **MMR:** diversity-aware retrieval, configured with
    `mmr_lambda=0.70`.
3.  **Multi-Query:** deterministic query expansion, multiple vector
    searches and Reciprocal Rank Fusion (RRF).

The evaluation also includes a baseline candidate retrieval, a
production-threshold simulation and a threshold sweep.

> **Scope limitation:** This is a retrieval evaluation. It does not by
> itself establish answer-generation quality, end-to-end Copilot
> quality, production latency, or production readiness.

## 3. Evaluation design

### 3.1 Dataset

The evaluation set contains 16 questions:

  IDs              Topic
  ---------------- ---------------------------------------------
  RAG001           Architecture
  RAG002--RAG006   Agent responsibilities and interactions
  RAG007--RAG010   Data pipeline and Spark processing
  RAG011           Remediation approval
  RAG012           MCP tools
  RAG013--RAG014   Configuration and deployment
  RAG015           Agent interactions and remediation controls
  RAG016           Docker services and runtime configuration

Each test case contains a question, category and `expected_sources`. The
evaluator compares retrieved document IDs against these expected source
IDs.

### 3.2 Retrieval configuration

  Parameter                                                  Value
  --------------------------------- ------------------------------
  `CANDIDATE_K`                                                 20
  `DOCUMENT_K`                                                   5
  Experiment threshold                                        0.40
  Production simulation threshold                             0.70
  MMR lambda                                                  0.70
  Threshold sweep                     0.30, 0.40, 0.50, 0.60, 0.70

Similarity and MMR use the experiment threshold of 0.40. Multi-Query
also uses 0.40 and limits results to one chunk per document.

### 3.3 Metrics

The evaluator calculates metrics at document level. Retrieved chunks are
deduplicated by `document_id`, preserving retrieval order, before the
top five unique documents are scored.

-   **Hit@5:** 1 if at least one expected source appears in the top
    five; otherwise 0.
-   **MRR:** reciprocal rank of the first relevant document.
-   **Precision@5:** relevant retrieved documents divided by five. The
    denominator is fixed even if fewer than five documents are returned.
-   **Recall@5:** relevant retrieved documents divided by the number of
    expected sources.
-   **Average similarity:** average similarity score across the
    evaluated top documents.
-   **Duplicate document rate:** proportion of returned chunks that
    repeat a document ID.
-   **Questions with no results:** number of questions with zero
    evaluated documents.
-   **Average evaluated documents:** average number of unique documents
    scored per question.

**Interpretation caution:** Hit@5 is a coarse metric: it does not show
how many expected sources were retrieved. MRR measures the position of
the first relevant source, while precision and recall provide additional
coverage information. Duplicate rate is affected by each strategy's
deduplication behavior.

## 4. Problems observed and troubleshooting

### 4.1 High similarity threshold caused retrieval failures

**Observed problem**

The production simulation used a similarity threshold of 0.70. It
returned no documents for 14 of the 16 evaluation questions.

**Measured result at threshold 0.70**

-   Hit@5: 12.50%
-   MRR: 0.1250
-   Precision@5: 2.50%
-   Recall@5: 12.50%
-   Questions with no results: 14 of 16

**Troubleshooting and action**

A threshold sweep was run using the same top-20 candidate chunks. Each
threshold was applied before document-level deduplication and metric
calculation.

**Outcome**

Thresholds 0.30 and 0.40 retained full Hit@5 and the highest measured
recall in the sweep. Thresholds 0.50, 0.60 and 0.70 progressively
reduced retrieval coverage.

**Decision**

Use 0.40 as the current experimental threshold. Do not treat this as a
universally optimal threshold; it is supported by the current
16-question dataset and should be revalidated as the knowledge base and
question set grow.

### 4.2 Repeated chunks from the same document

**Observed problem**

Similarity retrieval sometimes returned multiple chunks from the same
source document. This reduced the number of distinct documents available
in the final context.

**Action**

The evaluation calculates document-level metrics after deduplicating by
`document_id`. MMR also applies diversity-aware selection. Multi-Query
is configured with `max_chunks_per_document=1`.

**Outcome**

The final duplicate document rate was:

-   Similarity: 41.88%
-   MMR: 0%
-   Multi-Query: 0%

**Interpretation**

The zero duplicate rate for MMR and Multi-Query is partly a consequence
of their document-diversity controls. It should not be interpreted in
isolation as proof of better relevance.

### 4.3 Single-query retrieval missed a relevant source for RAG015

**Observed problem**

For the question "How are agent interactions and remediation
controlled?", single-query similarity retrieval did not include
`operations/remediation-workflow.md` in its final five results during
the earlier focused test.

**Action: Multi-Query Retrieval v1**

A deterministic query expansion mechanism was introduced:

-   Always include the original question.
-   For agent, supervisor, RCA or root-cause questions, add an
    agent-interaction query.
-   For remediation, approval, quarantine or execution questions, add a
    remediation-workflow query.

Each query is embedded, searched independently in Pinecone, filtered by
the similarity threshold and merged by chunk ID. Reciprocal Rank Fusion
is used to combine rankings. The implementation tracks the number of
query lists in which a chunk appears (`query_matches`) and retains the
maximum similarity score.

**RAG015 focused test outcome**

The Multi-Query result set included both expected source documents:

-   `architecture/agent-interactions.md`
-   `operations/remediation-workflow.md`

The result contained five unique documents, with no duplicate document
IDs.

**Limitation**

This focused test established improved source coverage for RAG015. It
did not establish improved answer faithfulness or latency.

### 4.4 Retriever dispatch inefficiency identified during implementation

An earlier version of the retriever dispatched to Multi-Query after
executing the normal single-query embedding and search path. This could
cause an unnecessary additional embedding/search operation.

**Corrective action**

The Multi-Query strategy was moved to an early dispatch path in
`retrieve()`, before the normal single-query embedding and Pinecone
search.

**Validation status**

The later Multi-Query test returned the expected expanded-query
retrieval results. Re-run unit or integration tests if the retriever
dispatch code is changed again.

### 4.5 Production configuration alignment

The evaluator's production simulation uses threshold 0.70. A prior
Copilot API configuration used threshold 0.40 during validation.

**Risk**

If the live API is configured at 0.40 while the evaluator simulates
0.70, the production simulation does not represent the live API.

**Action required**

Before reporting a production benchmark:

1.  Inspect the actual `RAGRetriever` initialization in the Copilot API.
2.  Record its `top_k`, `candidate_k`, threshold, strategy and MMR
    parameters.
3.  Make the evaluator's production configuration match those values.
4.  Record the configuration in the generated report.

The 0.70 result in this document is therefore a threshold experiment
unless it is confirmed to match the live API configuration at the time
of evaluation.

## 5. Threshold sweep results

The threshold sweep used the same top-20 candidate chunks for each
threshold.

  -----------------------------------------------------------------------------------
    Threshold      Hit@5        MRR   Precision@5   Recall@5      Average   Questions
                                                               similarity     with no
                                                                              results
  ----------- ---------- ---------- ------------- ---------- ------------ -----------
         0.30    100.00%     0.8177        23.75%     96.88%       0.5363           0

         0.40    100.00%     0.8177        23.75%     96.88%       0.5447           0

         0.50     81.25%     0.6302        16.25%     73.96%       0.4679           3

         0.60     43.75%     0.4062         8.75%     40.62%       0.3962           6

         0.70     12.50%     0.1250         2.50%     12.50%       0.0939          14
  -----------------------------------------------------------------------------------

### Threshold decision

Threshold 0.40 is the current experimental setting because it preserves
the full Hit@5 score and high recall while producing a slightly higher
average similarity than 0.30 in this sweep.

Threshold 0.30 and 0.40 have identical Hit@5, MRR, precision and recall
in this run. More evaluation questions are needed to distinguish them
reliably.

## 6. Strategy comparison results

The full strategy comparison ran all three retrieval strategies against
all 16 evaluation questions.

  Metric                          Similarity          MMR   Multi-Query
  ----------------------------- ------------ ------------ -------------
  Hit@5                              100.00%      100.00%       100.00%
  MRR                                 0.8177   **0.8250**        0.7865
  Precision@5                         22.50%       23.75%    **25.00%**
  Recall@5                            94.79%       96.88%   **100.00%**
  Average similarity                  0.5685       0.5403    **0.6057**
  Duplicate document rate             41.88%           0%            0%
  Questions with no results                0            0             0
  Average evaluated documents         2.8125       4.3125        4.5625

### Interpretation

**Similarity**

-   Achieved 100% Hit@5.
-   MRR was 0.8177.
-   Had the highest duplicate document rate.
-   Returned fewer unique documents on average than MMR and Multi-Query.

**MMR**

-   Achieved the highest MRR at 0.8250.
-   Achieved 96.88% recall.
-   Returned diverse documents and had no duplicate document IDs in the
    final result set.
-   Provides a strong balance of ranking and diversity in this
    experiment.

**Multi-Query**

-   Achieved the highest recall at 100%.
-   Achieved the highest precision at 25%.
-   Had the highest average similarity at 0.6057.
-   MRR was 0.7865, lower than both Similarity and MMR.
-   Returned the highest average number of evaluated documents.

### Strategy decision

Do not select a universal production winner based only on this
experiment.

A reasonable next experiment is:

-   MMR for general-purpose retrieval.
-   Multi-Query for complex questions that span multiple topics or
    source documents.
-   Similarity as a simple baseline and possible low-cost path for
    direct questions.

This is a proposed routing approach for testing, not an implemented
production routing policy.

## 7. Actions taken to improve evaluation and retrieval

The following changes were completed or validated during this evaluation
cycle:

1.  Created and maintained a 16-question retrieval evaluation dataset
    with expected source documents.
2.  Implemented document-level deduplication before calculating top-five
    metrics.
3.  Added Hit@5, MRR, Precision@5, Recall@5, average similarity and
    duplicate document rate.
4.  Added a threshold sweep from 0.30 through 0.70.
5.  Compared Similarity and MMR using the same evaluation questions and
    configuration.
6.  Implemented deterministic Multi-Query expansion for agent and
    remediation topics.
7.  Added Reciprocal Rank Fusion to merge Multi-Query results.
8.  Added a per-document result limit for Multi-Query.
9.  Added Multi-Query to the 16-question strategy comparison and JSON
    report.
10. Reused the production retriever instance rather than recreating it
    for every question.
11. Executed the evaluator successfully and parsed the generated JSON
    report.

## 8. Validation evidence

The completed run reported:

-   Total questions: 16.
-   Strategies: `similarity`, `mmr`, `multi_query`.
-   Evaluation completed.
-   Report saved to:

``` text
/app/copilot/rag/evaluation/reports/retrieval_evaluation.json
```

The JSON validation command successfully loaded the report and printed
the three strategy summaries.

### Reproduction commands

Run from the DataPilot AI project environment:

``` bash
docker-compose exec copilot python -m py_compile \
  /app/copilot/rag/evaluation/retrieval_evaluator.py
```

Run the evaluation:

``` bash
docker-compose exec copilot \
  python -m copilot.rag.evaluation.retrieval_evaluator
```

Validate and print the strategy summaries:

``` bash
docker-compose exec copilot python -c "
import json
p='/app/copilot/rag/evaluation/reports/retrieval_evaluation.json'
d=json.load(open(p))
print('Questions:', d['total_questions'])
print('Strategies:', d['strategy_comparison']['configuration']['strategies'])
for name, metrics in d['strategy_comparison']['summary'].items():
    print(name, metrics)
"
```

## 9. Known limitations

1.  The evaluation contains 16 questions and 14 knowledge-base
    documents. Results may change as either grows.
2.  Expected-source labels determine relevance. Incorrect or incomplete
    labels can distort recall and precision.
3.  Document-level evaluation does not assess whether the retrieved
    chunks contain enough detail to answer the question.
4.  The evaluator does not currently measure retrieval latency,
    embedding cost, Pinecone request count or token consumption.
5.  Multi-Query has only been compared on retrieval metrics here. Its
    generated answers have not been evaluated in this run.
6.  The RAGAS evaluation performed earlier is a separate evaluation run.
    Its scores should not be attributed to Multi-Query because
    Multi-Query was not included in that reported RAGAS comparison.
7.  The evaluator's production threshold must be aligned with the live
    API before its production simulation is treated as representative.
8.  Zero duplicate rate is influenced by strategy-level document
    diversity controls.
9.  A 100% Hit@5 score does not mean all expected sources were
    retrieved. Recall and precision must also be considered.
10. This evaluation does not establish that every generated answer is
    grounded or free from hallucinations.

## 10. Next steps

### Priority 1 --- Configuration and reproducibility

-   Align evaluator production settings with the live Copilot API.
-   Record retriever settings in every evaluation report.
-   Version the evaluation dataset and expected-source labels.

### Priority 2 --- Latency and cost

Measure each strategy with `time.perf_counter()` around the complete
`retrieve()` call. Record per-question latency and aggregate average,
median and p95 latency. Also record the number of expanded queries and
Pinecone searches for Multi-Query.

Run repeated trials to reduce the effect of network and service
variability.

### Priority 3 --- RAGAS answer-quality evaluation

Add Multi-Query as a retrieval strategy in the answer-generation
evaluation and compare:

-   Faithfulness
-   Answer relevancy
-   Context precision
-   Context recall

Use the same question set, generation model, prompt and evaluation
configuration for a fair comparison. Keep the RAGAS report separate from
the retrieval-only report.

### Priority 4 --- Retrieval improvements

-   Analyze questions where MRR is lower for Multi-Query.
-   Inspect whether RRF ranking places the most useful source too low.
-   Evaluate query expansion quality and unnecessary expansions.
-   Add questions covering ambiguous, multi-hop and operational
    troubleshooting requests.
-   Consider selective Multi-Query routing only after latency and
    answer-quality evaluation.

### Priority 5 --- Regression and release criteria

Create a repeatable regression run whenever the following change:

-   Knowledge-base documents or chunking
-   Embedding model or embedding configuration
-   Pinecone index or metadata
-   Similarity threshold
-   Retrieval strategy or ranking logic
-   Prompt or answer-generation logic

Define release criteria using a combination of recall, MRR, precision,
faithfulness, latency and cost rather than a single metric.

## 11. Final engineering summary

The DataPilot AI retrieval evaluation now has a reproducible 16-question
benchmark, a threshold sweep and a three-strategy comparison. The 0.70
threshold performed poorly on this dataset, while 0.40 retained much
stronger retrieval coverage. Multi-Query v1 improved source coverage and
achieved 100% recall in the strategy comparison, while MMR achieved the
best MRR.

The next engineering milestone is to align production configuration,
measure retrieval latency and cost, and evaluate Multi-Query answer
quality with RAGAS before selecting a production strategy.

------------------------------------------------------------------------

**Evidence basis:** This record reflects the implementation details,
command output and metric results shared during the DataPilot AI
evaluation work on 9 November 2026. It distinguishes completed
validation from proposed next steps and does not claim unmeasured
latency, cost or production performance.
