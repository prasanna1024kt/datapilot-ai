# DataPilot AI – RAG Retrieval Evaluation

## 1. Purpose

This document summarizes the available retrieval evaluation for the DataPilot AI Copilot. It separates retrieval performance from generated-answer quality and records limitations that affect interpretation.

## 2. Knowledge-base ingestion evidence

A previously reported live ingestion run recorded:

| Measure | Reported result |
|---|---:|
| Documents loaded | 14 |
| Chunks created | 316 |
| Embeddings generated | 316 |
| Vectors upserted | 316 |
| Dry run | `false` |
| Pinecone index | `datapilot-ai-knowledge` |
| Dimension | 1536 |
| Namespace | `datapilot-ai` |

These values describe that ingestion run, not a guarantee of the current remote index contents. Confirm current Pinecone statistics before using them as live-state evidence.

## 3. Retrieval experiment design

The previously reported benchmark used 16 questions and compared three retrieval strategies:

- Similarity retrieval.
- Maximum Marginal Relevance (MMR).
- Multi-Query retrieval.

The reported experiment evaluated retrieval using measures including recall/Hit@5, precision@5, and mean reciprocal rank (MRR). The benchmark is small and should be treated as an engineering comparison rather than a statistically conclusive evaluation.

## 4. Reported findings

The previously shared results indicated:

- Multi-Query achieved 100% recall in that 16-question experiment.
- Multi-Query precision@5 was 25% in that experiment.
- MMR achieved the highest reported MRR, 0.825.
- A threshold sweep found that a 0.70 similarity threshold returned no result for 14 questions, while 0.40 retained full Hit@5 in the reported sweep.

These findings show a tradeoff between retrieving relevant evidence and ranking a compact set of useful chunks. They do not establish that one strategy is universally best.

## 5. Interpretation

- **Recall:** Multi-Query retrieved at least one relevant result for each benchmark question in the reported run. Its lower precision@5 indicates that the top-five result set also contained non-relevant chunks.
- **Ranking:** MMR produced the strongest reported MRR, meaning relevant material tended to appear earlier in the ranked results for this benchmark.
- **Thresholding:** A high threshold can suppress useful results. Threshold selection should be based on a versioned evaluation set and the desired balance between abstention and retrieval coverage.

The results should be re-run with the same dataset, expected-source labels, retriever settings, and metric implementation before making a new strategy decision.

## 6. Important limitations

The available evaluation notes identify these limitations:

1. Retrieval latency and cost were not measured.
2. Multi-Query was compared at retrieval level; generated answers were not evaluated in that run.
3. Earlier RAGAS results were from a separate evaluation and cannot be attributed to Multi-Query.
4. The live API retriever configuration must be aligned with the evaluator configuration before production comparisons.
5. Faithfulness, answer relevancy, context precision, and context recall require a consistent RAGAS evaluation run.
6. The evaluation dataset and expected-source labels should be versioned, with explicit regression criteria.

## 7. Recommended next evaluation

For the next evaluation cycle:

- Version the 16-question dataset and expected source labels.
- Record chunker version, embedding model, index configuration, namespace, retrieval strategy, top-k, threshold, and MMR parameters.
- Align evaluator and live API settings.
- Report per-question retrieval results as well as aggregate metrics.
- Measure retrieval latency and token/API cost.
- Evaluate generated answers separately using a fixed answer-quality protocol and RAGAS metrics where appropriate.
- Define minimum regression thresholds before changing the production/default retrieval strategy.

## 8. Conclusion

The available experiment provides useful comparative evidence: Multi-Query favored recall, while MMR produced the best reported ranking metric. The experiment is not sufficient on its own to select a production strategy because cost, latency, generated-answer quality, and regression behavior were not fully measured. Keep the current strategy choice explicit and evidence-driven rather than presenting the benchmark as a completed production optimization.
