-- 90_results.sql — both pipelines side by side for the report (table queries over everything logged so far).
SELECT use_case, qtype, backend, model, count() AS n,
       round(count_if(decision = expected) / count_if(err = ''), 3) AS accuracy,
       round(count_if(err != '') / count(), 3) AS error_rate,
       quantile(0.5)(latency_ms) AS p50_ms, quantile(0.99)(latency_ms) AS p99_ms,
       round(avg(state_chars)) AS avg_state_chars
FROM (
  SELECT * FROM table(qoe.decision_log)
  UNION ALL
  SELECT * FROM table(ddos.decision_log)
)
GROUP BY use_case, qtype, backend, model
ORDER BY use_case, qtype;
