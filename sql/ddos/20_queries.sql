-- ddos/20_queries.sql — what to put on screen for the ddos pipeline. All streaming unless marked table().
-- Everything is read from database `ddos`; the other pipeline has the same queries against its own database,
-- and sql/90_results.sql joins both logs for the report.

-- A. Live feed of decisions as they happen.
SELECT _tp_time, cohort, qtype, decision, expected, decision = expected AS correct,
       round(p, 2) AS p, rule_flag, truth, latency_ms, model, err
FROM ddos.decision_log;

-- B. Per-minute scorecard: model vs rule, auto-decide share, latency.
--    (rule accuracy = did the threshold alert agree with "something is wrong"; model accuracy = exact answer)
SELECT
  window_start, qtype,
  count()                                                               AS n,
  count_if(err != '')                                                   AS failed,
  round(count_if(decision = expected) / count_if(err = ''), 3)          AS model_accuracy,
  round(count_if(rule_flag = (truth NOT IN ('none','flash_crowd'))) / count(), 3) AS rule_accuracy,
  round(count_if(p >= 0.85) / count_if(err = ''), 3)                    AS auto_share,
  round(count_if(p >= 0.85 AND decision = expected) / count_if(p >= 0.85), 3) AS auto_accuracy,
  quantile(0.5)(latency_ms)                                             AS p50_ms,
  quantile(0.99)(latency_ms)                                            AS p99_ms
FROM tumble(ddos.decision_log, 1m)
GROUP BY window_start, qtype;

-- C. Confusion by scenario (sliding 10 min): what the model answered for each true scenario.
SELECT window_end, qtype, truth, decision, count() AS n
FROM hop(ddos.decision_log, 10s, 10m)
WHERE err = ''
GROUP BY window_end, qtype, truth, decision;

-- D. The designed rule gap(s), live: flash_crowd, port_scan (rule false alarm and rule miss)
SELECT window_end, truth,
       count()                                           AS windows,
       count_if(rule_flag)                               AS rule_alerted,
       count_if(decision IN ('legitimate_surge','observe','normal')) AS model_said_benign_or_observe,
       count_if(decision = expected)                     AS model_correct
FROM hop(ddos.decision_log, 10s, 10m)
WHERE truth IN ('flash_crowd','port_scan') AND err = ''
GROUP BY window_end, truth;

-- E. Calibration: does confidence track accuracy? (last hour, 10 buckets)
SELECT window_end, floor(p * 10) / 10 AS p_bucket, count() AS n,
       round(count_if(decision = expected) / count(), 3) AS observed_accuracy
FROM hop(ddos.decision_log, 1m, 1h)
WHERE err = ''
GROUP BY window_end, p_bucket;

-- F. Backend health.
SELECT window_start, backend, model, err, count() AS n
FROM tumble(ddos.decision_log, 1m)
GROUP BY window_start, backend, model, err;

-- G. What the model is actually shown (recent window states).
SELECT win_start, truth, rule_flag, state FROM table(ddos.windows) ORDER BY win_start DESC LIMIT 3;

-- H. Results for the report (table query over everything so far).
SELECT qtype, backend, model, count() AS n,
       round(count_if(decision = expected) / count_if(err = ''), 3) AS accuracy,
       round(count_if(err != '') / count(), 3) AS error_rate,
       quantile(0.5)(latency_ms) AS p50_ms, quantile(0.99)(latency_ms) AS p99_ms,
       round(avg(state_chars)) AS avg_state_chars
FROM table(ddos.decision_log)
GROUP BY qtype, backend, model;
