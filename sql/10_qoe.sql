-- 10_qoe.sql — Use case 1: streaming video QoE incident triage.
-- Generic schema (public QoE conventions): no customer fields.
--
--   qoe_raw (random seeds) -> mv_qoe_gen -> qoe_events -> mv_qoe_windows -> qoe_windows
--   -> mv_qoe_decide_cause / mv_qoe_decide_severity -> decision_log
--
-- Scenario schedule: one scenario per 60 s period, chosen from the wall clock, so truth is known per window.
--   sid = int_div(unix_ts, 60) % 6 : 0 none | 1 cdn_fault | 2 isp_congestion | 3 player_bug | 4 encoding_issue | 5 flash_crowd
--   affected cohort for the period: cdn-(period%3), isp-(period%6), player 5.(period%3).0, asset-(period%20)

-- 1. random seeds, ~2,000 events/s (generated only while mv_qoe_gen reads them)
CREATE RANDOM STREAM IF NOT EXISTS qoe_raw (
  s1 uint32 DEFAULT rand(), s2 uint32 DEFAULT rand(), s3 uint32 DEFAULT rand(), s4 uint32 DEFAULT rand()
) SETTINGS eps = 2000;

-- 2. generic player telemetry
CREATE STREAM IF NOT EXISTS qoe_events (
  session_id     string,
  cdn            string,
  isp            string,
  region         string,
  player_version string,
  asset_id       string,
  requests       uint8,      -- segment requests in this heartbeat (load proxy; rises in a flash crowd)
  rebuffer_ms    uint32,
  startup_ms     uint32,
  bitrate_kbps   uint32,
  error_code     string,     -- none | edge_503 | segment_timeout | decoder_error | abr_null_ref
  scenario       string,     -- ground truth (never shown to the model)
  affected       bool
);

-- 3. generator: cohort from seeds, metrics shifted only for the affected cohort of the active scenario
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_qoe_gen INTO qoe_events AS
SELECT
  session_id, cdn, isp, region, player_version, asset_id,
  to_uint8(if(scenario = 'flash_crowd', 3 + s4 % 4, 1))                                            AS requests,
  to_uint32(multi_if(
    scenario = 'cdn_fault'      AND affected, 1500 + s3 % 3000,
    scenario = 'isp_congestion' AND affected,  800 + s3 % 2500,
    scenario = 'encoding_issue' AND affected,  300 + s3 % 900,
    s4 % 100 < 3,                              1000 + s3 % 1500,     -- background noise, 3% of sessions
    s3 % 400))                                                                                        AS rebuffer_ms,
  to_uint32(multi_if(
    scenario = 'player_bug' AND affected, 4000 + s3 % 4000,
    scenario = 'cdn_fault'  AND affected, 2500 + s3 % 2000,
    800 + s3 % 1200))                                                                                 AS startup_ms,
  to_uint32(multi_if(
    scenario = 'isp_congestion' AND affected, 400 + s3 % 600,
    2500 + s3 % 3500))                                                                                AS bitrate_kbps,
  multi_if(
    scenario = 'cdn_fault'      AND affected AND s4 % 10 < 6, if(s4 % 2 = 0, 'edge_503', 'segment_timeout'),
    scenario = 'player_bug'     AND affected AND s4 % 10 < 4, 'abr_null_ref',
    scenario = 'encoding_issue' AND affected AND s4 % 10 < 6, 'decoder_error',
    s4 % 100 < 1,                                              'segment_timeout',                    -- background
    'none')                                                                                           AS error_code,
  scenario, affected
FROM (
  SELECT
    s1, s2, s3, s4,
    'sess-' || to_string(s1 % 20000)                       AS session_id,
    'cdn-'  || to_string(s1 % 3)                           AS cdn,
    'isp-'  || to_string(s2 % 6)                           AS isp,
    'region-' || to_string((s1 + s2) % 5)                  AS region,
    '5.' || to_string(s2 % 3) || '.0'                      AS player_version,
    'asset-' || to_string(s1 % 20)                         AS asset_id,
    int_div(to_unix_timestamp(_tp_time), 60)               AS period,
    array_element(['none','cdn_fault','isp_congestion','player_bug','encoding_issue','flash_crowd'], 1 + period % 6) AS scenario,
    multi_if(
      scenario = 'cdn_fault',      s1 % 3 = period % 3,
      scenario = 'isp_congestion', s2 % 6 = period % 6,
      scenario = 'player_bug',     s2 % 3 = period % 3,
      scenario = 'encoding_issue', s1 % 20 = period % 20,
      scenario = 'flash_crowd',    true,
      false)                                               AS affected
  FROM qoe_raw
);

-- 4. condensed 5 s windows: what an operator's dashboard would show, plus the truth label
CREATE STREAM IF NOT EXISTS qoe_windows (
  win_start       datetime64(3),   -- window_start is a reserved column name
  events          uint32,
  sessions        uint32,
  load_ratio      float32,   -- requests per event; ~1 normally, 3 to 6 in a flash crowd
  degraded_share  float32,   -- share of events with rebuffer > 1000 ms or an error
  p95_rebuffer_ms uint32,
  p50_startup_ms  uint32,
  p50_bitrate     uint32,
  err_edge_503    uint32,
  err_seg_timeout uint32,
  err_decoder     uint32,
  err_abr_null    uint32,
  deg_cdns        uint8,     -- distinct values among DEGRADED events (concentration signal: 1 = one CDN)
  deg_isps        uint8,
  deg_regions     uint8,
  deg_players     uint8,
  deg_assets      uint8,
  rule_flag       bool,      -- the legacy threshold alert
  truth           string,    -- scenario active in this window
  expected_cause  string,    -- truth mapped to the model's option set
  expected_sev    string,    -- truth mapped to the severity levels
  state           string     -- the text the model is given
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_qoe_windows INTO qoe_windows AS
SELECT
  window_start AS win_start, events, sessions, load_ratio, degraded_share, p95_rebuffer_ms, p50_startup_ms, p50_bitrate,
  err_edge_503, err_seg_timeout, err_decoder, err_abr_null,
  deg_cdns, deg_isps, deg_regions, deg_players, deg_assets,
  (p95_rebuffer_ms > 1200 OR degraded_share > 0.08 OR load_ratio > 2.0)                              AS rule_flag,
  truth,
  multi_if(truth = 'flash_crowd', 'traffic_spike_benign', truth = 'none', 'no_incident', truth)     AS expected_cause,
  multi_if(truth = 'cdn_fault', 'critical', truth IN ('isp_congestion', 'player_bug'), 'major',
           truth = 'encoding_issue', 'minor', 'negligible')                                          AS expected_sev,
  concat(
    'Video QoE window (5 s). events=', to_string(events), ' sessions=', to_string(sessions),
    ' load_ratio=', to_string(round(load_ratio, 2)), ' (1.0 is normal)',
    ' degraded_share=', to_string(round(degraded_share, 3)), ' (0.03 is normal)',
    ' p95_rebuffer_ms=', to_string(p95_rebuffer_ms), ' (normal < 500)',
    ' p50_startup_ms=', to_string(p50_startup_ms), ' (normal ~1400)',
    ' p50_bitrate_kbps=', to_string(p50_bitrate), ' (normal ~4000).',
    ' errors: edge_503=', to_string(err_edge_503), ' segment_timeout=', to_string(err_seg_timeout),
    ' decoder_error=', to_string(err_decoder), ' abr_null_ref=', to_string(err_abr_null), '.',
    ' Degraded sessions span ', to_string(deg_cdns), ' of 3 CDNs, ', to_string(deg_isps), ' of 6 ISPs, ',
    to_string(deg_regions), ' of 5 regions, ', to_string(deg_players), ' of 3 player versions, ',
    to_string(deg_assets), ' of 20 assets.',
    if(rule_flag, ' Threshold alert fired.', ' No threshold alert.'))                              AS state
FROM (
  SELECT
    window_start,
    to_uint32(count())                                                                     AS events,
    to_uint32(uniq(session_id))                                                            AS sessions,
    to_float32(sum(requests) / count())                                                    AS load_ratio,
    to_float32(count_if(rebuffer_ms > 1000 OR error_code != 'none') / count())             AS degraded_share,
    to_uint32(quantile(0.95)(rebuffer_ms))                                                 AS p95_rebuffer_ms,
    to_uint32(quantile(0.5)(startup_ms))                                                   AS p50_startup_ms,
    to_uint32(quantile(0.5)(bitrate_kbps))                                                 AS p50_bitrate,
    to_uint32(count_if(error_code = 'edge_503'))                                           AS err_edge_503,
    to_uint32(count_if(error_code = 'segment_timeout'))                                    AS err_seg_timeout,
    to_uint32(count_if(error_code = 'decoder_error'))                                      AS err_decoder,
    to_uint32(count_if(error_code = 'abr_null_ref'))                                       AS err_abr_null,
    to_uint8(uniq_if(cdn,            rebuffer_ms > 1000 OR error_code != 'none'))          AS deg_cdns,
    to_uint8(uniq_if(isp,            rebuffer_ms > 1000 OR error_code != 'none'))          AS deg_isps,
    to_uint8(uniq_if(region,         rebuffer_ms > 1000 OR error_code != 'none'))          AS deg_regions,
    to_uint8(uniq_if(player_version, rebuffer_ms > 1000 OR error_code != 'none'))          AS deg_players,
    to_uint8(uniq_if(asset_id,       rebuffer_ms > 1000 OR error_code != 'none'))          AS deg_assets,
    array_element(['none','cdn_fault','isp_congestion','player_bug','encoding_issue','flash_crowd'],
                  1 + int_div(to_unix_timestamp(window_start), 60) % 6)                    AS truth
  FROM tumble(qoe_events, 5s)
  GROUP BY window_start
);

-- 5. decisions: cause (choice) and severity (score), one each per window, into the shared log
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_qoe_decide_cause INTO decision_log AS
SELECT
  'qoe' AS use_case, win_start, 'global' AS cohort, question, 'choice' AS qtype,
  json_extract_string(d, 'choice')                 AS decision,
  to_float32_or_null(json_extract_raw(d, 'p'))     AS p,
  to_float32_or_null(json_extract_raw(d, 'p_raw')) AS p_raw,
  json_extract_raw(d, 'probabilities')             AS probabilities,
  json_extract_string(d, 'model')                  AS model,
  json_extract_string(d, 'backend')                AS backend,
  to_uint32(json_extract_int(d, 'latency_ms'))     AS latency_ms,
  json_extract_string(d, 'batch')                  AS batch,
  json_extract_string(d, 'err')                    AS err,
  rule_flag, truth, expected_cause AS expected, to_uint32(length(state)) AS state_chars
FROM (
  SELECT win_start, rule_flag, truth, expected_cause, state, question,
         decide_choice(state, question,
           '["cdn_fault","isp_congestion","player_bug","encoding_issue","traffic_spike_benign","no_incident"]') AS d
  FROM (
    SELECT *, 'What is the most likely cause of the viewer-quality situation in this window?' AS question
    FROM qoe_windows
  )
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_qoe_decide_severity INTO decision_log AS
SELECT
  'qoe' AS use_case, win_start, 'global' AS cohort, question, 'score' AS qtype,
  json_extract_string(d, 'level')                  AS decision,
  to_float32_or_null(json_extract_raw(d, 'p'))     AS p,
  to_float32_or_null(json_extract_raw(d, 'p_raw')) AS p_raw,
  json_extract_raw(d, 'probabilities')             AS probabilities,
  json_extract_string(d, 'model')                  AS model,
  json_extract_string(d, 'backend')                AS backend,
  to_uint32(json_extract_int(d, 'latency_ms'))     AS latency_ms,
  json_extract_string(d, 'batch')                  AS batch,
  json_extract_string(d, 'err')                    AS err,
  rule_flag, truth, expected_sev AS expected, to_uint32(length(state)) AS state_chars
FROM (
  SELECT win_start, rule_flag, truth, expected_sev, state, question,
         decide_score(state, question, '["negligible","minor","major","critical"]') AS d
  FROM (
    SELECT *, 'How severe is this window for viewers?' AS question
    FROM qoe_windows
  )
);
