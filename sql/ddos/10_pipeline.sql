-- ddos/10_pipeline.sql — Use case 2: edge flow-anomaly (DDoS) response. Everything lives in database `ddos`.
-- Generic NetFlow-like schema: no customer fields. Needs the shared UDF from sql/01_udf.sql.
--
--   ddos.seeds (random) -> ddos.mv_generate -> ddos.records -> ddos.mv_windows -> ddos.windows
--   -> ddos.mv_decide_action / ddos.mv_decide_kind -> ddos.decision_log
--
-- Scenario schedule: one scenario per 60 s period from the wall clock.
--   sid = int_div(unix_ts, 60) % 6 : 0 none | 1 syn_flood | 2 udp_amplification | 3 http_flood | 4 port_scan | 5 flash_crowd
--   victim for the period: svc-(period % 8). A port scan hits every service from one small source pool.

CREATE DATABASE IF NOT EXISTS ddos;

-- 0. every decision this pipeline makes, appended forever: audit trail, scorecard source, replay source
CREATE STREAM IF NOT EXISTS ddos.decision_log (
  use_case      string,              -- constant per pipeline, kept so logs can be unioned
  win_start     datetime64(3),       -- window_start is a reserved column name in Timeplus Enterprise
  cohort        string,              -- qoe: global ; ddos: dst_ip
  question      string,
  qtype         string,              -- choice | score | noul
  decision      string,              -- chosen option / level
  p             nullable(float32),   -- calibrated probability of the chosen answer
  p_raw         nullable(float32),
  probabilities string,              -- JSON, all options
  model         string,
  backend       string,
  latency_ms    uint32,
  batch         string,
  err           string,
  rule_flag     bool,                -- what the threshold rule said
  truth         string,              -- ground truth from the scenario schedule
  expected      string,              -- expected answer for this question (truth mapped to the option set)
  state_chars   uint32
);

-- 1. random seeds, ~5,000 records/s
CREATE RANDOM STREAM IF NOT EXISTS ddos.seeds (
  s1 uint32 DEFAULT rand(), s2 uint32 DEFAULT rand(), s3 uint32 DEFAULT rand(), s4 uint32 DEFAULT rand()
) SETTINGS eps = 5000;

-- 2. flow records
CREATE STREAM IF NOT EXISTS ddos.records (
  src_ip      string,
  dst_ip      string,     -- one of 8 protected services: svc-0 .. svc-7
  dst_port    uint16,
  proto       string,     -- tcp | udp
  packets     uint32,
  bytes       uint32,
  tcp_flags   string,     -- S | SA | PA | FA | '' (udp)
  duration_ms uint32,
  scenario    string,     -- ground truth (never shown to the model)
  attack_flow bool
);

-- 3. generator
CREATE MATERIALIZED VIEW IF NOT EXISTS ddos.mv_generate INTO ddos.records AS
SELECT
  multi_if(
    scenario = 'udp_amplification' AND attack_flow, '198.51.100.' || to_string(s2 % 50),            -- ~50 reflectors
    scenario = 'http_flood'        AND attack_flow, '203.0.113.' || to_string(s2 % 200) || '.' || to_string(s3 % 10),
    scenario = 'port_scan'         AND attack_flow, '192.0.2.' || to_string(s2 % 10),               -- 10 scanners
    '10.' || to_string(s1 % 200) || '.' || to_string(s2 % 250) || '.' || to_string(s3 % 250))        AS src_ip,
  if(attack_flow AND scenario != 'port_scan', victim, 'svc-' || to_string(s2 % 8))                   AS dst_ip,
  to_uint16(multi_if(
    scenario = 'udp_amplification' AND attack_flow, array_element([53, 123, 11211], 1 + s3 % 3),
    scenario = 'http_flood'        AND attack_flow, 443,
    scenario = 'port_scan'         AND attack_flow, 1 + s3 % 65000,
    s3 % 10 < 7, 443, s3 % 10 < 9, 80, 22))                                                           AS dst_port,
  if(scenario = 'udp_amplification' AND attack_flow, 'udp', if(s4 % 20 = 0, 'udp', 'tcp'))             AS proto,
  to_uint32(multi_if(
    scenario = 'syn_flood'         AND attack_flow, 1,
    scenario = 'udp_amplification' AND attack_flow, 20 + s4 % 40,
    scenario = 'http_flood'        AND attack_flow, 20 + s4 % 30,
    scenario = 'port_scan'         AND attack_flow, 1,
    1 + s4 % 40))                                                                                      AS packets,
  to_uint32(multi_if(
    scenario = 'syn_flood'         AND attack_flow, 60,
    scenario = 'udp_amplification' AND attack_flow, (20 + s4 % 40) * 1400,
    scenario = 'port_scan'         AND attack_flow, 60,
    (1 + s4 % 40) * (200 + s3 % 900)))                                                                 AS bytes,
  multi_if(
    scenario = 'udp_amplification' AND attack_flow, '',
    scenario = 'syn_flood'         AND attack_flow, 'S',
    scenario = 'port_scan'         AND attack_flow, 'S',
    s4 % 20 = 0, '',
    array_element(['SA', 'PA', 'PA', 'FA', 'S'], 1 + s3 % 5))                                          AS tcp_flags,
  to_uint32(if(attack_flow, s4 % 50, 50 + s4 % 2000))                                                   AS duration_ms,
  scenario, attack_flow
FROM (
  SELECT
    s1, s2, s3, s4,
    int_div(to_unix_timestamp(_tp_time), 60)               AS period,
    array_element(['none','syn_flood','udp_amplification','http_flood','port_scan','flash_crowd'], 1 + period % 6) AS scenario,
    'svc-' || to_string(period % 8)                        AS victim,
    multi_if(
      scenario = 'syn_flood',         s4 % 100 < 60,
      scenario = 'udp_amplification', s4 % 100 < 40,
      scenario = 'http_flood',        s4 % 100 < 50,
      scenario = 'port_scan',         s4 % 100 < 10,
      scenario = 'flash_crowd',       s4 % 100 < 50,
      false)                                               AS attack_flow
  FROM ddos.seeds
);

-- 4. condensed 5 s windows per protected service
CREATE STREAM IF NOT EXISTS ddos.windows (
  win_start       datetime64(3),   -- window_start is a reserved column name
  dst_ip          string,
  records         uint32,
  pps             float32,
  mbps            float32,
  uniq_src        uint32,
  syn_only_ratio  float32,   -- share of flows that are a bare SYN
  udp_share       float32,
  mean_pkt_bytes  float32,
  uniq_dst_ports  uint32,
  p443            uint32, p80 uint32, p53 uint32, p123 uint32, p11211 uint32,
  rule_flag       bool,      -- legacy alert: volume or source-count threshold
  candidate       bool,      -- looser gate that decides which windows the model sees
  truth           string,    -- scenario, as it applies to THIS service ('none' for non-victims)
  expected_action string,    -- mitigate | observe | escalate
  expected_kind   string,
  state           string
);

CREATE MATERIALIZED VIEW IF NOT EXISTS ddos.mv_windows INTO ddos.windows AS
SELECT
  window_start AS win_start, dst_ip, records, pps, mbps, uniq_src, syn_only_ratio, udp_share, mean_pkt_bytes, uniq_dst_ports,
  p443, p80, p53, p123, p11211,
  (pps > 20000 OR uniq_src > 6000)                                                                   AS rule_flag,
  (pps > 9000 OR uniq_src > 3000 OR uniq_dst_ports > 100 OR syn_only_ratio > 0.5 OR udp_share > 0.5)  AS candidate,
  truth,
  multi_if(truth IN ('syn_flood', 'udp_amplification', 'http_flood'), 'mitigate', 'observe')        AS expected_action,
  multi_if(truth = 'flash_crowd', 'legitimate_surge', truth = 'none', 'normal', truth)              AS expected_kind,
  concat(
    'Edge flow window (5 s) for service ', dst_ip, '. records=', to_string(records),
    ' pps=', to_string(round(pps)), ' (normal ~2500)',
    ' mbps=', to_string(round(mbps, 1)),
    ' unique_sources=', to_string(uniq_src), ' (normal ~600)',
    ' syn_only_ratio=', to_string(round(syn_only_ratio, 2)), ' (normal ~0.2)',
    ' udp_share=', to_string(round(udp_share, 2)), ' (normal ~0.05)',
    ' mean_packet_bytes=', to_string(round(mean_pkt_bytes)), ' (normal ~650)',
    ' unique_dst_ports=', to_string(uniq_dst_ports), ' (normal ~3).',
    ' flows by port: 443=', to_string(p443), ' 80=', to_string(p80), ' 53=', to_string(p53),
    ' 123=', to_string(p123), ' 11211=', to_string(p11211), '.',
    if(rule_flag, ' Volume threshold alert fired.', ' No volume threshold alert.'))                 AS state
FROM (
  SELECT
    window_start, dst_ip,
    to_uint32(count())                                                 AS records,
    to_float32(sum(packets) / 5)                                       AS pps,
    to_float32(sum(bytes) * 8 / 5 / 1000000)                           AS mbps,
    to_uint32(uniq(src_ip))                                            AS uniq_src,
    to_float32(count_if(tcp_flags = 'S') / count())                    AS syn_only_ratio,
    to_float32(count_if(proto = 'udp') / count())                      AS udp_share,
    to_float32(sum(bytes) / sum(packets))                              AS mean_pkt_bytes,
    to_uint32(uniq(dst_port))                                          AS uniq_dst_ports,
    to_uint32(count_if(dst_port = 443))   AS p443,
    to_uint32(count_if(dst_port = 80))    AS p80,
    to_uint32(count_if(dst_port = 53))    AS p53,
    to_uint32(count_if(dst_port = 123))   AS p123,
    to_uint32(count_if(dst_port = 11211)) AS p11211,
    -- truth for THIS service: the period's scenario if this service is the victim (a scan hits every service).
    -- Windows are 5 s and periods 60 s, so a window never straddles two scenarios.
    array_element(['none','syn_flood','udp_amplification','http_flood','port_scan','flash_crowd'],
                  1 + int_div(to_unix_timestamp(window_start), 60) % 6)  AS sched,
    multi_if(
      sched = 'port_scan', 'port_scan',
      dst_ip = 'svc-' || to_string(int_div(to_unix_timestamp(window_start), 60) % 8), sched,
      'none')                                                          AS truth
  FROM tumble(ddos.records, 5s)
  GROUP BY window_start, dst_ip
);

-- 5. decisions, only for candidate windows (a handful per minute)
CREATE MATERIALIZED VIEW IF NOT EXISTS ddos.mv_decide_action INTO ddos.decision_log AS
SELECT
  'ddos' AS use_case, win_start, dst_ip AS cohort, question, 'choice' AS qtype,
  json_extract_string(d, 'choice')                 AS decision,
  to_float32_or_null(json_extract_raw(d, 'p'))     AS p,
  to_float32_or_null(json_extract_raw(d, 'p_raw')) AS p_raw,
  json_extract_raw(d, 'probabilities')             AS probabilities,
  json_extract_string(d, 'model')                  AS model,
  json_extract_string(d, 'backend')                AS backend,
  to_uint32(json_extract_int(d, 'latency_ms'))     AS latency_ms,
  json_extract_string(d, 'batch')                  AS batch,
  json_extract_string(d, 'err')                    AS err,
  rule_flag, truth, expected_action AS expected, to_uint32(length(state)) AS state_chars
FROM (
  SELECT win_start, dst_ip, rule_flag, truth, expected_action, state, question,
         decide_choice(state, question, '["mitigate","observe","escalate"]') AS d
  FROM (
    SELECT *, 'What should the edge do now for this service? mitigate = rate-limit or block the offending traffic, observe = keep watching, escalate = page a human.' AS question
    FROM ddos.windows WHERE candidate
  )
);

CREATE MATERIALIZED VIEW IF NOT EXISTS ddos.mv_decide_kind INTO ddos.decision_log AS
SELECT
  'ddos' AS use_case, win_start, dst_ip AS cohort, question, 'choice' AS qtype,
  json_extract_string(d, 'choice')                 AS decision,
  to_float32_or_null(json_extract_raw(d, 'p'))     AS p,
  to_float32_or_null(json_extract_raw(d, 'p_raw')) AS p_raw,
  json_extract_raw(d, 'probabilities')             AS probabilities,
  json_extract_string(d, 'model')                  AS model,
  json_extract_string(d, 'backend')                AS backend,
  to_uint32(json_extract_int(d, 'latency_ms'))     AS latency_ms,
  json_extract_string(d, 'batch')                  AS batch,
  json_extract_string(d, 'err')                    AS err,
  rule_flag, truth, expected_kind AS expected, to_uint32(length(state)) AS state_chars
FROM (
  SELECT win_start, dst_ip, rule_flag, truth, expected_kind, state, question,
         decide_choice(state, question,
           '["syn_flood","udp_amplification","http_flood","port_scan","legitimate_surge","normal"]') AS d
  FROM (
    SELECT *, 'What kind of traffic is this service receiving in this window?' AS question
    FROM ddos.windows WHERE candidate
  )
);
