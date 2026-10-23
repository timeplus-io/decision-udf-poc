-- 01_streams.sql — shared streams.

-- Every decision the model makes, for both use cases, appended forever: audit trail and replay source.
CREATE STREAM IF NOT EXISTS decision_log (
  use_case      string,              -- 'qoe' | 'ddos'
  win_start     datetime64(3),       -- window_start is a reserved column name in Timeplus Enterprise
  cohort        string,              -- qoe: cdn|isp|region ; ddos: dst_ip
  question      string,
  qtype         string,              -- 'choice' | 'score' | 'noul'
  decision      string,              -- chosen option / level / 'yes'/'no'
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
