-- 99_teardown.sql — drop everything the other files created. Order matters: views before their streams,
-- functions before decision_log is gone. Add new objects here when you add them elsewhere.
DROP VIEW IF EXISTS mv_qoe_decide_cause;
DROP VIEW IF EXISTS mv_qoe_decide_severity;
DROP VIEW IF EXISTS mv_qoe_windows;
DROP VIEW IF EXISTS mv_qoe_gen;
DROP STREAM IF EXISTS qoe_windows;
DROP STREAM IF EXISTS qoe_events;
DROP STREAM IF EXISTS qoe_raw;
DROP VIEW IF EXISTS mv_ddos_decide_action;
DROP VIEW IF EXISTS mv_ddos_decide_kind;
DROP VIEW IF EXISTS mv_flow_windows;
DROP VIEW IF EXISTS mv_flow_gen;
DROP STREAM IF EXISTS flow_windows;
DROP STREAM IF EXISTS flow_records;
DROP STREAM IF EXISTS flow_raw;
DROP FUNCTION IF EXISTS decide_noul;
DROP FUNCTION IF EXISTS decide_choice;
DROP FUNCTION IF EXISTS decide_score;
DROP FUNCTION IF EXISTS decide;
DROP STREAM IF EXISTS decision_log;
