-- 99_teardown.sql — remove everything: both pipeline databases (streams, views, logs) and the shared UDF.
-- For one pipeline only, use sql/qoe/99_teardown.sql or sql/ddos/99_teardown.sql.
DROP DATABASE IF EXISTS qoe CASCADE;
DROP DATABASE IF EXISTS ddos CASCADE;
DROP FUNCTION IF EXISTS decide_noul;
DROP FUNCTION IF EXISTS decide_choice;
DROP FUNCTION IF EXISTS decide_score;
DROP FUNCTION IF EXISTS decide;
