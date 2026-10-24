-- qoe/99_teardown.sql — remove the qoe pipeline: the database and everything in it (streams, views, log).
-- The shared UDF (sql/01_udf.sql) stays; sql/99_teardown.sql removes everything.
DROP DATABASE IF EXISTS qoe CASCADE;
