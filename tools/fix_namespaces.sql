-- ===========================================================================
--  fix_namespaces.sql  -  replace placeholder namespace ids in redshift.duckdb
--
--  The three CONSUMER profiles were loaded while redshift_cluster_profiles.json
--  still held its template values, so every consumer row was stored under the
--  literal string "REPLACE-CONSUMER-n-NAMESPACE-ID". The PRODUCER namespace is
--  already correct and is not touched.
--
--  The data is correct - only its namespace label is wrong. The analysis scope
--  filters on the real namespace, so those clusters show zero rows while their
--  data sits right there. This relabels in place: no Redshift connection, no
--  reload, so a busy cluster does not block the fix.
--
--  MAPPING
--    REPLACE-CONSUMER-1-NAMESPACE-ID  ->  602d7b45-...  (Commercial Bank)
--    REPLACE-CONSUMER-2-NAMESPACE-ID  ->  4d44d131-...  (Consumer Bank)
--    REPLACE-CONSUMER-3-NAMESPACE-ID  ->  201bbc47-...  (Finance & Risk)
--
--  HOW TO RUN
--    1. CLOSE the Databa6ix app first (DuckDB allows a single writer).
--    2. Back up the file:
--         copy "%USERPROFILE%\RQP\dataedshift.duckdb" ^
--              "%USERPROFILE%\RQP\dataedshift.backup.duckdb"
--    3. duckdb "%USERPROFILE%\RQP\dataedshift.duckdb"
--    4. Run SECTION 1 and READ IT. Only then run SECTION 2.
--
--  Updates BASE TABLES only. Views carrying namespace_id derive from these and
--  follow automatically; updating a view would fail.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- SECTION 1 - INSPECT  (read-only; run this first)
-- ---------------------------------------------------------------------------

SELECT
  COALESCE(NULLIF(TRIM(namespace_id), ''), '(null or empty)') AS namespace_id,
  LENGTH(namespace_id)                                        AS id_length,
  COUNT(*)                                                    AS rows,
  CASE WHEN namespace_id LIKE 'REPLACE-%' THEN 'PLACEHOLDER - repaired below'
       WHEN LENGTH(namespace_id) = 36     THEN 'ok'
       WHEN namespace_id IS NULL
         OR TRIM(namespace_id) = ''       THEN 'unknown origin - LEAVE ALONE'
       ELSE 'review' END                                      AS status
FROM query_history
GROUP BY 1, 2
ORDER BY rows DESC;

-- STOP if no row shows 'PLACEHOLDER'. The updates below would match nothing
-- and the zero-rows problem is somewhere else.
--
-- NULL / empty namespaces are NOT repaired here. Their origin cluster is
-- unknown, so any value written would be a guess. With "All loaded clusters"
-- ticked in Settings the scope filter is dropped and those rows are visible
-- anyway.


-- ---------------------------------------------------------------------------
-- SECTION 2 - REPAIR
-- ---------------------------------------------------------------------------
-- One transaction: every table moves together, or a failure rolls the whole
-- thing back and leaves the database untouched.

BEGIN TRANSACTION;

-- Commercial Bank:  REPLACE-CONSUMER-1-NAMESPACE-ID  ->  602d7b45-34f6-40ee-b69e-54c9e0cb5d96
UPDATE "analysis_cache_repeat_members" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "child_query_text" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "external_table_info_all" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "external_table_metadata" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "external_tables_catalog" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "procedure_definitions" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_detail_flow" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_details" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_explain" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_group_assignments" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_health" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_history" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_history_all" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "query_text" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "snapshot_cluster_runs" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "svv_table_info_all" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "table_scan_info" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "user_info" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "user_roster" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';
UPDATE "view_definitions" SET namespace_id = '602d7b45-34f6-40ee-b69e-54c9e0cb5d96' WHERE namespace_id = 'REPLACE-CONSUMER-1-NAMESPACE-ID';

-- Consumer Bank:  REPLACE-CONSUMER-2-NAMESPACE-ID  ->  4d44d131-c4df-41f0-8260-ec99d58419d7
UPDATE "analysis_cache_repeat_members" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "child_query_text" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "external_table_info_all" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "external_table_metadata" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "external_tables_catalog" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "procedure_definitions" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_detail_flow" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_details" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_explain" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_group_assignments" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_health" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_history" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_history_all" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "query_text" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "snapshot_cluster_runs" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "svv_table_info_all" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "table_scan_info" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "user_info" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "user_roster" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';
UPDATE "view_definitions" SET namespace_id = '4d44d131-c4df-41f0-8260-ec99d58419d7' WHERE namespace_id = 'REPLACE-CONSUMER-2-NAMESPACE-ID';

-- Finance & Risk:  REPLACE-CONSUMER-3-NAMESPACE-ID  ->  201bbc47-5536-4caf-af86-3d8e827f90c9
UPDATE "analysis_cache_repeat_members" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "child_query_text" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "external_table_info_all" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "external_table_metadata" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "external_tables_catalog" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "procedure_definitions" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_detail_flow" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_details" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_explain" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_group_assignments" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_health" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_history" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_history_all" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "query_text" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "snapshot_cluster_runs" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "svv_table_info_all" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "table_scan_info" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "user_info" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "user_roster" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';
UPDATE "view_definitions" SET namespace_id = '201bbc47-5536-4caf-af86-3d8e827f90c9' WHERE namespace_id = 'REPLACE-CONSUMER-3-NAMESPACE-ID';

COMMIT;


-- ---------------------------------------------------------------------------
-- SECTION 3 - VERIFY
-- ---------------------------------------------------------------------------
-- No row should still read 'REPLACE-...'. Expect four 36-character ids
-- (producer plus the three consumers).

SELECT
  COALESCE(NULLIF(TRIM(namespace_id), ''), '(null or empty)') AS namespace_id,
  LENGTH(namespace_id)                                        AS id_length,
  COUNT(*)                                                    AS rows
FROM query_history
GROUP BY 1, 2
ORDER BY rows DESC;


-- ---------------------------------------------------------------------------
-- AFTERWARDS - two steps this script cannot do for you
-- ---------------------------------------------------------------------------
-- 1. Edit %USERPROFILE%\RQPedshift_cluster_profiles.json and replace the
--    three consumer placeholders. WITHOUT THIS THE NEXT LOAD RE-CREATES THEM.
--
--      REDSHIFT_CONSUMER_1  Commercial Bank  602d7b45-34f6-40ee-b69e-54c9e0cb5d96
--      REDSHIFT_CONSUMER_2  Consumer Bank    4d44d131-c4df-41f0-8260-ec99d58419d7
--      REDSHIFT_CONSUMER_3  Finance & Risk   201bbc47-5536-4caf-af86-3d8e827f90c9
--
--    Each must be exactly 36 characters. If a value is 72 characters it is the
--    same UUID pasted twice - keep only the first half.
--
-- 2. In the app: Settings -> Analysis cluster scope. Tick "All loaded
--    clusters", or confirm the ticked namespaces match the ids above. A stale
--    scope still shows zero rows even after the data is repaired.
-- ---------------------------------------------------------------------------
