-- workflow=test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=read
-- source=mysql
--  dbName=bigdata_etl
--  tableName=job_log
-- target=hive
--  dbName=default
--  tableName=student
-- writeMode=append
select 1 as id, 'sss' as name;