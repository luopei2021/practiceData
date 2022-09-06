-- workflow=etl_{{name.lower()}}
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- stepId=1
-- sourceConfig
--  dataSourceType=temp
-- targetConfig
--  dataSourceType=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy')                                as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')                                  as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')                                  as `DAY`;


-- step=2
-- source=mysql
--  dbName=raw
--  tableName={{name}}
-- target=hive
--  dbName=ods
--  tableName={{name}}
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select {% for col in columns -%}
           {{col.name}}	AS {{col.name}},
       {% endfor -%}
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.{{name}}
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName={{name}}
-- target=hive
--  dbName=dw
--  tableName={{name}}
-- writeMode=overwrite
select {% for col in columns -%}
           {{col.name}}	AS {{col.name}}{{ "" if loop.last else "," }}
       {% endfor -%}
from ods.{{name}}
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select {% for col in columns -%}
           {{col.name}}	AS {{col.name}}{{ "" if loop.last else "," }}
       {% endfor -%}
from dw.{{name}} as a
left join ods.{{name}} as b on
       {% for onekey in primary_key -%}
           a.{{onekey}} = b.{{onekey}} {{ "" if loop.last else "and" }}
       {% endfor -%}
      and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.{{primary_key[0]}}  is null
