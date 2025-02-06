
-- @existeparquet
select count (name_file)
from PRO_BRONZE_DB.{schema}.CONTROL_PARQUETS
where name_file =  '{nametable}' ;

-- @firstload
select count(distinct name_file )
from PRO_BRONZE_DB.{schema}.CONTROL_PARQUETS
where table_name = '{nametable}';

--@existstage
SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.STAGES
        WHERE STAGE_SCHEMA = '{schema}'
        AND STAGE_NAME = '{stage_name}';

--@createstage
CREATE STAGE IF NOT EXISTS  {stage_name};

--@addfilestage
PUT file://{name} @{stage_name};

--@createtable
CREATE TABLE IF NOT EXISTS  PRO_BRONZE_DB.{schema}.{nametable}
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@{stage_name}/{name_parquet}' ,
          FILE_FORMAT=>'PRO_BRONZE_DB.{schema}.my_parquet_format'
        )
      ));

--@createtabletaux
CREATE TABLE IF NOT EXISTS  PRO_BRONZE_DB.{schema}.{taux_nametable} LIKE PRO_BRONZE_DB.{schema}.{nametable};

--@createformat
CREATE FILE FORMAT IF NOT EXISTS  PRO_BRONZE_DB.{schema}.my_parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

--@copytotable
COPY INTO PRO_BRONZE_DB.{schema}.{taux_nametable}
FROM @{stage_name}/{name_parquet}
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
ON_ERROR = 'CONTINUE';

--@removestage
REMOVE @{stage_name}/{name};

--@inserdata
INSERT INTO PRO_BRONZE_DB.{schema}.CONTROL_PARQUETS (table_name, fecha, name_file)
VALUES ('{table_name}', '{fecha}', '{name_file}');

--@createcontrol
CREATE TABLE IF NOT EXISTS PRO_BRONZE_DB.{schema}.CONTROL_PARQUETS  (
    table_name string,
    fecha datetime,
    name_file string
);

--@createschema
CREATE SCHEMA IF NOT EXISTS PRO_BRONZE_DB.{schema};

--@defaultdatabase
USE DATABASE PRO_BRONZE_DB;

--@insert_to_finall_table
insert into PRO_BRONZE_DB.{schema}.{nametable} (
  select  A.*, CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS INSERT_DATE_UTC

  from
    (
      select * exclude INSERT_DATE_UTC
      from PRO_BRONZE_DB.{schema}.{taux_nametable}
      minus
      select * exclude INSERT_DATE_UTC
      from PRO_BRONZE_DB.{schema}.{nametable}
    )A
);

--@remove_taux_table
drop table PRO_BRONZE_DB.{schema}.{taux_nametable};

--@add_insertdate
ALTER TABLE PRO_BRONZE_DB.{schema}.{nametable}
ADD COLUMN INSERT_DATE_UTC TIMESTAMP_NTZ(9)