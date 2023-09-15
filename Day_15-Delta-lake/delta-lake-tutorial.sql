-- Databricks notebook source
-- MAGIC %md
-- MAGIC - [What is Delta Lake](https://docs.databricks.com/en/delta/index.html) 
-- MAGIC - [Delta Lake Tutorial](https://docs.databricks.com/en/delta/tutorial.html#language-sql): This tutorial introduces common Delta Lake operations on Databricks.
-- MAGIC - Delta Lake is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Lakehouse Platform. 
-- MAGIC - Delta Lake is open source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling.
-- MAGIC - Delta Lake is the default for all reads, writes, and table creation commands in Databricks Runtime 8.0 and above. 
-- MAGIC - You can use the delta keyword to specify the format if using Databricks Runtime 7.3 LTS.

-- COMMAND ----------

-- MAGIC %md ## Basic Stuffs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/databricks-datasets/flights/")

-- COMMAND ----------

drop table if exists delayflights;
create table if not exists delayflights
as select * 
from read_files("/databricks-datasets/flights/departuredelays.csv")


-- COMMAND ----------

DESCRIBE DETAIL delayflights;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/databricks-datasets/learning-spark-v2/people/")

-- COMMAND ----------

DROP TABLE IF EXISTS people_10m;

CREATE TABLE IF NOT EXISTS people_10m
AS SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

-- COMMAND ----------

DESCRIBE DETAIL people_10m;

-- COMMAND ----------

select count(*) from people_10m

-- COMMAND ----------

-- MAGIC %md ## Upsert to a Table
-- MAGIC - To merge a set of updates and insertions into an existing Delta table, you use the [MERGE INTO](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html) statement

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW people_updates (
  id, firstName, middleName, lastName, gender, birthDate, ssn, salary
) AS VALUES
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
  (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);

MERGE INTO people_10m
USING people_updates
ON people_10m.id = people_updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

DESCRIBE people_10m;

-- COMMAND ----------

SHOW COLUMNS FROM people_10m;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW people_updates (
  id, firstName, middleName, lastName, gender, birthDate, ssn, salary
) AS VALUES
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
  (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);

MERGE INTO people_10m
USING (
  SELECT id, firstName, middleName, lastName, gender, ssn, salary, try_cast(birthDate AS timestamp) AS birthDate
  FROM people_updates
) AS updates
ON people_10m.id = updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md ## Read a table

-- COMMAND ----------

select * from people_10m limit 10;

-- COMMAND ----------

describe detail people_10m

-- COMMAND ----------

select * from delta.`dbfs:/user/hive/warehouse/people_10m` limit 10;

-- COMMAND ----------

-- MAGIC %md ## Update a table

-- COMMAND ----------

UPDATE people_10m SET gender = 'Female' WHERE gender = 'F';
UPDATE people_10m SET gender = 'Male' WHERE gender = 'M';

-- COMMAND ----------

select * from people_10m limit 10;

-- COMMAND ----------

-- MAGIC %md ## Delete from a table

-- COMMAND ----------

delete from people_10m where birthDate < '1960-01-01'

-- COMMAND ----------

select * from people_10m limit 10;

-- COMMAND ----------

-- MAGIC %md ## Display table history

-- COMMAND ----------

describe history people_10m

-- COMMAND ----------

-- MAGIC %md ## Query an earlier version of the table (time travel)

-- COMMAND ----------

SELECT * FROM people_10m VERSION AS OF 0 limit 10

-- COMMAND ----------

-- select * from people_10m timestamp as of "2023-09-15T10:53:20.000+0000"

-- COMMAND ----------

-- MAGIC %md ## Optimize a table
-- MAGIC Once you have performed multiple changes to a table, you might have a lot of small files. To improve the speed of read queries, you can use OPTIMIZE to collapse small files into larger ones:
-- MAGIC
-- MAGIC

-- COMMAND ----------

optimize people_10m

-- COMMAND ----------

-- MAGIC %md ## Z-ordering By Columns
-- MAGIC - [Useful read](https://docs.databricks.com/en/delta/optimize.html#how-often-should-i-run-optimize)
-- MAGIC - Z-Ordering is a technique used in Delta Lake to co-locate related information in the same set of files, which is automatically used by Delta Lake in data-skipping algorithms. This behavior dramatically reduces the amount of data that Delta Lake needs to read, resulting in faster queries and improved query performance.
-- MAGIC - By reorganizing the data in storage, certain queries can read less data, so they run faster.

-- COMMAND ----------

OPTIMIZE people_10m
ZORDER BY (gender)

-- COMMAND ----------

-- MAGIC %md ## Clean up snapshots with VACUUM
-- MAGIC - Delta Lake provides snapshot isolation for reads, which means that it is safe to run OPTIMIZE even while other users or jobs are querying the table. 
-- MAGIC - However, you should clean up old snapshots.

-- COMMAND ----------

VACUUM people_10m

-- COMMAND ----------


