Question - 1
Create roles as per the below-mentioned hierarchy. Accountadmin already exists in Snowflake.

```
CREATE ROLE "ADMIN";
CREATE ROLE "DEVELOPER";
CREATE ROLE "PII";

GRANT ROLE "DEVELOPER" TO ROLE "ADMIN";
GRANT ROLE "ADMIN" TO ROLE "ACCOUNTADMIN";
GRANT ROLE "PII" TO ROLE "ACCOUNTADMIN";
```
Question - 2
Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries.

```
CREATE OR REPLACE WAREHOUSE assignment_wh WITH WAREHOUSE_SIZE='MEDIUM';
```

Granting privileges -
As we created new roles we need to give it some privileges as they are required to run some functional queries.

Approach -
```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;
```
Question - 3
Switch to the admin role
```
USE ROLE ADMIN;
```
Question - 4
Create a database assignment_db

```
CREATE OR REPLACE DATABASE assignment_db;
```
Question - 5
Create a schema my_schema

```
CREATE OR REPLACE schema my_schema;
```
Question - 6
Create a table using any sample csv. You can get 1 by googling for sample csv’s. Preferably search for a sample employee dataset so that you have PII related columns else you can consider any column as PII .
```
CREATE OR REPLACE TABLE EMPLOYEE(
ID NUMBER,
NAME VARCHAR(255),
EMAIL VARCHAR(255),
COUNTRY VARCHAR(255),
REGION VARCHAR(255),
elt_ts TIMESTAMP default current_timestamp(),
elt_by varchar default 'snow',
file_name varchar default 'assignment'
);
```
creating file format 
```
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;
  ```
copying in table using external staging 
```
CREATE STORAGE INTEGRATION s3_integration 
 type = external_stage 
 storage_provider ='s3'
 enabled = true 
 storage_aws_role_arn = 'arn:aws:iam::003905319674:role/snowflake_role' 
 storage_allowed_locations = ('s3://snowflakerb123/employee.csv');
 
-- As we are working on admin role, we grant all on integration object
GRANT ALL ON INTEGRATION s3_integration TO ROLE admin;
-- Describing Integration object to arrange a rrelatoinship between aws and snowflake
DESC INTEGRATION s3_integration;

-- creating file format for csv type file
CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;

-- doing external staging form storage external integration
CREATE OR REPLACE STAGE external_stage URL = 's3://snowflakerb123/employee.csv' STORAGE_INTEGRATION = s3_integration FILE_FORMAT = my_csv_format;
```

querying on table 
```
select * from employee;
```
<img width="1102" alt="employeeTable" src="https://user-images.githubusercontent.com/123542137/234824836-2a1a9729-e6f1-4d56-8aaa-f8ba7ff4fae8.png">


Question - 7
Also, create a variant version of this dataset.

```
create or replace table json_employee(
 json_raw_data variant
);

-- file format for json
create or replace file format json_format
type='json'
strip_outer_array=true;

-- staging for json file
create or replace stage json_stage file_format=json_format;
list @json_stage;
```

--putting in stage using snowsql
```
<!-- PUT file:///Users/rahul/Downloads/employee.json @%json_stage; -->
```

copying into json_employee table
```
copy into json_employee
from @json_stage/employee.json
file_format=json_format;

-- querying on json_employee
select * from json_employee;
```

<img width="1043" alt="json_query" src="https://user-images.githubusercontent.com/123542137/234827439-cbc427a9-f327-431d-afac-aa0fdf49f684.png">



Question - 8
Load data into the tables using copy into statements. In one table load from the internal stage 

```create or replace stage internal_stage;
show stages;
list @internal_stage;

-- copying into in_employee from internal stage
COPY INTO in_employee(
        id,
        name,
        email,
        phone,
        city,
        company,
        file_name
    )
FROM
    (
        SELECT
            emp.$1,
            emp.$2,
            emp.$3,
            emp.$4,
            emp.$5,
            emp.$6,
            METADATA$FILENAME
        FROM
            @internal_stage/employee.csv.gz (file_format => my_csv_format) emp
    );
 ```
```
select * from in_employee;
```

<img width="1102" alt="employeeTable" src="https://user-images.githubusercontent.com/123542137/234830387-c84b25bd-9b68-4f80-9d84-ccf6cdaed8ee.png">



### Question - 9
Upload any parquet file to the stage location and infer the schema of the file.

```create or replace stage parquet_stage;

list @parquet_stage;

-- file format for parquet
CREATE FILE FORMAT my_parquet_format
  TYPE = parquet;
```

```
<!-- PUT file:///Users/rahul/Downloads/final.parquet @parquet_stage; -->
```
Query to Infer about the schema, The below is the code snippet which does the above said things.
```SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@parquet_stage',
      FILE_FORMAT=>'my_parquet_format'
      )
    );
```
The below is the snapshot of INFER SCHEMA.

<img width="840" alt="infer_schema" src="https://user-images.githubusercontent.com/123542137/234832369-d44ecbc2-6ffd-424c-927f-3440b14526f1.png">


Question - 10
Run a select query on the staged parquet file without loading it to a snowflake table.
```
select $1:ID,$1:name,$1:email,$1:country,$1:region from @parquet_stage (file_format=>my_parquet_format);
```
The below is the snapshot of fetched result.
<img width="823" alt="parquet_stage_query" src="https://user-images.githubusercontent.com/123542137/234832760-1a27b9f2-9086-4921-ac37-8123784fcdb5.png">

Question - 11
Add masking policy to the PII columns such that fields like email etc. show as masked to a user with the developer role. If the role is PII the value of these columns should be visible.

```
USE ROLE "ACCOUNTADMIN";

-- creating masking policy on developer roleASSIGNMENT_DB
use role accountadmin;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE developer;
GRANT USAGE ON DATABASE "ASSIGNMENT_DB" to role "DEVELOPER";
GRANT USAGE ON SCHEMA "MY_SCHEMA" to role "DEVELOPER";
GRANT SELECT ON TABLE assignment_db.my_schema.in_employee to role "DEVELOPER";
-- GRANT apply MASKING POLICY on  hideEmail_mask TO ROLE developer;

-- creating masking policy for hiding email on the developer
CREATE OR REPLACE MASKING POLICY hideEmail_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ADMIN', 'PII') THEN val
    ELSE '************'
  END;
  -- applying masking policy on column email
ALTER TABLE IF EXISTS in_employee MODIFY COLUMN email SET MASKING POLICY hideEmail_mask;

-- creating masking policy on phone for developer role
CREATE OR REPLACE MASKING POLICY hidePhone_mask AS (val string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ADMIN', 'PII') THEN val
    ELSE '************'
  END;

  -- on column phone we are applying masking policy hidephone_mask
ALTER TABLE IF EXISTS in_employee MODIFY COLUMN phone SET MASKING POLICY hidePhone_mask;
```
changing the role to developer and then checking the masking applied or not
```use role developer;
select * from in_employee;
```
Below is the snapshot for masked phone and email in developer role

<img width="1111" alt="maskedemail" src="https://user-images.githubusercontent.com/123542137/234833759-704111e4-53ae-4fb0-9854-1e696c7e2666.png">

FOR THE ROLE PII
```
use role accountadmin;
-- granting privileges and permission on database,datawarehouse and schmea on role PII
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE "ASSIGNMENT_DB" to role "PII";
GRANT USAGE ON SCHEMA "MY_SCHEMA" to role "PII";
GRANT SELECT ON TABLE assignment_db.my_schema.in_employee to role "PII";

-- changing the role to PII and querying the table
use role PII;
select * from in_employee;

```
not masked email and phone in PII role
<img width="1102" alt="employeeTable" src="https://user-images.githubusercontent.com/123542137/234834192-e0ae48e6-8f88-45d5-8237-2b7cf02f4f2d.png">

