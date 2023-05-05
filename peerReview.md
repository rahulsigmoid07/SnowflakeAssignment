# chakradhar review
* Created three roles named DEVELOPER, ADMIN and PII, and defined a hierarchy between them.
* Created a medium-sized warehouse named assignment_wh using the accountadmin role, and Granted all privileges on the warehouse to the ADMIN role. It also Granted the privilege to create databases on the account to the ADMIN role.
* Created a database named assignment_db and schema my_schema
* Created a table named EMPLOYEE_DATA to store CSV data. The table includes columns such as ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, MOBILE_NUMBER, CITY, etl_ts, etl_by, and file_name
* Created a file format called my_json_format and a variant version of the data, i.e., it transforms the data into JSON format. It also Created a table named employees_variant to store the variant version of the data.
* Created an internal stage named internal_stage, a file format called my_csv_format, and a storage integration object named s3_integration to access an S3 bucket. It also Granted all privileges on the integration object to the ADMIN role. Finally, it Created an external stage named external_stage to store CSV data in the S3 bucket.
* Created a file format called my_csv_format which holds data of format CSV. After this, created an internal stage called internal_stage with the holding my_csv_format file format. created a storage integration object called s3_integration with the holding S3 storage provider. Then, created an external stage called external_stage with holding my_csv_format file format and an S3 bucket URL.
* loaded data into the tables using copy into statements. In one table, loaded from the internal stage, and in another, loaded from the external. Created two tables called employee_internal_stage and employee_external_stage, respectively, for loading employee data from an internal stage and an external stage. Copied data into the respective tables from the corresponding stages, fetched the table data using metadata functions. Finally, runned a select query on the employee data to check whether they are loaded or not.
* Uploaded a parquet file to the stage location and infer the schema of the file. Created a file format called my_parquet_format, which holds data of format parquet. After this, created a stage called parquet_stage with holding my_parquet_format file format. Finally, query to infer about the schema.
* Created a select query on the staged parquet file without loading it into a Snowflake table.
* Created a masking policy on mobile number and applied it on developer role
# Amit review
He had written a script is written in SQL language that is used to perform given queries in
Snowflake. Below is the explanation of each query -
* He created three roles:
* Admin
* Developer
* PII
* He granted the Admin role to the ACCOUNTADMIN role, the Developer role to the
Admin role, and the PII role to the ACCOUNTADMIN role.
* He created a medium-sized data warehouse using the ACCOUNTADMIN role and grants
all privileges on the warehouse to the Admin role.
*  He created a database named "assignment_db" and a schema named "my_schema"
inside the database, using the Admin role.
* He created a table named "EMPLOYEES_USING_EXT_STAGE" with columns for first
name, last name, email, location, department, ELT timestamp, ELT by, and file name.
*  He created an external stage named "my_ext_stage" that points to an S3 bucket and
allows the Admin role to use it.
*  He created a file format named "my_file_format" that specifies CSV format with a comma
as the field delimiter and header row skipped.
* He loaded data from the external stage into a table named "EMPLOYEES_JSON" that
has a single column named "emp_data" of type VARIANT.
* He created a table named "EMPLOYEES_VARIANT" that extracts data from the
"emp_data" column and maps it to columns with appropriate data types.
* He loaded data from an internal stage named "EMPLOYEES_USING_INT_STAGE" into
a table named "EMPLOYEES_USING_INT_STAGE" using the COPY INTO command.
* He created a file format named "my_parquet_ff" that specifies Parquet format.
* He created a stage named "my_parquet_stage" that points to an S3 bucket with Parquet
files.
* He infered the schema of the Parquet file using the INFER_SCHEMA function and
selects data from the stage.
* It creates a masking policy named "email_mask" that masks the email column of the
"EMPLOYEES_USING_EXT_STAGE" table.
* He altered the "EMAIL" column of the "EMPLOYEES_EXT_STAGE" table to apply the
masking policy.
* He granted usage on the warehouse to the Developer role and grants usage on the
database and schema to the PII role using the ACCOUNTADMIN role to test masking
policy.
