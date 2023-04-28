
-- 1
create role Admin;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;

CREATE ROLE DEVELOPER;
GRANT ROLE DEVELOPER TO ROLE ADMIN;

CREATE ROLE PII;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;

-- 2
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS assignment_wh
WAREHOUSE_SIZE = 'MEDIUM'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

-- Grant permissions to create a database and access the warehouse to the Admin role
GRANT CREATE DATABASE ON account TO ROLE Admin;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE Admin;

-- 3

USE ROLE ADMIN;

-- 4

CREATE DATABASE IF NOT EXISTS assignment_db;

-- 5
CREATE SCHEMA IF NOT EXISTS assignment_db.my_schema;

-- 6
CREATE OR REPLACE TABLE my_schema.EMPLOYEE (
    Employee_ID NUMBER,
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    EMAIL VARCHAR(255),
    PHONE_NUMBER VARCHAR(255),
    salary string,
    department_id number,
    etl_ts timestamp default current_timestamp(),
    -- for getting the time at which the record is getting inserted
    etl_by string default 'snowsight',
    -- for getting application name from which the record was inserted
    file_name string -- for getting the name of the file used to insert data into the table.
);

-- 7
CREATE OR REPLACE FILE FORMAT my_json_format TYPE = JSON;
-- -- Creating a table variant_table which holds the raw data, i.e all data as one in variant coloumn
CREATE TABLE variant_table(variant_data variant);



-- 8
-- External stage
CREATE or replace STAGE external_stage
url = 's3://snowflake-assgn';
list @external_stage;

-- Internal stages
CREATE OR REPLACE STAGE internal_stage
  file_format = (type = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1);
  
-- Using snowsql run the following command
put file:///Users/kush/Desktop/employees.csv @internal_stage;
list @internal_stage;



-- 9
-- Copying from external stage into employee
CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'csv' FIELD_DELIMITER = ';' SKIP_HEADER = 1;



COPY INTO employee(Employee_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE_NUMBER,salary,department_id,file_name
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
            emp.$7,
            METADATA$FILENAME
        FROM
            @internal_stage/employees.csv.gz (file_format => my_csv_format) emp
    );  

SELECT * FROM employee;
truncate employee;
COPY INTO variant_table FROM (
SELECT PARSE_JSON('{
    "Employee_ID": "' || $1 || '",
    "first_name": "' || $2 || '",
    "last_name": "' || $3 || '",
    "email": "' || $4 || '",
    "phone_number": "' || $5 || '",
    "salary" : "' || $6 || '",
    "department_id" : "' || $7 || '",
    "elt_ts" : "' || CURRENT_TIMESTAMP() || '",
    "elt_by" : "snowsight",
    "file_name" : "' || METADATA$FILENAME || '"
  }')
  FROM @external_stage/employees.csv
  (file_format => my_csv_format) emp
  );

  select * from variant_table;

select ($1) from  @internal_stage/employees.csv.gz(file_format => my_csv_format) ;

-- 10
-- First, create a stage 
CREATE FILE FORMAT my_parquet_format TYPE = parquet;
CREATE OR REPLACE STAGE my_schema.my_parquet_stage file_format =my_parquet_format;

-- Upload a Parquet file to the stage using snowsql
put file:///Users/kush/Desktop/userdata1.parquet  @my_parquet_stage;
list @my_parquet_stage;

-- Infer the schema
SELECT
    *
FROM
    TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_parquet_stage',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

-- 11
SELECT $1:birthdate::String as bday from @my_parquet_stage;   



-- Question - 12
    -- Add masking policy to the PII columns such that fields like email, phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible
USE ROle ACCOUNTADMIN;
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string -> CASE
    WHEN CURRENT_ROLE() = 'PII' THEN VAL
    ELSE '****MASK****'
END;

CREATE OR REPLACE MASKING POLICY contact_Mask AS (VAL string) RETURNS string -> CASE
    WHEN CURRENT_ROLE() = 'PII' THEN VAL
    ELSE '****MASK****'
END;


-- Adding the email_mask policy to employee
ALTER TABLE IF EXISTS employee MODIFY EMAIL SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee MODIFY phone_number SET MASKING POLICY contact_mask;

SELECT * FROM employee;

-- Displaying data from PII view

-- Granting required previlages to role PII
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee TO ROLE PII;

USE ROLE PII;
SELECT * FROM employee LIMIT 10;



USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee TO ROLE DEVELOPER;

USE ROLE DEVELOPER;
-- Displaying data from developer role
SELECT * FROM employee LIMIT 10;




