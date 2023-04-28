
## snowflake

<b>1.</b> Created role admin, developer and PII using create role command and granted role as per required hierarchy. <br>

<img width="314" alt="Screenshot 2023-04-28 at 3 09 34 PM" src="https://user-images.githubusercontent.com/54627996/235113458-8c716b7c-f771-4459-a84d-14dc895749aa.png">

```
create role Admin;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;

CREATE ROLE DEVELOPER;
GRANT ROLE DEVELOPER TO ROLE ADMIN;

CREATE ROLE PII;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;
```


<b>2.</b> Created a warehouse assignnemnt_wh using create warehouse command<br>
```
CREATE WAREHOUSE IF NOT EXISTS assignment_wh
WAREHOUSE_SIZE = 'MEDIUM'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
```

Grant permissions to create a database and access the warehouse to the Admin role
```
GRANT CREATE DATABASE ON account TO ROLE Admin;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE Admin;
```

<b>3.</b> Switched to admin role using use role command<br>
```
USE ROLE ADMIN;
```

<b>4.</b> Created a database, assignment_db using create database command <br>
```
CREATE DATABASE IF NOT EXISTS assignment_db;
```

<b>5.</b> Created a schema, myschema using create schema command<br>
```
CREATE SCHEMA IF NOT EXISTS assignment_db.my_schema;
```

<b>6.</b> Created a table employee with required columns.<br>
```
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
```

<b>7.</b> Created a variant table with variant datatype<br>
```
CREATE TABLE variant_table(variant_data variant);
```

<b>8.</b> Created an external stage and loaded the data using aws bucket
```
CREATE or replace STAGE external_stage
url = 's3://snowflake-assgn';
```
```
list @external_stage;
```
<img width="1145" alt="Screenshot 2023-04-28 at 3 15 43 PM" src="https://user-images.githubusercontent.com/54627996/235114783-2c40f17a-b74e-4bd8-b98b-bce5e3bcce0c.png">


Created an internal stage 
```
CREATE OR REPLACE STAGE internal_stage
  file_format = (type = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1);
```
For loading data, run the following command into snowsql
```
put file:///Users/kush/Desktop/employees.csv @internal_stage;
```

The result : <br>
<img width="921" alt="Screenshot 2023-04-28 at 3 17 08 PM" src="https://user-images.githubusercontent.com/54627996/235115085-3866c0e5-a570-4bfc-b431-1768a4fb4648.png">



<b>9.</b> Created a file format my_csv_format
```
CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'csv' FIELD_DELIMITER = ';' SKIP_HEADER = 1;
```
Using copy into command, copied the data to employee table from internal stage
```
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
```

For the command : 
``` 
Select * from employees LIMIT 10;
```
<img width="1431" alt="Screenshot 2023-04-28 at 3 26 53 PM" src="https://user-images.githubusercontent.com/54627996/235117352-bf06e4d9-d8cc-45c5-8dd5-71206551169e.png">

Using copy into command and parse json function copied the data into data variant table 
```
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
```
<img width="1432" alt="Screenshot 2023-04-28 at 3 33 02 PM" src="https://user-images.githubusercontent.com/54627996/235118639-208f2aa2-f0dd-454f-89dc-8de430fd7411.png">


<b>10.</b> created a local stage named my_parquet_stage and loaded the parquet file using snowsql. Then inferred the schema using INFER_SCHEMA function<br>
```
CREATE FILE FORMAT my_parquet_format TYPE = parquet;
CREATE OR REPLACE STAGE my_schema.my_parquet_stage file_format =my_parquet_format;
```

```
put file:///Users/kush/Desktop/userdata1.parquet  @my_parquet_stage;
```
<img width="975" alt="Screenshot 2023-04-28 at 3 35 18 PM" src="https://user-images.githubusercontent.com/54627996/235119154-69b0d1f2-0e09-4d08-b384-3c4651d88745.png">

```
list @my_parquet_stage;
```
<img width="1426" alt="Screenshot 2023-04-28 at 3 36 06 PM" src="https://user-images.githubusercontent.com/54627996/235119314-9720a38b-70e7-4a4f-8667-b29f0246a1d0.png">

For inferring the schema : 
```
SELECT
    *
FROM
    TABLE(
        INFER_SCHEMA(
            LOCATION => '@my_parquet_stage',
            FILE_FORMAT => 'my_parquet_format'
        )
    );
```
<img width="1429" alt="Screenshot 2023-04-28 at 3 37 29 PM" src="https://user-images.githubusercontent.com/54627996/235119584-b0eb8e09-8df8-41a8-9104-608412074dd0.png">

<b>11.</b> Used select statement to perform the step by selecting the first coloumn birthday as bday<br>
```
SELECT $1:birthdate::String as bday from @my_parquet_stage;  
```
<img width="765" alt="Screenshot 2023-04-28 at 3 38 19 PM" src="https://user-images.githubusercontent.com/54627996/235119735-aab7f186-79e1-4c41-a2bb-1f545a049799.png">



<b>12.</b>Using role accountadmin created two masking policies email_mask and contact_mask.
```
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string -> CASE
    WHEN CURRENT_ROLE() = 'PII' THEN VAL
    ELSE '****MASK****'
END;

CREATE OR REPLACE MASKING POLICY contact_Mask AS (VAL string) RETURNS string -> CASE
    WHEN CURRENT_ROLE() = 'PII' THEN VAL
    ELSE '****MASK****'
END;
```

Adding the email_mask policy to employee
```
ALTER TABLE IF EXISTS employee MODIFY EMAIL SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee MODIFY phone_number SET MASKING POLICY contact_mask;
```

Granting required previlages to role PII
```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee TO ROLE PII;
```

```
USE ROLE PII;
SELECT * FROM employee LIMIT 10;
```
<img width="1419" alt="Screenshot 2023-04-28 at 3 40 12 PM" src="https://user-images.githubusercontent.com/54627996/235120108-e82ca50d-10f4-4c5c-b4e3-a70ee7011c41.png">


Granting required previlages to role developer
```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee TO ROLE DEVELOPER;
```

```
USE ROLE DEVELOPER;
SELECT * FROM employee LIMIT 10;
```

<img width="1438" alt="Screenshot 2023-04-28 at 3 45 48 PM" src="https://user-images.githubusercontent.com/54627996/235121263-e098dfb6-05c2-4a08-9eec-510aeaa551ce.png">


