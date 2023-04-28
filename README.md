
## snowflake

<b>1.</b> Created role admin, developer and PII using create role command and granted role as per required hierarchy. <br>

<b>2.</b> Created a warehouse assignnemnt_wh using create warehouse command<br>

<b>3.</b> Switched to admin role using use role command/<br>

<b>4.</b> Created a database, assignment_db using create database command <br>

<b>5.</b> Created a schema, myschema using create schema command<br>

<b>6.</b> Created a table employee with required columns.<br>

<b>7.</b> Created a variant table with variant datatype<br>

<b>8.</b> Created an external and an internal stage and loaded the data into external stage using aws bucket and for internal bucket used snowsql and loaded the data from the system<br>

<b>9.</b> In the employee table, loaded the data from the internal stage, and in the variant table loaded the data from external stage using PARSE_JSON<br>

<b>10.</b> created a local stage named my_parquet_stage and loaded the parquet file using snowsql. Then inferred the schema using INFER_SCHEMA function<br>

<b>11.</b> Used select statement to perform the step by selecting the first coloumn birthday as bday<br>

<b>12.</b>Using role accountadmin created two masking policies email_mask and contact_mask. Updated the employee table by modifying with the mask created. Gave required permissions to roles PII and DEVELOPER. We can query the table using both roles and see the changes.
