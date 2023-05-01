1. Created roles using SQL commands under the given constraints
2. Created the warehouse using SQL command CREATE WAREHOUSE
3. Granted privileges to the role ADMIN for the new warehouse created
4. Switched to the admin role and created the database using SQL command and then created the schema
5. Created an employee table and copied data, but used default name for filename column instead of using any function. Also there is no staging area created as of now to copy data.
6. Created an employee variant dataset and copied data from the staging area.
7. Created an internal stage and loaded the file using snowsql.
8. For external stage, created a storage integraton for aws bucket, and then created an external staging area.
9. Created two new tables and copied data from the staging area created.
10. Created a stage for the parquet file and uploaded the parquet file using snowsql.
11. Using select * command, queried the data from the staging area.
12. Created a masking policy for email and altered the employee table column.
13. Granted requied permissions for developer and PII role and then queried the table using both roles.
