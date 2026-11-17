Teradata DDL extract - Next Pathway

1. In create_ddls.sh, set include_databases and exclude_databases (guide, Step 1).
2. On the first line of create_ddls.btq, replace tdpid, username and password.
3. Run: ./create_ddls.sh
4. Output is written to the "output" folder. Check output/create_ddls.log for "*** Failure".
5. Delete the password from create_ddls.btq before sending the folder.

The scripts only run SELECT and SHOW statements. Nothing on your Teradata system is changed.
DDL_Drop_Databases.sql contains DROP statements written as text only; they are not run.
