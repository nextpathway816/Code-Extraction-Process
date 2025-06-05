#!/bin/sh

source $(pwd)/create_ddls.btq

tm=`date +%Y-%m-%d_%H%M%S`

out_dir=$(pwd)/Results_$tm/Teradata_Source_Extract

mkdir -p $out_dir

# list of databases


include_databases=" IN ('PRDETL', 'PRDUTIL')"
exclude_databases=" NOT IN ('DBC')"

log=${out_dir}/create_ddls.log
summary=${out_dir}/Object_Type_Summary.log
object_types=${out_dir}/Object_Type_List.log

tbl_list=${out_dir}/table_names.txt
sp_list=${out_dir}/${host}_sp_list.txt
show_sp=${out_dir}/SHOW_Procedures.sql
show_tbl=${out_dir}/SHOW_Tables.sql
show_views=${out_dir}/SHOW_Views.sql

sp_ddl=${out_dir}/DDL_Procedures.sql
tbl_ddl=${out_dir}/DDL_Tables.sql
view_ddl=${out_dir}/DDL_Views.sql
db_ddl=${out_dir}/DDL_Database.sql
drop_db=${out_dir}/DDL_Drop_Databases.sql


echo -e "\n\n\n*****  START OF TERADATA SOURCE EXTRACTION   *****\n\n\n"

###########################################################################
# Extract DDL for Tables
###########################################################################
echo "[`date +%F\ %r`] Process started for Table DDL extraction">>log$
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${show_tbl};

.SET PAGEBREAK OFF;

SELECT 'SHOW TABLE '||T.DatabaseName ||'.' || T.TableName||';' (TITLE '')
FROM    DBC.TablesV T
WHERE  T.TableKind in ('T') and upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT

ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to extract table list completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to extract table list">>$log
fi


bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${tbl_ddl};

.SET PAGEBREAK OFF;

.RUN FILE='${show_tbl}'
.EXPORT RESET
.logoff;
.quit;
EOT


###########################################################################
# Extract DDL for Views
###########################################################################
echo "[`date +%F\ %r`] Process started for View DDL extraction">>$log
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${show_views};

.SET PAGEBREAK OFF;

SELECT 'SHOW VIEW '||T.DatabaseName ||'.' || T.TableName||';' (TITLE '')
FROM    DBC.TablesV T
WHERE  T.TableKind in ('V') and upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT

ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to extract view list completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to extract view list">>$log
fi


bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${view_ddl};

.SET PAGEBREAK OFF;

.RUN FILE='${show_views}'
.EXPORT RESET
.logoff;
.quit;
EOT



###########################################################################
# Extract DDL for Procedures
###########################################################################
echo "[`date +%F\ %r`] Process started for Procedures DDL extraction">>$log
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${show_sp};

.SET PAGEBREAK OFF;

SELECT 'SHOW PROCEDURE '||T.DatabaseName ||'.' || T.TableName||';' (TITLE '')
FROM    DBC.TablesV T
WHERE  T.TableKind in ('P' , 'E') and upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT

ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to extract procedure list completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to extract procedure list">>$log
fi


bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${sp_ddl};

.SET PAGEBREAK OFF;

.RUN FILE='${show_sp}'
.EXPORT RESET
.logoff;
.quit;
EOT


###########################################################################
# Extract Object Types
###########################################################################
echo "[`date +%F\ %r`] Process started for Object Types extraction">>$log
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${object_types};

.SET PAGEBREAK OFF;

SELECT distinct upper(DatabaseName), T.TableKind
FROM    DBC.TablesV T
WHERE  upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT

ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to extract Object Types completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to extract Object Types">>$log

fi

###########################################################################
# Extract Objects Summary
###########################################################################
echo "[`date +%F\ %r`] Process started for Objects Summary extraction">>$log
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${summary};

.SET PAGEBREAK OFF;

SELECT upper(DatabaseName), T.TableKind,T.TableName
FROM    DBC.TablesV T
WHERE  upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT

ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to extract Object Summary completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to extract Object Summary">>$log

fi
###########################################################################
# Extract Database Names
###########################################################################
echo "[`date +%F\ %r`] Process started for Database CREATE statements">>$log
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${db_ddl};

.SET PAGEBREAK OFF;

SELECT 'CREATE DATABASE '||T.DatabaseName ||'  FROM '||ownername || 'AS PERMANENT = '||permspace||', SPOOL = '||spoolspace||', ACCOUNT = '||accountname||';' (TITLE '')
FROM    DBC.databases T
WHERE  upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT


ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to create Database CREATE statements completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to extract Databases">>$log

fi

###########################################################################
# Drop Database
###########################################################################
echo "[`date +%F\ %r`] Process started for Database DROP statements">>$log
bteq << EOT
.logon ${host}/${username},${password};
.SET TITLEDASHES OFF
.SET ECHOREQ OFF;
.SET RECORDMODE OFF;
.SET FORMAT OFF;
.SET PAGEBREAK OFF;
.width 200000
.SET SEPARATOR '|';

.EXPORT REPORT FILE = ${drop_db};

.SET PAGEBREAK OFF;

SELECT 'DROP DATABASE '||T.DatabaseName ||';' (TITLE '')
FROM    DBC.databases T
WHERE  upper(DatabaseName) ${include_databases} AND upper(DatabaseName) ${exclude_databases};
.EXPORT RESET
.logoff;
.quit;
EOT

ReturnCode=$?

if [[ ${ReturnCode} -eq 0 ]]; then
        echo "[`date +%F\ %r`] BTEQ script to CREATE DROP STATEMENTS completed successfully">>$log
else
        echo "[`date +%F\ %r`] BTEQ script failled to CREATE DROP STATEMENTS">>$log

fi

echo -e "\n\n\n*****  END OF TERADATA SOURCE EXTRACTION   *****\n\n\n"

echo -e "Please verify and zip the files at below path and share with Next Pathway. \nResults Path:   $out_dir\n\n"