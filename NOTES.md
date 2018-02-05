
TODO:
# Transforms given string with a date into another string with a differently formatted date.
#
# transdate /path/to/ingest/dir/1215-file-11.txt -f '%H%M-*-%d.txt'
# find /path/to/ingest/dir/ -exec airflow run my_dag_id transdate {} 'in-fmt-str-%Y%M%D.txt' '%Y-%m-%d' \;

find /dir/ | sed 's#^.A[0-9]{4}[0-9]{3}[0-9]{2}[0-9]{2}00.L1A_LAC.x.hdf.bz2#\1-/2T/3:/4:00#' | (xargs?) airflow run my_dag_id



This is just nasty no matter how I cut it (mostly b/c of the julian day).

Best approach: extend satfilename.py with get-iso8601-date-from-filename option
then use that to spin up with something like:

find /ingest/dir -exec airflow run my_dag_id `satfilename --getDate --type myd01 {}` \;

find /ingest/dir -exec airflow run my_dag_id `mysql -u watev -p=123 -c \
'USE data_api_atomic; SELECT date FROM products WHERE path IS ;'`

INFILE=`mysql -u=u -p=123 -c 'USE data_api_atomic; SELECT full_path FROM files WHERE region="" AND dateTime=exec_date'`
