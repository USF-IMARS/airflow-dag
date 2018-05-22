import logging

from airflow.operators.python_operator import PythonOperator

def add_extract(dag, sql_selector, test):
    with dag as dag:
        # === Extract
        # ======================================================================
        def extract_file(**kwargs):
            """
            Extracts a file from the remote system & makes it avaliable locally.
            `templates_dict` should be used to pass sql_selection in the context
            provided by kwargs.

            parameters:
            -----------
            kwargs : keyword-arguments
                kwargs contains the context set by airflow.
                Within this context we expect the following variables:

                `sql_selection` should look something like:
                    'date_time="{{ dt }} AND product_id={{ product_id }}"'

            returns:
            --------
            fname : str
                File path to acess the extracted file locally.
                Because this is returned the path can be accessed by other tasks
                using xcom like:
                `{{ ti.xcom_pull(task_ids="extract_file") }}`
            """
            sql_selection = kwargs['templates_dict']['sql_selection']
            if kwargs['templates_dict']['test'] == "True":
                return "/tmp/fake/file.name"
            else:
                fname = imars_etl.extract({
                    "sql":sql_selection
                })
                print(       "extracting product matching SQL:\n\t" + sql_selection)
                logging.info("extracting product matching SQL:\n\t" + sql_selection)
                # ti.xcom_push(key='fname', value=fname)
                return fname

        extract_file = PythonOperator(
            task_id='extract_file',
            provide_context=True,
            python_callable=extract_file,
            templates_dict={
                "sql_selection": 'date_time="{{ execution_date }}" AND (' + sql_selector + ')',
                "test": str(test)
            }
        )

    return extract_file
