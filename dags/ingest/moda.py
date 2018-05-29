# TODO: port this stuff:
        # # =========================================================================
        # # === unzip granule l1a data
        # # =========================================================================
        # # === option 1: l1a file already exists so we have nothing to do
        # # TODO: change to mysqlOperator which checks metadata db
        # # check_for_extant_l1a_file = BashOperator(
        # #     task_id='check_for_extant_l1a_file',
        # #     bash_command='[[ -s {{ params.filepather.myd01(execution_date, params.roi) }} ]]',
        # #     params={
        # #         'filepather': satfilename,
        # #         'roi': region.place_name
        # #     },
        # #     retries=0,
        # #     retry_delay=timedelta(seconds=3)
        # # )
        # # === option 2: unzip bz2 file from our local archive
        # unzip_l1a_bz2 = BashOperator(
        #     task_id='unzip_l1a_bz2',
        #     bash_command="""
        #         bzip2 -k -c
        #         -d {{ ti.xcom_pull(task_ids="extract_file") }}
        #         > """+TMP_DIR,
        #     params={
        #         'filepather': satfilename,
        #         'roi': region.place_name
        #     },
        #     trigger_rule=TriggerRule.ALL_FAILED,  # only run if upstream fails
        #     retries=0,
        #     retry_delay=timedelta(seconds=5)
        # )
        # # check_for_extant_l1a_file >> unzip_l1a_bz2

        # # =========================================================================
