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
        # #     retries=1,
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
        #     retries=1,
        #     retry_delay=timedelta(seconds=5)
        # )
        # # check_for_extant_l1a_file >> unzip_l1a_bz2
        # # === option 3: download the granule
        # # reads the download url from a metadata file created in the last step and
        # # downloads the file iff the file does not already exist.
        # download_granule = BashOperator(
        #     task_id='download_granule',
        #     bash_command="""
        #         METADATA_FILE={{ params.filepather.metadata(execution_date, params.roi) }} &&
        #         OUT_PATH={{ params.filepather.myd01(execution_date, params.roi) }}         &&
        #         FILE_URL=$(grep "^upstream_download_link" $METADATA_FILE | cut -d'=' -f2-) &&
        #         [[ -s $OUT_PATH ]] &&
        #         echo "file already exists; skipping download." ||
        #         curl --user {{params.username}}:{{params.password}} -f $FILE_URL -o $OUT_PATH
        #         && [[ -s $OUT_PATH ]]
        #     """,
        #     params={
        #         "filepather": satfilename,
        #         "username": secrets.ESDIS_USER,
        #         "password": secrets.ESDIS_PASS,
        #         "roi": region.place_name
        #     },
        #     trigger_rule=TriggerRule.ALL_FAILED  # only run if upstream fails
        # )
        # unzip_l1a_bz2 >> download_granule
        # # =========================================================================
