"""
Processing DAG segmented by geographical region of interest (ROI).

```
from imars_dags.regions.regions import regions

# --- gom.py
# just one
gom_sst = RegionDAG(short_name="gom", root_dag='sst')
gom_chl = RegionDAG(short_name="gom", root_dag='chl')
# RegionDag does:
# import imars_dags.sst; gom_sst.add_tasks(RegionOpts);

# --- sst.py
# all regions
ROOT_DAG_NAME = "sst"
REGIONAL_DAG_OPTIONS = {
    REGION_SHORT_NAME.GOM: {  # regions enum
        "args":[],
        "kwargs":{},
    },
    ...
}
for r_key in DAG_OPTIONS:
    dag_options = DAG_OPTIONS[r_key]
    RegionDAGBuilder(
        short_name=r_key,
        root_dag=ROOT_DAG_NAME,
        dag_builder_params=dag_options
    ).build()
"""

from airflow import DAG

from imars_dags.regions.regions import regions

class RegionDAG(DAG):
    def __init__(self, *args, **kwargs):
        """
        parameters:
        -----------
        short_name : str
            region short_name
        dag_builder : str
            name of dag we are using on this region.
        dag_builder_params : dict
            dict in form {"args":[], "kwargs":{}} that gets passed to the
            dag_builder
        """
        self.short_name = kwargs.pop('short_name')
        self.root_dag_name = kwargs.pop('root_dag_name')
        self.dag_builder_params = kwargs.pop('dag_builder_params')
        kwargs['dag_id'] = self.region + '_' + self.root_dag

        # TODO: ... something?

        super(RegionDAG, self).__init__(*args, **kwargs)
