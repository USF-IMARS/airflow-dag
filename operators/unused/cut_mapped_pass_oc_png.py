from airflow.operators.bash_operator import BashOperator

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import QUEUE
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS

# =============================================================================
# === IMaRS chlor_a oc_png cut_mapped_pass PngGenerator
# =============================================================================
# Ported from [IMaRS IPOPP SPA](https://github.com/USF-IMARS/imars)
#     * SPA=imars
#     * station=oc_png station [cfgfile](https://github.com/USF-IMARS/imars/blob/master/station/oc_png/station.cfgfile)
#     * algorithm=cut_mapped_pass
#         * [generic.xml](https://github.com/USF-IMARS/imars/blob/master/wrapper/cut_mapped_pass/generic.xml)
#         * [installation.xml](https://github.com/USF-IMARS/imars/blob/master/wrapper/cut_mapped_pass/installation.xml)
#
# Ncs_run.cmd="{generate_thumbnail} -f {input_file} -m {{mask_file}}
#   -c {conversion} -o {output_file} -s {sds} -n {no_data} -l {valid_min}
#   -u {valid_max} {coordinates}"
#
# construct one of these tasks for each region:
# for region in REGIONS:
#     oc_png_region = BashOperator(
#         task_id='oc_png_'+region['place_name'],
#         bash_command="""
#             /opt/sat-scripts/sat-scripts/PngGenerator.py
#                 -f {{ params.l2_pather(execution_date) }}
#                 -m /opt/sat-scripts/masks/{{params.product}}_{{params.place_name}}_{{params.sensor}}mask
#                 -c {{params.conversion}}
#                 -o {{params.png_pather(execution_date, params.place_name)}}
#                 -s {{params.sds}}
#                 -n {{params.no_data}}
#                 -l {{params.valid_min}}
#                 -u {{params.valid_max}}
#                 {{params.coordinates}}
#         """,
#         params={
#             'l2_pather':  satfilename.l2,
#             'png_pather': satfilename.png,
#             'product':"chlor_a",
#             'sds': "chlor_a",
#             'sat': 'aqua',
#             'place_name':region['place_name'],
#             'sensor':'modis',
#             # This is a python equation for each thumbnails.
#             #   DO NOT PUT SPACES INSIDE THE EQUATION, but separate equations
#             #   with spaces. Input must be data.
#             'conversion': 'np.log10(data+1)/0.00519 250*np.log10((0.59*(data*5)**.86)+1.025)/np.log10(2)',
#             'no_data': 0, # seadas raw value for no data 0 for hdf5, was -32767
#             'valid_min': 'NaN',
#             'valid_max': 'NaN',
#             'coordinates': (  # -w {latmax} -x {lonmax} -y {latmin} -z {lonmin}"
#                 " -w " + str(region['latmax']) +
#                 " -x " + str(region['lonmax']) +
#                 " -y " + str(region['latmin']) +
#                 " -z " + str(region['lonmin'])
#             )
#             # should also include source_{N/S/E/W} vars?
#             # "-a {source_north} -d {source_east} -e {source_south} -g {source_west}
#         },
#         dag=modis_aqua_processing,
#         queue=QUEUE.SAT_SCRIPTS
#     )
#     # TODO: set imars.{sat}.{sensor}.{product_family}.mapped as upstream
#     l2gen >> oc_png_region
# =============================================================================
