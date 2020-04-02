#
# DAG
#
# dag_schedule_interval='@hourly'
dag_schedule_interval = '0/1 * * * *'

#
# Input Files
#
id = "S2_NDVI"
filename_filter = "S2* NDVI*"
platformname = 'Sentinel-2 NDVI'

src_dir = '/opt/data_samples/IBF/_input_EGEOS_NDVI'
dst_dir = '/opt/data_samples/IBF/NDVI'
filename_filter = '*.TIF'
regex = r'.*\_([a-zA-Z0-9]{6})\_.*'

#
# GDAL Translate
#
tx_prefix = 'translated_'
output_type = 'Int16'
creation_options = {
    "tiled": True,
    "compress": "DEFLATE",
    "blockxsize": 512,
    "blockysize": 512,
}

#
# GDAL Addo
#
resampling_method = 'nearest'
max_overview_level = 64
compress_overview = True

#
# GeoServer
#
geoserver_rest_url = "http://localhost:8080/geoserver/rest"
geoserver_user = "*********"
geoserver_password = "*********"
geoserver_workspace = "geonode"
geoserver_store_name = "NDVI"
geoserver_layer = "NDVI"
