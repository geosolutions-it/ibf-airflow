import logging
import os
from subprocess import check_output, STDOUT, CalledProcessError

import six
from airflow.models import XCOM_RETURN_KEY
from airflow.operators import BaseOperator
from airflow.operators import BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


def get_overview_levels(max_level):
    levels = []
    current = 2
    while current <= max_level:
        levels.append(current)
        current *= 2
    return levels


def get_gdaladdo_command(source, overview_levels, resampling_method,
                         compress_overview=None):
    compress_token = (
        "--config COMPRESS_OVERVIEW {}".format(compress_overview) if
        compress_overview is not None else ""
    )
    return "gdaladdo {compress} -r {method} {src} {levels}".format(
        method=resampling_method,
        compress=compress_token,
        src=source,
        levels=" ".join(str(level) for level in overview_levels),
    )


def get_gdal_translate_command(source, destination, output_type,
                               creation_options):
    return (
        "gdal_translate -ot {output_type} {creation_opts} "
        "{src} {dst}".format(
            output_type=output_type,
            creation_opts=_get_gdal_creation_options(**creation_options),
            src=source,
            dst=destination
        )
    )


def _get_gdal_creation_options(**creation_options):
    result = ""
    for name, value in creation_options.items():
        opt = '-co "{}={}"'.format(name.upper(), value)
        result = " ".join((result, opt))
    return result


class GDALWarpOperator(BaseOperator):
    """ Execute gdalwarp with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        target_srs (str): parameter for gdalwarp
        tile_size (str): parameter for gdalwarp
        overwrite (str): parameter for gdalwarp
        dstdir (str): output files directory
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        list: list of output files path
    """

    @apply_defaults
    def __init__(self, target_srs, tile_size, overwrite, dstdir, get_inputs_from=None,
                 *args, **kwargs):
        self.target_srs = target_srs
        self.tile_size = str(tile_size)
        self.overwrite = overwrite
        self.dstdir = dstdir
        self.get_inputs_from = get_inputs_from

        super(GDALWarpOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Warp running------------')
        task_instance = context['task_instance']
        log.info("""
            target_srs: {}
            tile_size: {}
            overwrite: {}
            dstdir: {}
            get_inputs_from: {}
            """.format(
            self.target_srs,
            self.tile_size,
            self.overwrite,
            self.dstdir,
            self.get_inputs_from,
        )
        )

        dstdir = self.dstdir

        input_paths = task_instance.xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)
        if input_paths is None:
            log.info('Nothing to process')
            return None

        output_paths = []
        for srcfile in input_paths:
            log.info('srcfile: %s', srcfile)
            srcfilename = os.path.basename(srcfile)
            dstfile = os.path.join(dstdir, srcfilename)
            log.info('dstfile: %s', dstfile)

            # build gdalwarp command
            self.overwrite = '-overwrite' if self.overwrite else ''
            gdalwarp_command = (
                    'gdalwarp ' + self.overwrite + ' -t_srs ' + self.target_srs +
                    ' -co TILED=YES -co BLOCKXSIZE=' + self.tile_size +
                    ' -co BLOCKYSIZE=' + self.tile_size + ' ' + srcfile + ' ' +
                    dstfile
            )
            log.info('The complete GDAL warp command is: %s', gdalwarp_command)
            env = {
                'LD_LIBRARY_PATH': '/usr/local/lib:$LD_LIBRARY_PATH'
            }
            bo = BashOperator(
                task_id="bash_operator_warp",
                env=env,
                bash_command=gdalwarp_command
            )
            bo.execute(context)
            output_paths.append(dstfile)

        return output_paths


class GDALAddoOperator(BaseOperator):
    """ Execute gdaladdo with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        resampling_method (str): parameter for gdaladdo
        max_overview_level (str): parameter for gdaladdo
        compress_overview (str): parameter for gdaladdo
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        list: list containing output files path
    """

    @apply_defaults
    def __init__(self, get_inputs_from, resampling_method,
                 max_overview_level, compress_overview=None,
                 *args, **kwargs):
        super(GDALAddoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.resampling_method = resampling_method
        self.max_overview_level = int(max_overview_level)
        self.compress_overview = compress_overview

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)
        if input_paths is None:
            log.info("Nothing to process")
            return None

        output_paths = []
        for input_path in input_paths:
            levels = get_overview_levels(self.max_overview_level)
            log.info("Generating overviews for {!r}...".format(input_path))
            command = get_gdaladdo_command(
                input_path, overview_levels=levels,
                resampling_method=self.resampling_method,
                compress_overview=self.compress_overview
            )
            output_path = input_path
            output_paths.append(output_path)
            env = {
                'LD_LIBRARY_PATH': '/usr/local/lib:$LD_LIBRARY_PATH'
            }
            bo = BashOperator(
                task_id='bash_operator_addo_{}'.format(
                    os.path.basename(input_path)),
                env=env,
                bash_command=command
            )
            bo.execute(context)

        log.info(output_paths)
        return output_paths


class GDALTranslateOperator(BaseOperator):
    """ Execute gdaltranslate with given options on file fetched from XCom. Returns output file paths to XCom.

    Args:
        creation_options (str): parameter for gdaltranslate
        output_type (str): parameter for gdaltranslate
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        list: list containing output files path
    """

    @apply_defaults
    def __init__(self, get_inputs_from, output_type="UInt16",
                 creation_options=None, *args, **kwargs):
        super(GDALTranslateOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.output_type = str(output_type)
        self.creation_options = dict(
            creation_options) if creation_options is not None else {
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
        }

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)

        if input_paths is None:
            log.info("Nothing to process")
            return None

        # If message from XCom is a string with single file path, turn it into a string
        if isinstance(input_paths, six.string_types):
            input_paths = [input_paths]

        if not len(input_paths):
            log.info("Nothing to process")
            return None

        log.info(input_paths[0])
        working_dir = os.path.dirname(input_paths[0])
        try:
            os.makedirs(working_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        output_paths = []
        for input_path in input_paths:
            output_img_filename = 'translated_{}'.format(
                os.path.basename(input_path))
            output_path = os.path.join(working_dir, output_img_filename)
            output_paths.append(output_path)
            command = get_gdal_translate_command(
                source=input_path, destination=output_path,
                output_type=self.output_type,
                creation_options=self.creation_options
            )

            log.info("The complete GDAL translate command is: {}".format(command))
            env = {
                'LD_LIBRARY_PATH': '/usr/local/lib:$LD_LIBRARY_PATH'
            }
            b_o = BashOperator(
                task_id="bash_operator_translate",
                env=env,
                bash_command=command
            )
            b_o.execute(context)

        log.info(output_paths)
        return output_paths


class GDALInfoOperator(BaseOperator):
    """ Execute gdalinfo with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        dict: dictionary mapping input files to the matching gdalinfo output
    """

    @apply_defaults
    def __init__(self, get_inputs_from, *args, **kwargs):
        super(GDALInfoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)

        if input_paths is None:
            log.info("Nothing to process")
            return None

        gdalinfo_outputs = {}
        for input_path in input_paths:
            command = ["gdalinfo"]
            log.info("Running GDALInfo on {}...".format(input_path))
            command.append(input_path)
            try:
                env = {
                    'LD_LIBRARY_PATH': '/usr/local/lib:$LD_LIBRARY_PATH'
                }
                gdalinfo_output = check_output(' '.join(command), env=env, shell=True, stderr=STDOUT)
                log.info("{}".format(gdalinfo_output))
                gdalinfo_outputs[input_path] = gdalinfo_output
            except CalledProcessError as e:
                raise RuntimeError(
                    "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

        return gdalinfo_outputs


class GDALInfoEGEOSValidOperator(BaseOperator):
    """ Execute gdaladdo with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        get_inputs_from (str): task_id used to fetch input files list from XCom
        env_parameter (str): one of {"MSAVI", "NDVI", "RGB"}
        wrg_dir (str): absolute path where wrong files will be moved

    Returns:
        list: list containing output files path
    """

    @apply_defaults
    def __init__(self, get_inputs_from, env_parameter, wrg_dir, *args, **kwargs):
        super(GDALInfoEGEOSValidOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.env_parameter = env_parameter
        self.wrg_dir = wrg_dir

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)

        if input_paths is None:
            log.info("Nothing to process")
            return None

        # If message from XCom is a string with single file path, turn it into a string
        if isinstance(input_paths, six.string_types):
            input_paths = [input_paths]

        if not len(input_paths):
            log.info("Nothing to process")
            return None

        log.info(input_paths[0])
        working_dir = os.path.dirname(input_paths[0])
        try:
            os.makedirs(working_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        output_paths = []
        for input_path in input_paths:
            command = ["gdalinfo", "-stats"]
            log.info("Running GDALInfo on {}...".format(input_path))
            command.append(input_path)
            try:
                env = {
                    'LD_LIBRARY_PATH': '/usr/local/lib:$LD_LIBRARY_PATH'
                }
                gdalinfo_output = check_output(' '.join(command), env=env, shell=True, stderr=STDOUT)
                log.info("{}".format(gdalinfo_output.decode()))
            except CalledProcessError as e:
                raise RuntimeError(
                    "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
            return_message = None
            tif_compression = 'No compression'
            for lines in gdalinfo_output.decode().split("\n"):
                if len(lines.split(' ')) > 1 and lines.split(" ")[0] + ' ' + lines.split(' ')[1] == 'Size is':
                    tif_file_size = [int((lines.split(" ")[2]).split(',')[0]), int(lines.split(" ")[3])]
                    if not (4000 <= tif_file_size[0] <= 4002 or 4000 <= tif_file_size[1] <= 4002):
                        return_message = '[incorrect image size {} {}]'.format(tif_file_size[0], tif_file_size[1])
                        break
                elif len(lines.split(' ')) > 1 and lines.split(" ")[0] + ' ' + lines.split(' ')[1] == 'Pixel Size':
                    tif_pixel_size = [float(((lines.split(" ")[3]).split(',')[0])[1:]),
                                      float(((lines.split(" ")[3]).split(',')[1])[0:-1])]
                    if tif_pixel_size[0] != 10 or tif_pixel_size[1] != -10:
                        return_message = '[incorrect pixel size {} {}]'.format(tif_pixel_size[0], tif_pixel_size[1])
                        break
                elif len(lines.split(' ')) > 1 and lines.split("=")[0] == '  COMPRESSION':
                    tif_compression = lines.split('=')[1]
                    if tif_compression != 'DEFLATE':
                        return_message = '[incorrect Compression type {}]'.format(tif_compression)
                        break
                elif len(lines.split(' ')) > 1 and lines.split("=")[0] == '    STATISTICS_MAXIMUM':
                    tif_MAX_Stats = int((lines.split('=')[1]))
                    if tif_MAX_Stats < 1000 or tif_MAX_Stats > 12000:
                        return_message = '[incorrect STATISTICS_MAXIMUM {}]'.format(tif_MAX_Stats)
                        break
                elif len(lines.split(' ')) > 1 and lines.split("=")[0] == '    STATISTICS_MINIMUM':
                    tif_MIN_Stats = int((lines.split('=')[1]))
                    if tif_MIN_Stats < -12000 or tif_MIN_Stats > 1000:
                        return_message = '[incorrect STATISTICS_MINIMUM {}]'.format(tif_MIN_Stats)
                        break
                elif len(lines.split(' ')) > 1 and lines.split("=")[0] == '    STATISTICS_VALID_PERCENT':
                    tif_valid_perc = float((lines.split('=')[1]))
                    if tif_valid_perc <= 2:
                        return_message = '[incorrect STATISTICS_VALID_PERCENT {}]'.format(tif_valid_perc)
                        break
                elif len(lines.split(' ')) > 1 and lines[:14] == '  NoData Value':
                    tif_no_data = lines.split(' ')[3][6:]
                    if self.env_parameter in ('MSAVI', 'NDVI'):
                        if tif_no_data != '-32768':
                            return_message = '[incorrect NO-DATA value {}]'.format(tif_no_data)
                            break
                    elif self.env_parameter == 'RGB':
                        if tif_no_data != '0':
                            return_message = '[incorrect NO-DATA value {}]'.format(tif_no_data)
                            break
            if return_message or tif_compression != 'DEFLATE':
                log.info("ERROR: Image '{}' is not valid accordingly to E-GEOS checks {}".format(input_path, return_message))
                dst_filename = os.path.join(self.wrg_dir, os.path.basename(input_path))
                log.info("Moving {} to {}".format(input_path, dst_filename))
                os.rename(input_path, dst_filename)
                _base_name, _file_extension = os.path.splitext(dst_filename)
                for _ext in ('.md5', '.md5_processed', '{}.aux.xml'.format(_file_extension)):
                    _aux_file = input_path.replace(_file_extension, _ext)
                    if os.path.exists(_aux_file):
                        dst_filename = os.path.join(self.wrg_dir, os.path.basename(_aux_file))
                        log.info("Moving {} to {}".format(_aux_file, dst_filename))
                        os.rename(_aux_file, dst_filename)
            else:
                log.info("OK: Image '{}' is valid accordingly to E-GEOS checks...".format(input_path))
                output_path = input_path
                output_paths.append(output_path)

        log.info(output_paths)
        return output_paths


class GDALPlugin(AirflowPlugin):
    name = "GDAL_plugin"
    operators = [
        GDALWarpOperator,
        GDALAddoOperator,
        GDALTranslateOperator,
        GDALInfoOperator,
        GDALInfoEGEOSValidOperator
    ]
