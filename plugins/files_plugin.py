import fnmatch
import hashlib
import logging
import mmap
import os
import pprint
import re
import sys
from functools import partial

import six
from airflow.models import XCOM_RETURN_KEY
from airflow.models import BaseOperator, SkipMixin, TaskReschedule
from airflow.operators import BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

try:
    from osgeo import gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules, install gdal with python bindings')

from zipfile import ZipFile
import config.xcom_keys as xk

log = logging.getLogger(__name__)


class ZipInspector(BaseOperator, SkipMixin):
    """ ZipInspector takes list of downloaded products and goes through the downloaded product's zipfiles and searches for a defined extension (.tiff)  

    Args:
        extension_to_search (str): image extension to search for 
        get_inputs_from (str): task_id used to fetch input files from XCom (as a list of products)

    Returns:
        dict: keys are zipfiles and values are lists containing virtual paths 
    """

    @apply_defaults
    def __init__(self, extension_to_search, get_inputs_from=None, *args, **kwargs):
        self.substring = extension_to_search
        self.get_inputs_from = get_inputs_from
        log.info('--------------------ZipinspectorOperator------------')
        super(ZipInspector, self).__init__(*args, **kwargs)

    def _do_skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    def execute(self, context):
        zip_files = list()
        if self.get_inputs_from != None:
            zip_files = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)
        else:
            # legacy
            log.info('**** Inside execute ****')
            log.info("ZipInspector Operator params list")
            log.info('Substring to search: %s', self.substring)

            downloaded = context['task_instance'].xcom_pull(task_ids='dhus_download_task', key='downloaded_products')
            zip_files = downloaded.keys()

        # stop processing if there are no products
        if zip_files is None or len(zip_files) == 0:
            log.info("Nothing to process.")
            self._do_skip_downstream_tasks(context)

        return_dict = dict()
        log.info("Processing {} ZIP files:\n{} ".format(len(zip_files), pprint.pformat(zip_files)))
        for zip_file in zip_files:
            log.info("Processing {}..".format(zip_file))
            counter = 0;
            vsi_paths = list()
            zip = ZipFile(zip_file)
            for file in zip.namelist():
                filename = zip.getinfo(file).filename
                if self.substring in filename:
                    counter = counter + 1
                    raster_vsizip = "/vsizip/" + zip_file + "/" + filename
                    vsi_paths.append(raster_vsizip)
                    log.info(str(counter) + ") '" + raster_vsizip + "'")
                    context['task_instance'].xcom_push(key=xk.IMAGE_ZIP_ABS_PATH_PREFIX_XCOM_KEY + str(counter),
                                                       value=raster_vsizip)
            return_dict[zip_file] = vsi_paths
        return return_dict


class RSYNCOperator(BaseOperator, SkipMixin):
    """ RSYNCOperator is using rsync command line to upload files remotely using ssh keys

    Args:
        host (str): hostname/ip of the remote machine
        remote_usr (str): username of the remote account/machine
        ssh_key_file (str): path to the ssh key file
        remote_dir (str): remote directory to receive the uploaded files
        get_inputs_from (str): task_id used to fetch input file(s) from XCom 

    Returns:
        list: list of paths for the uploaded/moved files  
    """

    @apply_defaults
    def __init__(self,
                 host,
                 remote_usr,
                 ssh_key_file,
                 remote_dir,
                 get_inputs_from=None,
                 *args, **kwargs):
        self.host = host
        self.remote_usr = remote_usr
        self.ssh_key_file = ssh_key_file
        self.remote_dir = remote_dir
        self.get_inputs_from = get_inputs_from

        log.info('--------------------RSYNCOperator ------------')
        super(RSYNCOperator, self).__init__(*args, **kwargs)

    def _do_skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    def execute(self, context):
        log.info(context)
        log.info("###########")
        log.info("## RSYNC ##")
        log.info('Host: %s', self.host)
        log.info('User: %s', self.remote_usr)
        log.info('Remote dir: %s', self.remote_dir)
        log.info('SSH Key: %s', self.ssh_key_file)

        # check default XCOM key in task_id 'get_inputs_from'
        files_str = ""
        files = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)

        # stop processing if there are no products
        if files is None:
            log.info("Nothing to process.")
            self._do_skip_downstream_tasks(context)

        if isinstance(files, six.string_types):
            files_str = files
        else:
            for f in files:
                files_str += " " + f
            log.info("Retrieving input from task_id '{}'' and key '{}'".format(self.get_inputs_from, XCOM_RETURN_KEY))

        bash_command = 'rsync -avHPze "ssh -i ' + self.ssh_key_file + ' -o StrictHostKeyChecking=no" ' + files_str + ' ' + self.remote_usr + '@' + self.host + ':' + self.remote_dir
        bo = BashOperator(task_id='bash_operator_rsync_', bash_command=bash_command)
        bo.execute(context)

        # construct list of filenames uploaded to remote host
        files_list = files_str.split()
        filenames_list = list(os.path.join(self.remote_dir, os.path.basename(path)) for path in files_list)
        if filenames_list and len(filenames_list) > 0:
            log.info("Uploaded files: {}".format(pprint.pformat(files_list)))
            return filenames_list
        self._do_skip_downstream_tasks(context)


class MoveFilesOperator(BaseOperator, SkipMixin):
    """ MoveFilesOperator is moving files according to a filter to be applied on the file's names

    Args:
        src_dir (str): source directory to look for files inside
        dst_dir (str): destination directory to send files to
        filename_filter (str): expression to filter the files accordingly

    Returns:
        True
    """

    @apply_defaults
    def __init__(self, src_dir, dst_dir, filename_filter, prefix=None, regex=None, *args, **kwargs):
        super(MoveFilesOperator, self).__init__(*args, **kwargs)
        self.src_dir = src_dir
        self.dst_dir = dst_dir
        self.filename_filter = filename_filter
        self.prefix = prefix
        self.regex = regex
        log.info('--------------------MoveFilesOperator------------')

    def _do_skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    def execute(self, context):
        log.info(
            '\nsrc_dir={}\ndst_dir={}\nfilename_filter={}'.format(self.src_dir, self.dst_dir, self.filename_filter))
        filenames = fnmatch.filter(os.listdir(self.src_dir), self.filename_filter)

        if filenames is None or len(filenames) == 0:
            log.info("No files to move.")
            self._do_skip_downstream_tasks(context)

        if not os.path.exists(self.dst_dir):
            log.info("Destination folder does not exist.")
            self._do_skip_downstream_tasks(context)

        filenames_list = []
        for filename in filenames:
            filepath = os.path.join(self.src_dir, filename)
            _dst_dir = self.dst_dir
            _dst_filename = filename

            log.info("Matching {} && {} : {}".format(
                filename,
                self.regex,
                re.match(self.regex, filename)
            ))
            if self.regex and re.match(self.regex, filename):
                _dst_subfolder = re.match(self.regex, filename).groups()[0]
                _dst_dir = os.path.join(self.dst_dir, _dst_subfolder)
                if not os.path.exists(_dst_dir):
                    os.makedirs(_dst_dir)

            if self.prefix:
                if self.prefix in filename:
                    _dst_filename = _dst_filename[len(self.prefix):]
                else:
                    continue

            dst_filename = os.path.join(_dst_dir, os.path.basename(_dst_filename))
            log.info("Moving {} to {}".format(filepath, dst_filename))
            os.rename(filepath, dst_filename)

            filenames_list.append(dst_filename)

            _base_name, _file_extension = os.path.splitext(_dst_filename)
            try:
                for _ext in ('.md5', '.md5_processed', '{}.aux.xml'.format(_file_extension)):
                    _aux_file = os.path.join(self.src_dir, ''.join([_base_name, _ext]))
                    log.info("Checking {} ...".format(_aux_file))
                    if os.path.exists(_aux_file):
                        _dst_aux_filename = os.path.join(_dst_dir, os.path.basename(_aux_file))
                        log.info("Moving {} to {}".format(_aux_file, _dst_aux_filename))
                        os.rename(_aux_file, _dst_aux_filename)
            except Exception as e:
                log.exception(e)

            _org_file = os.path.join(self.src_dir, ''.join([_base_name, _file_extension]))
            log.info("Checking {} ...".format(_org_file))
            try:
                if os.path.exists(_org_file):
                    os.remove(_org_file)
            except Exception as e:
                log.exception(e)
                self._do_skip_downstream_tasks(context)

        if filenames_list and len(filenames_list) > 0:
            log.info(filenames_list)
            return filenames_list
        self._do_skip_downstream_tasks(context)


class SearchFilesOperator(BaseOperator, SkipMixin):
    """ MoveFilesOperator is moving files according to a filter to be applied on the file's names

    Args:
        src_dir (str): source directory to look for files inside
        dst_dir (str): destination directory to send files to
        filter (str): expression to filter the files accordingly

    Returns:
        True
    """

    @apply_defaults
    def __init__(self,
                 src_dir,
                 filename_filter,
                 *args, **kwargs):
        super(SearchFilesOperator, self).__init__(*args, **kwargs)
        self.src_dir = src_dir
        self.filename_filter = filename_filter
        log.info('--------------------SearchFilesOperator-----------')

    def _do_skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    def execute(self, context):
        log.info('\nsrc_dir={}\nfilter={}'.format(self.src_dir, self.filename_filter))
        filenames = fnmatch.filter(os.listdir(self.src_dir), self.filename_filter)

        if filenames is None or len(filenames) == 0:
            log.info("No files to move.")
            self._do_skip_downstream_tasks(context)

        def md5sum(filename):
            with open(filename, mode='rb') as f:
                d = hashlib.md5()
                for buf in iter(partial(f.read, 128), b''):
                    d.update(buf)
            return d.hexdigest()

        filenames_list = []
        for filename in filenames:
            log.info("Checking file {} ...".format(filename))
            filename = os.path.join(self.src_dir, filename)
            _md5 = md5sum(filename)
            _base_name, _file_extension = os.path.splitext(filename)
            _md5_file = ''.join([_base_name, ".md5"])
            if os.path.exists(_md5_file):
                try:
                    with open(_md5_file, 'rb', 0) as file, \
                            mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
                        if s.find(_md5.encode('utf8')) != -1:
                            log.info("Found file {} with md5 {}".format(filename, _md5))
                            filenames_list.append(filename)
                            dst_filename = ''.join([os.path.splitext(_md5_file)[0], ".md5_processed"])
                            os.rename(_md5_file, dst_filename)
                except Exception as e:
                    log.exception(e)
                    self._do_skip_downstream_tasks(context)

        if filenames_list and len(filenames_list) > 0:
            log.info(filenames_list)
            return filenames_list
        self._do_skip_downstream_tasks(context)


class FilesPlugin(AirflowPlugin):
    name = "FILES_plugin"
    operators = [RSYNCOperator, ZipInspector, MoveFilesOperator, SearchFilesOperator]
