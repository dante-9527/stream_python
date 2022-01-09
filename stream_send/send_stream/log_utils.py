import os
import logging
import subprocess
from airflow.configuration import AIRFLOW_HOME

DG_WORK_PATH = AIRFLOW_HOME
# 定义日志输出路径
DG_LOG_PATH = os.path.abspath(os.path.join(DG_WORK_PATH, "dg_backend_logs"))

DG_DBREC_LOG_FOLDER = '/dens/dbrec/logs/'


def dg_set_logger(log_path, log_prefix):
    """
    定义日志显示格式
    """
    if not os.path.exists(log_path):
        subprocess.check_call("mkdir -p %s" % log_path, shell=True)
    logger = logging.getLogger(__name__)
    log_path = log_path
    log_name = log_path + "/" + log_prefix + ".log"
    log_fh = logging.FileHandler(log_name, mode="w")
    log_fh.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter(
        "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
    )
    log_fh.setFormatter(log_formatter)
    logger.addHandler(log_fh)
    logger.info("Initial logger Done!")
    return logger



