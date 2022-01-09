import os
import sys
import click
import socket
from log_utils import dg_set_logger
from airflow.configuration import AIRFLOW_HOME

DG_WORK_PATH = AIRFLOW_HOME
# 定义日志输出路径
DG_LOG_PATH = os.path.abspath(os.path.join(DG_WORK_PATH, "dg_backend_logs"))

DG_DBREC_LOG_FOLDER = '/dens/dbrec/logs/'

logger = dg_set_logger(DG_LOG_PATH, "Gen_dags_template")

HOST = None
PORT = None


@click.command()
@click.option('--host', help='host to connect', type=str)
@click.option('--port', help='port to connect', type=int)
def get_cmd_option(host, port):
    global HOST, PORT
    HOST = host
    PORT = port


class StreamSender(object):

    def __init__(self, host, port):
        self.client = socket.socket()
        self.client.connect((host, port))

    def run(self):
        send_size = 0
        with sys.stdin.buffer as f1:
            try:
                while True:
                    chunk = f1.read(1024)
                    if chunk == b'':
                        break
                    self.client.send(chunk)
                    send_size += len(chunk)
            except Exception as err:
                logger.info(f'Send backup failed, err={err}')
        logger.info(f'Send total size is {send_size}')
        self.check_stream_size(send_size)
        self.client.close()

    def check_stream_size(self, send_size):
        """检查stream流是否完整发送"""
        # 接收worker端传回来的数据量大小
        recv_size = self.client.recv(4)
        # todo 将byte转换成数字
        if recv_size == send_size:
            logger.info(f'The amount of data received at the sysnode is equal to source side, is {recv_size}')
            self.client.send(b'1')
        else:
            logger.info(f'The amount of data received at the sysnode is not equal to source side, is {recv_size}')
            self.client.send(b'0')
            raise Exception(f'The amount of data received at the sysnode is not equal to source side, '
                            f'send size is {send_size},recv size is {recv_size}, ')


if __name__ == '__main__':
    # todo 接收命令行给到的参数(ip, port)
    """xtrabackup --defaults-file=/etc/my.cnf --host=192.168.100.93  --safe-slave-backup --slave-info --user=root 
    --port=3306 --password=YiMu@20201128 --parallel=6 --backup --stream=xbstream | qpress -ioT4 src dest | 
    python3 send_stream.py --host=192.168.0.0 --port=9999"""
    client = StreamSender(HOST, PORT)
    client.run()
