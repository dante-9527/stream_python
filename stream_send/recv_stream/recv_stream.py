import os
import sys
import click
import socketserver
import subprocess
from log_utils import dg_set_logger
from airflow.configuration import AIRFLOW_HOME

DG_WORK_PATH = AIRFLOW_HOME
# 定义日志输出路径
DG_LOG_PATH = os.path.abspath(os.path.join(DG_WORK_PATH, "dg_backend_logs"))

DG_DBREC_LOG_FOLDER = '/dens/dbrec/logs/'

logger = dg_set_logger(DG_LOG_PATH, "Gen_dags_template")
IPADDR = ('0.0.0.0', 9999)
DBS_DIR = None


def validate_port(ctx, param, value):
    """校验端口号是否被使用"""
    port_use = subprocess.check_output(f'netstat -ntulp | grep {value}')
    if port_use:
        raise click.BadParameter(f'port:{value} has been used')
    return value


@click.command()
@click.option('--dbs_dir', help='The dbs dir')
@click.option('--host', default='0.0.0.0', help='ip bind, default is 0.0.0.0')
@click.option('--port', default=9999, help='port bind, default is 9999',
              type=click.IntRange(9000, 65535, clamp=True), callback=validate_port)
def get_cmd_option(dbs_dir, host, port):
    global IPADDR, DBS_DIR
    IPADDR = (host, port)
    DBS_DIR = dbs_dir


class RecvServer(socketserver.BaseRequestHandler):
    def handle(self):
        logger.info(f'{self.client_address} connect in')
        with sys.stdout.buffer as f1:
            while True:
                try:
                    data = self.request.recv(1024)
                    if not data: break
                    f1.write(data)
                except ConnectionResetError as err:
                    logger.info(f'Conn error, err={err}')
                    raise ConnectionResetError(f'Conn error, err={err}')
                except Exception as err:
                    # 清空备份片文件夹
                    subprocess.check_call(f'rm -rf {DBS_DIR}/*', shell=True)
                    logger.info(f'Read or write data error, err={err}')
                    raise Exception(f'Read or write data error, err={err}')

    def finish(self):
        """完成数据流接收之后，关闭server"""
        self.server.shutdown()


if __name__ == "__main__":
    """python3 recv_stream --host=192.168.0.0 --port=9999 --dbs-dir={dbs_dir} | qpress -idTo4 > /dev/stdout | 
    xbtream -x -C {dbs_dir}"""
    ret = get_cmd_option()
    logger.info(f'Stream recv start, ip-port is {IPADDR}')
    recv_server = socketserver.ThreadingTCPServer(IPADDR, RecvServer)
    recv_server.serve_forever()

