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
@click.option('--dbs-dir', help='The dbs dir',type=str)
@click.option('--host', default='0.0.0.0', help='ip bind, default is 0.0.0.0', type=str)
@click.option('--port', default=9999, help='port bind, default is 9999',
              type=click.IntRange(9000, 65535, clamp=True), callback=validate_port)
def get_cmd_option(dbs_dir, host, port):
    global IPADDR, DBS_DIR
    IPADDR = (host, port)
    DBS_DIR = dbs_dir


class RecvServer(socketserver.BaseRequestHandler):
    def handle(self):
        logger.info(f'{self.client_address} connect in')
        recv_size = 0
        with sys.stdout.buffer as f1:
            while True:
                try:
                    data = self.request.recv(1024)
                    if not data: break
                    recv_size += len(data)
                    f1.write(data)
                except ConnectionResetError as err:
                    logger.info(f'Conn error, err={err}')
                    raise ConnectionResetError(f'Conn error, err={err}')
                except Exception as err:
                    logger.info(f'Recv or write data error, err={err}')
                    # 清空备份片文件夹
                    try:
                        logger.info(f'Start to delete {DBS_DIR}')
                        subprocess.check_call(f'rm -rf {DBS_DIR}/*', shell=True)
                        logger.info(f'Delete {DBS_DIR} succeed')
                    except Exception as e:
                        logger.info(f'Delete {DBS_DIR} failed, err={e}')
                    raise Exception(f'Recv or write data error, err={err}')
        logger.info(f'Recv backup data total size is {recv_size}')
        # 将收到的数据量传递回源端
        # todo 使用stract模块将数字转化成4byte
        self.request.send(recv_size)

        # 接收源端对比数据量之后返回的retcode
        retcode = self.request.recv(1)
        if retcode == b'1':
            logger.info(f'Recv backup from {self.client_address} succeed')
        else:
            logger.info(f'Recv backup from {self.client_address} failed')
            raise Exception(f'Recv backup from {self.client_address} failed')

    def finish(self):
        """关闭server"""
        self.server.shutdown()


if __name__ == "__main__":
    """python3 recv_stream --host=192.168.0.0 --port=9999 --dbs-dir={dbs_dir} | qpress -idTo4 > /dev/stdout | 
    xbtream -x -C {dbs_dir}"""
    ret = get_cmd_option()
    logger.info(f'Stream recv start, ip-port is {IPADDR}')
    recv_server = socketserver.ThreadingTCPServer(IPADDR, RecvServer)
    recv_server.serve_forever()

