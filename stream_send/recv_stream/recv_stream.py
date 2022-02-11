import os
import time
import json
import click
import struct
import socketserver
from log_utils import dg_set_logger
from airflow.configuration import AIRFLOW_HOME


DG_WORK_PATH = AIRFLOW_HOME
# 定义日志输出路径
DG_LOG_PATH = os.path.abspath(os.path.join(DG_WORK_PATH, "dg_backend_logs"))

DG_DBREC_LOG_FOLDER = '/dens/dbrec/logs/'

logger = dg_set_logger(DG_LOG_PATH, "mysql_bkp_recv")
IPADDR = ('0.0.0.0', 9999)
QP_PATHS = {}

conns = []


class RecvServer(socketserver.BaseRequestHandler):
    def handle(self):
        logger.info(f'{self.client_address} connect in, start to recv backup stream')
        # todo 如果conns有连接，则等待
        while 1:
            if conns:
                self.request.sendall(b'0')
                time.sleep(2)
            else:
                self.request.sendall(b'1')
                break
        conns.append(self.request)
        recv_size = 0
        client_ip = self.client_address[0]
        qp_path = QP_PATHS.pop(client_ip)
        with open(qp_path, mode='wb') as f1:
            while True:
                try:
                    data = self.recvall()
                    if not data: break
                    recv_size += len(data)
                    f1.write(data)
                except ConnectionResetError as err:
                    logger.info(f'Conn error, err={err}')
                    raise ConnectionResetError(f'Conn error, err={err}')
                except Exception as err:
                    logger.info(f'Recv or write data error, err={err}')
                    raise Exception(f'Recv or write data error, err={err}')
        logger.info(f'Recv backup data total size is {recv_size}')
        # 将接收到的数据大小传输回去进行对比
        recv_size_byte = json.dumps(recv_size).encode('utf-8')
        recv_size_len = len(recv_size_byte)
        header = struct.pack("i", recv_size_len)
        self.request.sendall(header)
        self.request.sendall(recv_size_byte)

        # 接收源端对比数据量之后返回的retcode
        retcode = self.request.recv(1)
        if retcode == b'1':
            logger.info(f'Recv backup from {self.client_address} succeed')
            conns.remove(self.request)
        else:
            logger.info(f'Recv backup from {self.client_address} failed')
            raise Exception(f'Recv backup from {self.client_address} failed')

    def finish(self):
        """关闭server"""
        logger.info('There is no recv task run, server shutdown')
        self.server.shutdown()

    def recvall(self):
        """接收全部数据"""
        # 获取数据头
        has_read_size = 0
        header_list = []
        while has_read_size < 4:
            chunk = self.request.recv(4 - has_read_size)
            has_read_size += len(chunk)
            header_list.append(chunk)
        header = b"".join(header_list)
        data_length = struct.unpack("i", header)[0]
        if data_length == 0:
            return None

        # 获取数据
        data_list = []
        has_read_data_size = 0
        while has_read_data_size < data_length:
            size = 2048 if (data_length - has_read_data_size) > 2048 else (
                    data_length - has_read_data_size)
            data_chunk = self.request.recv(size)
            has_read_data_size += len(data_chunk)
            data_list.append(data_chunk)
        data = b"".join(data_list)
        return data


@click.command()
@click.option('--qp-path', help='The dbs dir', type=str)
@click.option('--host', default='0.0.0.0', help='ip bind, default is 0.0.0.0', type=str)
@click.option('--port', default=9999, help='port bind, default is 9999', type=click.IntRange(9000, 65535, clamp=True))
def run(qp_path, host, port):
    global IPADDR, QP_PATHS
    IPADDR = (host, port)
    client_ip = qp_path.split('/')[3]
    QP_PATHS[client_ip] = qp_path
    # try:
    #     # 查询端口号使用情况，如果已经开启则什么都不做往下执行，没有则开启服务端
    #     port_use = subprocess.check_output(f'netstat -ntulp | grep {port}', shell=True)
    #     if port_use:
    #         logger.info(f'Recv server {IPADDR} has already run')
    # except subprocess.CalledProcessError:
    #     logger.info(f'Recv server {IPADDR} don\'t start')
    logger.info(f'Stream recv start, ip-port is {IPADDR}')
    recv_server = socketserver.ThreadingTCPServer(IPADDR, RecvServer)
    recv_server.serve_forever()


if __name__ == "__main__":
    """python3 recv_stream --host=192.168.0.0 --port=9999 --qp-path={dbs_dir} """
    run()
