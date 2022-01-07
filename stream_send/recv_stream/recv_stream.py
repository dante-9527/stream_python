import os
import sys
import socketserver
from log_utils import dg_set_logger
from airflow.configuration import AIRFLOW_HOME

DG_WORK_PATH = AIRFLOW_HOME
# 定义日志输出路径
DG_LOG_PATH = os.path.abspath(os.path.join(DG_WORK_PATH, "dg_backend_logs"))

DG_DBREC_LOG_FOLDER = '/dens/dbrec/logs/'


logger = dg_set_logger(DG_LOG_PATH, "Gen_dags_template")
ip_port = ('0.0.0.0', 9999)


class RecvServer(socketserver.BaseRequestHandler):
    def handle(self):
        logger.info(f'{self.client_address} connect in')  # addr
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
                    logger.info(f'Read or write data error, err={err}')
                    raise Exception(f'Read or write data error, err={err}')

    def finish(self):
        """完成数据流接收之后，关闭server"""
        self.server.shutdown()


if __name__ == "__main__":
    logger.info(f'Stream recv start, ip-port is {ip_port}')
    recv_server = socketserver.ThreadingTCPServer(ip_port, RecvServer)
    recv_server.serve_forever()

