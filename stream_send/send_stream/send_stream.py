import sys
import socket
import logging


class StreamSender(object):

    def __init__(self, host, port):
        self.client = socket.socket()
        self.client.connect((host, port))

    def run(self):
        size = 0
        with sys.stdin.buffer as f1:
            try:
                while True:
                    chunk = f1.read(1024)
                    if chunk == b'':
                        break
                    self.client.send(chunk)
                    size += len(chunk)
            except Exception as err:
                print(err)
        self.client.close()

    def check_stream_size(self):
        """检查stream流是否完整发送"""
        ...


if __name__ == '__main__':
    # todo 接收命令行给到的参数(ip, port)
    """xtrabackup --defaults-file=/etc/my.cnf --host=192.168.100.93  --safe-slave-backup --slave-info --user=root 
    --port=3306 --password=YiMu@20201128 --parallel=6 --backup --stream=xbstream | qpress -ioT4 src dest | 
    python3 send_stream.py --ip=192.168.0.0 --port=9999"""
    client = StreamSender("127.0.0.1", 9999)
    client.run()
