import json
import sys
import click
import socket
import struct

HOST = None
PORT = None
CHUNK_SIZE = 4096


@click.command()
@click.option('--host', help='host to connect', type=str)
@click.option('--port', help='port to connect', type=int)
def run(host, port):
    global HOST, PORT
    HOST = host
    PORT = port
    client = StreamSender(HOST, PORT)
    client.run()


class StreamSender(object):

    def __init__(self, host, port):
        self.client = socket.socket()
        self.client.connect((host, port))

    def run(self):
        # 判断是否可以传输数据
        while 1:
            ok = self.client.recv(1)
            if ok == b'1':
                break
        send_size = 0
        try:
            with sys.stdin.buffer as f1:
                while True:
                    chunk = f1.read(CHUNK_SIZE)
                    if chunk == b'':
                        # 传递结束传输flag
                        flag = 0
                        flag_header = struct.pack("i", flag)
                        self.client.sendall(flag_header)
                        break
                    # 制作数据头
                    header = struct.pack("i", len(chunk))
                    self.client.sendall(header)
                    self.client.sendall(chunk)
                    send_size += len(chunk)
            self.check_stream_size(send_size)
        except Exception as err:
            raise Exception(f'Send backup failed, err={err}')
        finally:
            self.client.close()

    def check_stream_size(self, send_size):
        """检查stream流是否完整发送"""
        # 获取数据头
        has_read_size = 0
        header_list = []
        while has_read_size < 4:
            chunk = self.client.recv(4 - has_read_size)
            has_read_size += len(chunk)
            header_list.append(chunk)
        header = b"".join(header_list)
        data_length = struct.unpack("i", header)[0]
        # 获取数据
        data_list = []
        has_read_data_size = 0
        while has_read_data_size < data_length:
            size = 2048 if (data_length - has_read_data_size) > 2048 else (
                    data_length - has_read_data_size)
            data_chunk = self.client.recv(size)
            has_read_data_size += len(data_chunk)
            data_list.append(data_chunk)
        recv_size = b"".join(data_list)
        recv_size = json.loads(recv_size.decode('utf-8'))
        if recv_size == send_size:
            # logger.info(f'The amount of data received at the sysnode is equal to source side, is {recv_size}')
            self.client.send(b'1')
        else:
            # logger.info(f'The amount of data received at the sysnode is not equal to source side, is {recv_size}')
            self.client.send(b'0')
            raise Exception(f'The amount of data received at the sysnode is not equal to source side, '
                            f'send size is {send_size},recv size is {recv_size}, ')


if __name__ == '__main__':
    run()
