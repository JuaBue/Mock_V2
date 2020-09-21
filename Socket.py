import socket
import errno

# Error codes
SZ_SOCKET_MAX_BUFFER = 4096
SOCKET_TIMEOUT = 1

# Error codes sockets.
ERROR_SOCKET_OK = 0
ERROR_SOCKET_TIMEOUT = 1
ERROR_SOCKET_DISCONNECTED = -2
ERROR_SOCKET_SEND = -3

# Exit codes
ERROR_SERIAL_CONFIGURATION = -1
ERROR_SOCKET_CONFIGURATION = -2


def get_local_host():
    localhost = socket.gethostbyname(socket.gethostname())
    return localhost


class SocketHandler:
    def __init__(self):
        # Use '0.0.0.0' as address to listen on all interfaces (Ethernet, Wifi, etc.)
        self.ip_address = '0.0.0.0'
        self.port = 4445
        self.timeout = SOCKET_TIMEOUT
        self.sock_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_obj.settimeout(self.timeout)

    def set_ip(self, ip_address):
        if self.ip_address != ip_address:
            # Restart socket if address has changed
            self.restart()
        self.ip_address = ip_address

    def get_ip(self):
        # Get ip address used by the socket.
        return socket.gethostbyname(socket.gethostname())

    def set_port(self, port=504):
        if self.port != port:
            # Restart socket if port has changed
            self.restart()
        self.port = port

    def set_timeout(self, timeout):
        if self.port != timeout:
            # Restart socket if timeout has changed
            self.restart()
        self.timeout = timeout

    def config_menu(self):
        configured = False
        # port_input = 0
        print('')
        print('SOCKET COMMUNICATION (Address: {})'.format(self.ip_address))
        try:
            port_input = int(input('* Enter server port: '))
        except ValueError:
            print('[ERROR] Invalid port value')
            print('[ERROR] Socket configuration failed')
            return configured

        self.port = port_input
        configured = True
        return configured

    def start(self, reconfigure=False):
        started = True
        if reconfigure:
            started = self.config_menu()
        if started:
            self.restart()
        return started

    def wait_for_connection(self):
        address = (self.ip_address, self.port)
        is_connected = False
        print('')
        print('[INFO] Waiting server connection {}'.format(address))

        while not is_connected:
            try:
                self.sock_obj.connect(address)
            except socket.error as msg:
                # Already connected
                if msg.errno == errno.EISCONN:
                    is_connected = True
                else:
                    continue
            is_connected = True
        print('[INFO] Connected to {}'.format(address))

    def close(self):
        self.sock_obj.close()

    def restart(self):
        self.close()
        self.sock_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_obj.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def receive(self):
        error = ERROR_SOCKET_OK
        frame_rcv = ""
        check_frame = True
        try:
            frame_rcv = self.sock_obj.recv(SZ_SOCKET_MAX_BUFFER)
        except socket.error as msg:
            if 'timed out' in msg:
                # Ignore timeout errors
                error = ERROR_SOCKET_TIMEOUT
                check_frame = False
                pass

        if not frame_rcv and check_frame:
            error = ERROR_SOCKET_DISCONNECTED
        return error, frame_rcv

    def send(self, frame_to_send):
        error = ERROR_SOCKET_SEND
        num_bytes = self.sock_obj.send(frame_to_send)
        if len(frame_to_send) < num_bytes:
            print('[WARNING] {} bytes of {} sent'.format(num_bytes, len(frame_to_send)))
        else:
            error = ERROR_SOCKET_OK
        return error

    def server_start(self):
        self.sock_obj.bind((self.ip_address, self.port))
        self.sock_obj.listen(10)

    def accept_socket(self):
        current_connection = {}
        address = {}
        try:
            current_connection, address = self.sock_obj.accept()
        #             process = multiprocessing.Process(self.sock_obj, (current_connection, address))
        #             process.daemon = True
        #             process.start()
        #             logging.info("Started process %r", process)
        except socket.error as msg:
            if 'timed out' in msg:
                # Ignore timeout errors
                pass
        return current_connection, address
