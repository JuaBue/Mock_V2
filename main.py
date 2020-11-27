import TelechargeDB
from Socket import *
import logging
import os
from datetime import date, datetime
from TelechargeDB import DataBase
from Transaction import Transaction
from TicketDB import TDataBase

LOGGING_LEVEL_FILE = logging.DEBUG
LOGGING_LEVEL_CONSOLE = logging.DEBUG


class MockV2:
    logger_handler = None
    socket_handler = None

    def load_tables(self):
        data = DataBase(self.logger_handler)
        data.loadfile("tablas_V2.txt")

    def load_ticket(self):
        data = TDataBase(self.logger_handler)
        data.loadfile("ticket.txt")

    def __init__(self):
        self.init_logging()
        self.transaction = Transaction(self.logger_handler)

    def __del__(self):
        if self.socket_handler:
            self.socket_handler.close()

    def init_logging(self):
        # Set logger file, folder and format
        folder_name = r'.\LOGS'
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        file_name = '.\\LOGS\\{0}{1}.LOG'.format(date.today().strftime("%Y%m%d"), datetime.now().strftime("%H%M"))
        log_formatter = '%(asctime)s\t%(funcName)s\t%(lineno)d\t[%(levelname)s]\t%(message)s'

        # Create the logger
        self.logger_handler = logging.getLogger('Mock V2 Logger')
        self.logger_handler.setLevel(LOGGING_LEVEL_FILE)
        # Create file handler
        file_handler = logging.FileHandler(file_name)
        file_handler.setLevel(LOGGING_LEVEL_FILE)
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOGGING_LEVEL_CONSOLE)
        # Add the format to the handlers
        formatter_handler = logging.Formatter(log_formatter, datefmt='%Y-%m-%d\t%I:%M:%S')
        file_handler.setFormatter(formatter_handler)
        console_handler.setFormatter(formatter_handler)
        # Add the handler to the logger
        self.logger_handler.addHandler(file_handler)
        self.logger_handler.addHandler(console_handler)

        self.logger_handler.info("Logging into: \t{0}".format(os.path.abspath(file_name)))
        return

    def run(self):
        # Socket configuration
        self.socket_handler = SocketHandler()
        if not self.socket_handler.start(False):
            self.logger_handler.error("Socket configuration error")
            self.socket_handler.close()
            exit(ERROR_SOCKET_CONFIGURATION)
        self.socket_handler.server_start()
        print(self.socket_handler.get_ip())
        self.logger_handler.info("Server listening...")
        while True:
            current_connection, address = self.socket_handler.accept_socket()
            self.logger_handler.info(" Waiting for frames...\n")
            exit_socket = False
            while not exit_socket:
                data = current_connection.recv(SZ_SOCKET_MAX_BUFFER)
                if data:
                    # Convert bytes object into string.
                    try:
                        rcv_request = data.decode('ascii')
                        self.logger_handler.info("[RX] {0}".format(rcv_request))
                        responsestring = self.transaction.process(rcv_request)
                        respondedata = responsestring.encode('ascii')
                        print("[TX] {0}".format(responsestring))
                        current_connection.send(respondedata)
                        exit_socket = 1
                    except Exception as e:
                        self.logger_handler.exception(e)
                        self.logger_handler.warning(" The frame is ENCRYPTED!!!\n")
                        self.logger_handler.info("[RX] {0}".format(data))

                print("\n\n--------------------------------------------------------------------------------\n")
                self.logger_handler.info("Waiting for a new communication")
        return


if __name__ == "__main__":
    try:
        mock = MockV2()
        # mock.load_tables()
        mock.load_ticket()
        mock.run()

    except KeyboardInterrupt:
        pass
