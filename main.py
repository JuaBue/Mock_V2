from Socket import *
from OperationsView import OperationsWin
import logging
import os
import sys
from datetime import date, datetime
from TelechargeDB import DataBase
from Transaction import Transaction
from TicketDB import TDataBase
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox, QPushButton, QTableWidgetItem
from PyQt5 import uic, QtCore
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtGui import QPixmap, QIcon

RUN_AUTO = True

LOGGING_LEVEL_FILE = logging.DEBUG
LOGGING_LEVEL_CONSOLE = logging.DEBUG
ICON_RED_LED = "MainWindow/icon/led-red-on.png"
ICON_GREEN_LED = "MainWindow/icon/green-led-on.png"


class MockV2(QThread):
    logger_handler = None
    socket_handler = None
    op_signal = pyqtSignal(object)

    def load_tables(self):
        self.databasetables.loadfile("conf/tablas_V2.txt")

    def load_ticket(self):
        self.databaseticket.loadfile("conf/ticket.txt")

    def __init__(self):
        super().__init__()
        self.init_logging()
        self.process = True
        self.environment = {}
        self.telechargetype = 0
        self.EcupImage = 0
        self.Ecouponing = False
        self.databaseticket = TDataBase(self.logger_handler)
        self.databasetables = DataBase(self.logger_handler)
        self.load_tables()
        self.load_ticket()

    def __del__(self):
        pass

    def managesocked(self, value):
        self.process = value

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
        transaction = Transaction(self.logger_handler, self.__getenviroment(), self.databasetables)
        self.socket_handler = SocketHandler()
        if not self.socket_handler.start(False):
            self.logger_handler.error("Socket configuration error")
            self.socket_handler.close()
            exit(ERROR_SOCKET_CONFIGURATION)
        self.socket_handler.server_start()
        self.logger_handler.info("Server listening...")
        while self.process:
            current_connection, address = self.socket_handler.accept_socket()
            if not current_connection:
                break
            self.logger_handler.info(" Waiting for frames...\n")
            exit_socket = False
            while not exit_socket:
                data = current_connection.recv(SZ_SOCKET_MAX_BUFFER)
                if data:
                    # Convert bytes object into string.
                    try:
                        rcv_request = data.decode('ascii')
                        self.logger_handler.info("[RX] {0}".format(rcv_request))
                        op_data, responsestring = transaction.process(rcv_request, self.__getenviroment())
                        respondedata = responsestring.encode('ascii')
                        print("[TX] {0}".format(responsestring))
                        current_connection.send(respondedata)
                        exit_socket = True
                        # Emit signal with operation data1
                        self.op_signal.emit(op_data)
                    except UnicodeDecodeError as e:
                        self.logger_handler.exception(e)
                        self.logger_handler.warning(" CADENA ENCRIPTADA!!!\n")
                        self.logger_handler.info("[RX] {0}".format(data))
                    except Exception as e:
                        self.logger_handler.exception(e)
                        self.logger_handler.warning(" ERROR!!!\n")
                        self.logger_handler.info("[RX] {0}".format(data))

                print("\n\n--------------------------------------------------------------------------------\n")
                self.logger_handler.info("Waiting for a new communication")

    def stop(self):
        self.logger_handler.info("Server stopped...")
        self.terminate()

    def __getenviroment(self):
        self.environment['TelechargeType'] = self.telechargetype
        self.environment['EcupImage'] = self.EcupImage
        self.environment['EcupStatus'] = self.Ecouponing
        return self.environment

    def settypetelecharge(self, type):
        self.telechargetype = type

    def setecuponingimage(self, type):
        self.EcupImage = 'I00' + str(type + 1)

    def enableecuponing(self):
        self.Ecouponing = True


class MainWin(QMainWindow):
    def __init__(self):
        # Iniciar el objeto QMainWindow
        QMainWindow.__init__(self)
        # Cargar la configuracion del archivo .ui en el objeto.
        uic.loadUi("MainWindow\\MainWindow.ui", self)
        self.setWindowTitle("Mock v2.0")
        self.setWindowIcon(QIcon('MainWindow\\icon\\icon_wordline.png'))
        # Fijar el tamaño de la ventanda
        # Fijar el tamaño minimo de la ventana.
        self.setMinimumSize(500, 305)
        # Fijar el tamaño maximo de la ventana.
        self.setMaximumSize(500, 305)
        self.mock = MockV2()
        # Operations window
        self.op_win = OperationsWin()
        self.boton.clicked.connect(self.abrirsocket)
        self.cerrar.clicked.connect(self.closeEvent)
        self.ver_op.clicked.connect(self.op_win.show)
        self.botoncargar.clicked.connect(self.mock.load_tables)
        self.botonvolcar.clicked.connect(self.volcartablas)
        # TelechargeType
        self.TelechargeCombo.addItem("0 - None")
        self.TelechargeCombo.addItem("1 - Data")
        self.TelechargeCombo.addItem("3 - SW")
        self.TelechargeCombo.addItem("5 - Image")
        self.TelechargeCombo.currentIndexChanged.connect(self.mock.settypetelecharge)
        # e-Couponing
        self.EcoupImg.addItem("I001")
        self.EcoupImg.addItem("I002")
        self.EcoupImg.addItem("I003")
        self.EcoupImg.addItem("I004")
        self.EcoupImg.addItem("I005")
        self.EcoupImg.addItem("I006")
        self.EcoupImg.addItem("I007")
        self.EcoupImg.addItem("I008")
        self.EcoupImg.addItem("I009")
        self.EcoupImg.currentIndexChanged.connect(self.mock.setecuponingimage)
        self.checkEcup.stateChanged.connect(self.Ecupestatus)
        # QR
        # Connect signal to update the table
        self.mock.op_signal.connect(self.op_win.update_table)
        # Table P
        self.horatc.setTime(QtCore.QTime.currentTime())
        self.fechatc.setDate(QtCore.QDate.currentDate())


        if RUN_AUTO:
            self.abrirsocket()

    def Ecupestatus(self, state):
        if state == QtCore.Qt.Checked:
            self.mock.enableecuponing()

    def volcartablas(self):
        print("dadsa")
        pass

    def abrirsocket(self):
        if not self.mock.isRunning():
            self.mock.start()
            self.qled.setPixmap(QPixmap(ICON_GREEN_LED))
            self.boton.setText('Stop MOCK')
            self.infoip.setText(self.getipasstring())
        else:
            self.mock.stop()
            self.qled.setPixmap(QPixmap(ICON_RED_LED))
            self.boton.setText('Run MOCK')
            self.infoip.setText('IP:- \r\r\r| PORT: -')

    def closeEvent(self, event):
        close = QMessageBox.question(self,
                                     "Cerrar Mock v2.0",
                                     "¿Estas seguro de cerrar la aplicación?",
                                     QMessageBox.Yes | QMessageBox.No)
        if close == QMessageBox.Yes:
            if self.mock.isRunning():
                self.mock.stop()
            sys.exit()

    def getipasstring(self):
        listofip = socket.gethostbyname_ex(socket.gethostname())[-1]
        iptext = ' | IP: '.join([str(elem) for elem in listofip])
        iptext = 'IP: ' + iptext + '\r\r\r| PORT: 4445'
        return iptext





if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        #app_icon = QIcon('MainWindow\\icon\\icon_wordline.png')
        #app.setWindowIcon(app_icon)
        MainWin = MainWin()
        MainWin.show()
        app.exec_()

    except KeyboardInterrupt:
        pass
