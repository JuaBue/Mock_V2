from Socket import *
from OperationsView import OperationsWin
import logging
import os
import sys
from datetime import date, datetime
from TelechargeDB import DataBase
from Transaction import Transaction
from Transaction import TablePException
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

# Listas desplegables.
listaTelecarga = ['0 - None', '1 - Data', '3 - SW', '5 - Image']
listaImg = ['I001', 'I002', 'I003', 'I004', 'I005', 'I006', 'I007', 'I008', 'I009']
listaGroup = ['t ', 't1', 't2', 'Q ', 'Q1', 'Q2', 'Q3', 'Q4', 'P', 'h', 'z', 'l1', 'l2', 'l']
listaCierre = ['0', '1', '2', 'A', '']


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
        self.EcupImage = 'I001'
        self.Ecouponing = False
        self.Qrtext = ''
        self.Qrstatus = False
        self.databaseticket = TDataBase(self.logger_handler)
        self.databasetables = DataBase(self.logger_handler)
        self.load_tables()
        self.load_ticket()
        self.swversion = ''
        self.ftpuser = ''
        self.passftp = ''
        self.horatc = datetime.now().strftime("%H%M")
        self.fechatc = date.today().strftime("%d%m%y")
        self.background = '0'
        self.postprocess = '0'
        self.opemode = 'N'
        self.tablep = False
        self.movementdata = False
        self.moveproductinfo = ''
        self.movenumprod = '00'

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
        transaction = Transaction(self.logger_handler, self.databasetables)
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
                    except TablePException as e:
                        self.logger_handler.exception(e)
                        self.logger_handler.warning(" FALTA DE DATOS EN TABLA P!!!\n")
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
        self.environment['Qr'] = self.Qrtext
        self.environment['QRStatus'] = self.Qrstatus
        self.environment['OperationMode'] = self.opemode
        self.environment['TablePinfo'] = {'swversion': self.swversion, 'ftpuser': self.ftpuser,
                                          'passftp': self.passftp, 'horatc': self.horatc, 'fechatc': self.fechatc,
                                          'background': self.background, 'postprocess': self.postprocess}
        self.environment['MovementeInfo'] = {'movementdata': self.movementdata, 'movegroup': self.movegroup,
                                             'movecierre': self.movecierre, 'movedescrip': self.movedescrip,
                                             'movedescount': self.movedescount, 'moveproductinfo': self.moveproductinfo,
                                             'movenumprod': self.movenumprod}
        return self.environment

    def settypetelecharge(self, type):
        if type == 0 or type == 1:
            self.telechargetype = type
        elif type == 2:
            self.telechargetype = type + 1
        elif type == 3:
            self.telechargetype = type + 2
        else:
            self.telechargetype = 0

    def setecuponingimage(self, type):
        self.EcupImage = 'I00' + str(type + 1)

    def enableecuponing(self, value):
        self.Ecouponing = value

    def setqrtext(self, value):
        self.Qrtext = value

    def enableqr(self, value):
        self.Qrstatus = value

    def modifytablep(self, value):
        fields = {'versionsw': 1, 'userftp': 2, 'passftp': 3, 'horatc': 4,
                  'fechatc': 5, 'background': 6, 'postprocess': 7}
        process = 0
        for val, key in fields.items():
            if val == self.sender().objectName():
                process = key
                break
        if process == 1:
            self.swversion = value
        elif process == 2:
            self.ftpuser = value
        elif process == 3:
            self.passftp = value
        elif process == 4:
            self.horatc = value.toString("HHmm")
        elif process == 5:
            self.fechatc = value.toString("ddMMyy")
        elif process == 6:
            if value == 2:
                self.background = '1'
            else:
                self.background = '0'
        elif process == 7:
            if value == 2:
                self.postprocess = '1'
            else:
                self.postprocess = '0'
        else:
            self.logger_handler.error("The sender to callback  is unreachable.")

    def enabletablep(self, value):
        self.tablep = value

    def operationmode(self, value):
        fields = {'radioN': 1, 'radioE': 2, 'radioA': 3}
        process = 0
        for val, key in fields.items():
            if val == self.sender().objectName():
                process = key
                break
        if process == 1 and value:
            self.opemode = 'N'
        elif process == 2 and value:
            self.opemode = 'E'
        elif process == 3 and value:
            self.opemode = 'A'
        else:
            pass

    def mofifymovementdata(self, value):
        fields = {'comboBox': 1, 'comboBox2': 2, 'lineEdit': 3, 'discount': 4,
                  'cesta': 5, 'products': 6}
        process = 0
        for val, key in fields.items():
            if val == self.sender().objectName():
                process = key
                break
        if process == 1 and value:
            self.movegroup = listaGroup[value]
        elif process == 2 and value:
            self.movecierre = listaCierre[value]
        elif process == 3:
            self.movedescrip = value
        elif process == 4 and value:
            self.movedescount = value.replace(',', '')
        elif process == 5 and value:
            self.moveproductinfo = value
        elif process == 6 and value:
            self.movenumprod = "{:02d}".format(value)
        else:
            pass

    def enablemovementdata(self, value):
        self.movementdata = value

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
        self.setMinimumSize(600, 400)
        # Fijar el tamaño maximo de la ventana.
        self.setMaximumSize(600, 400)
        self.mock = MockV2()
        # Operations window
        self.op_win = OperationsWin()
        self.boton.clicked.connect(self.abrirsocket)
        self.cerrar.clicked.connect(self.closeEvent)
        self.ver_op.clicked.connect(self.op_win.show)
        self.botoncargar.clicked.connect(self.mock.load_tables)
        self.botonvolcar.clicked.connect(self.volcartablas)
        # TelechargeType
        for value in listaTelecarga:
            self.TelechargeCombo.addItem(value)
        self.TelechargeCombo.currentIndexChanged.connect(self.mock.settypetelecharge)
        # e-Couponing
        for value in listaImg:
            self.EcoupImg.addItem(value)
        self.EcoupImg.currentIndexChanged.connect(self.mock.setecuponingimage)
        self.checkEcup.stateChanged.connect(self.Ecupestatus)
        self.checkQR.stateChanged.connect(self.Qrstatus)
        self.textQR.textChanged.connect(self.mock.setqrtext)
        # Operation
        self.radioN.toggled.connect(self.mock.operationmode)
        self.radioE.toggled.connect(self.mock.operationmode)
        self.radioA.toggled.connect(self.mock.operationmode)
        self.radioN.setChecked(True)
        # Movement
        for value in listaGroup:
            self.comboBox.addItem(value)
        self.comboBox.currentIndexChanged.connect(self.mock.mofifymovementdata)
        self.comboBox.setCurrentIndex(listaGroup.index('Q1'))
        for value in listaCierre:
            self.comboBox2.addItem(value)
        self.comboBox2.currentIndexChanged.connect(self.mock.mofifymovementdata)
        self.comboBox2.setCurrentIndex(listaCierre.index('A'))
        self.lineEdit.textChanged.connect(self.mock.mofifymovementdata)
        self.discount.textChanged.connect(self.mock.mofifymovementdata)
        self.cesta.textChanged.connect(self.mock.mofifymovementdata)
        self.movement.stateChanged.connect(self.mock.enablemovementdata)
        self.products.valueChanged.connect(self.mock.mofifymovementdata)
        self.lineEdit.setText('OPERACIONES MOCK        ')
        self.discount.setText('1,00')
        self.cesta.setText('')
        self.products.setValue(0)
        # QR
        # Connect signal to update the table
        self.mock.op_signal.connect(self.op_win.update_table)
        # Table P
        self.activarp.stateChanged.connect(self.mock.enabletablep)
        self.horatc.setTime(QtCore.QTime.currentTime())
        self.fechatc.setDate(QtCore.QDate.currentDate())
        self.horatc.timeChanged.connect(self.mock.modifytablep)
        self.fechatc.dateChanged.connect(self.mock.modifytablep)
        self.versionsw.textChanged.connect(self.mock.modifytablep)
        self.userftp.textChanged.connect(self.mock.modifytablep)
        self.passftp.textChanged.connect(self.mock.modifytablep)
        self.background.stateChanged.connect(self.mock.modifytablep)
        self.postprocess.stateChanged.connect(self.mock.modifytablep)
        # Log Text viewer
        self.logtext.setReadOnly(True)
        self.logtext.appendPlainText("TODO: Implementar señal para logear info.")

        if RUN_AUTO:
            self.abrirsocket()

    def Ecupestatus(self, state):
        if state == QtCore.Qt.Checked:
            self.mock.enableecuponing(True)
        else:
            self.mock.enableecuponing(False)

    def Qrstatus(self, state):
        if state == QtCore.Qt.Checked:
            self.mock.enableqr(True)
        else:
            self.mock.enableqr(False)

    def volcartablas(self):
        self.logtext.appendPlainText("Volcado de tablas...")
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
