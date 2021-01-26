from PyQt5.QtWidgets import QDialog, QHeaderView, QTableWidgetItem, QPushButton, QTextEdit
from PyQt5 import uic, Qt


class OperationsWin(QDialog):
    def __init__(self):
        super(OperationsWin, self).__init__()
        uic.loadUi("MainWindow\\Operaciones.ui", self)
        # Operations table
        # Column count
        self.op_table.setColumnCount(8)
        # Insert header names
        self.op_table.setHorizontalHeaderLabels(['Date', 'Time', 'Result', 'Type', 'Number', 'Entry Mode',
                                                 'Amount', 'Ticket'])
        self.op_table.horizontalHeader().setStretchLastSection(True)
        self.op_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        # Create dialog to show tickets
        self.ticket_dialog = QDialog()
        self.ticket_dialog.setMinimumHeight(800)
        self.ticket_dialog.setMinimumWidth(600)
        self.ticket_dialog.setLayout(Qt.QVBoxLayout())
        self.ticket_text = QTextEdit()
        self.ticket_text.setReadOnly(True)
        self.ticket_dialog.layout().addWidget(self.ticket_text)

        self.op_table.cellDoubleClicked.connect(self.show_ticket)

    def update_table(self, op_data):
        row_count = self.op_table.rowCount()  # necessary even when there are no rows in the table
        self.op_table.insertRow(row_count)
        self.op_table.setItem(row_count, 0, QTableWidgetItem(op_data['Date']))
        self.op_table.setItem(row_count, 1, QTableWidgetItem(op_data['Time']))
        self.op_table.setItem(row_count, 2, QTableWidgetItem(op_data['Result']))
        self.op_table.setItem(row_count, 3, QTableWidgetItem(op_data['Op_Type']))
        self.op_table.setItem(row_count, 4, QTableWidgetItem(op_data['Num_Op']))
        self.op_table.setItem(row_count, 5, QTableWidgetItem(op_data['Entry_Mode']))
        self.op_table.setItem(row_count, 6, QTableWidgetItem(str(op_data['Importe'])))
        self.op_table.setItem(row_count, 7, QTableWidgetItem(op_data['Ticket']))

    def show_ticket(self, row, column):
        if 7 == column:
            item = self.op_table.item(row, column)
            self.ticket_text.clear()
            self.ticket_text.insertPlainText(item.text())
            self.ticket_dialog.setWindowTitle('Operacion {}'.format(row + 1))
            self.ticket_dialog.show()







