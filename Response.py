from datetime import *
from random import randint
from TicketDB import TDataBase

RESPONSE = 'PH24{0}PDI{1}{2}3960000L{3}#20602#{4}#{5}#{6}#{7}##{8}##0000##{9}{10}{11}'
BASE_SIZE = 68


class Response:
    def __init__(self, logging_handler, environment):
        self.logging = logging_handler
        self.ticket = TDataBase(logging_handler, False)
        self.TrameError = False
        self.Amount = 0
        self.EntryMode = ''
        self.OpCode = ''
        self.lastNSM = ''
        self.OpNum = ''
        self.ProtocolVersion = ''
        self.chipdata = ''
        self.OpString = ''
        self.ticketstring = ''
        self.Result = ''
        self.ecouponing = ''
        self.Merchant = ''
        self.authonum = ''.join(["{}".format(randint(0, 9)) for num in range(0, 6)])
        self.upload = self.__getuploadtask()
        #environment variables
        self.telechargetype = '0'
        self.environment = environment


    def build_response(self, data_response):
        if not self.__import_data(data_response):
            return
        self.ticketstring, long = self.__buildticket()
        self.__getresponsecode()
        op_data = self.__log_operation()
        if 'EcupStatus' in self.environment.keys() and self.environment['EcupStatus'] and \
                'EcupImage' in self.environment.keys() and self.environment['EcupImage']:
            self.ecouponing = '{0}####------------------------'.format(self.environment['EcupImage'])
        long = long + len(self.chipdata) + len(self.ecouponing)
        return op_data, RESPONSE.format("{0:0=5d}".format(BASE_SIZE + long),
                               self.ProtocolVersion,
                               self.Merchant,
                               self.Result,
                               date.today().strftime("%d%m%Y") + datetime.now().strftime("%H%M%S"),
                               self.lastNSM,
                               self.OpNum,
                               self.authonum,
                               self.upload,
                               self.chipdata,
                               self.ticketstring,
                               self.ecouponing)

    def __import_data(self, data_response):
        if 'Error' in data_response:
            self.TrameError = data_response['Error']
        if 'Amount' in data_response:
            self.Amount = data_response['Amount']
        if 'EntryMode' in data_response:
            self.EntryMode = data_response['EntryMode']
        if 'OpCode' in data_response:
            self.OpCode = data_response['OpCode']
        if 'lastNSM' in data_response:
            self.lastNSM = data_response['lastNSM']
        if 'OpNum' in data_response:
            self.OpNum = data_response['OpNum']
        if 'ProtocolVersion' in data_response:
            self.ProtocolVersion = data_response['ProtocolVersion']
        if 'MerchantID' in data_response:
            self.Merchant = data_response['MerchantID']
        return True

    def __buildticket(self):
        template = ''
        ticket_raw = ''
        # Operation result
        if not self.TrameError:
            template += 'A'
        else:
            template += 'D'

        # Transaction type
        if 'V ' == self.OpCode and not self.TrameError:
            template += 'S'
            self.OpString = 'VENTA'
        elif 'AV' == self.OpCode and not self.TrameError:
            template += 'C'
            self.OpString = 'DEVOLUCION'
        elif 'PAP' == self.OpCode and not self.TrameError:
            template += 'P'
            self.OpString = 'PREAUTORIZACION'
        elif 'PAC' == self.OpCode and not self.TrameError:
            template += 'K'
            self.OpString = 'VENTA CONFIRMADA'
        elif self.TrameError:
            template += 'E'
        elif self.TrameError:
            template += 'G'
        else:
            template += 'R'

        # Entry Mode
        template += self.__getentrymode()

        # Verification method
        template += self.__getverificationmethod()

        # Ticket copy type
        typecopy = ['M', 'C']
        for currentcopy in typecopy:
            if 'M' == currentcopy:
                ticket_raw += 'S#'
            elif 'C' == currentcopy:
                ticket_raw += "SBCUTMC#"
            else:
                pass
            ticket_template = self.ticket.obtain_template(template + currentcopy)
            if not ticket_template:
                ticket_template = self.ticket.obtain_template('DR**M')
            for block in ticket_template['Bloques'].split('/'):
                current_block = self.ticket.obtain_bloque(block)
                if isinstance(current_block, dict):
                    ticket_raw += self.__ticketformat(current_block)
                else:
                    pass
        ticket_raw = ticket_raw.replace("\\n", "\n").replace("\\r", "\r")
        longitud = len(ticket_raw)
        return ticket_raw, longitud

    def __ticketformat(self, block):
        list_placeholder = list()
        block_ticket = ''
        if 'Placeholder' in block:
            for placeholder in block['Placeholder'].split('|'):
                if 'STATION_NAME' == placeholder:
                    list_placeholder.append("Puebas Mock")
                elif 'STATION_CIF' == placeholder:
                    list_placeholder.append("B 12345678")
                elif 'STATION_NUMBER' == placeholder:
                    list_placeholder.append("9988899")
                elif 'STATION_PROVINCE' == placeholder:
                    list_placeholder.append("Madrid")
                elif 'TIMESTAMP' == placeholder:
                    list_placeholder.append("{:<12}{:>12}".format(date.today().strftime("%d-%m-%y"),
                                                                  datetime.now().strftime("%H:%M")))
                elif 'CARDHOLDER' == placeholder:
                    list_placeholder.append("Juan Bueno")
                elif 'EXPIRY_DATE' == placeholder:
                    list_placeholder.append("20/12")
                elif 'OP_NUMBER' == placeholder:
                    list_placeholder.append(int(self.OpNum))
                elif 'TEMPLATE_AMOUNT' == placeholder:
                    list_placeholder.append(float(self.Amount))
                elif 'OPCODE' == placeholder:
                    list_placeholder.append('{:^24}'.format(self.OpString))
                elif 'AUT_NUMBER' == placeholder:
                    list_placeholder.append(self.authonum)
                elif 'PAN_NUM' == placeholder:
                    list_placeholder.append("1234567890123456")
                elif 'ENTRY_MET' == placeholder:
                    list_placeholder.append(self.__getentrymode())
                elif 'VERI_MET' == placeholder:
                    list_placeholder.append(self.__getverificationmethod())
        if list_placeholder:
            try:
                block_ticket = block['Contenido'].format(*list_placeholder)
            except Exception as e:
                self.logging.exception(e)
                block_ticket = "[Error en bloque {0}]".format(block['Name'])
        else:
            block_ticket = block['Contenido']
        return block_ticket

    def __getentrymode(self):
        if 'CLS' == self.EntryMode:
            return 'C'
        elif 'EMV' == self.EntryMode:
            self.chipdata = 'EMV#8A023030'
            return 'E'
        elif 'SWP' == self.EntryMode:
            return 'S'
        else:
            return '*'

    def __getverificationmethod(self):
        if 'SWP' == self.EntryMode:
            return 'S'
        elif self.Amount <= 20 and ('EMV' == self.EntryMode or 'CLS' == self.EntryMode):
            return 'N'
        elif 'EMV' == self.EntryMode or 'CLS' == self.EntryMode:
            return 'P'
        else:
            return '*'

    def __getresponsecode(self):
        if not self.TrameError:
            self.Result = 'ACP'
        else:
            self.Result = 'ERR'

    def __getuploadtask(self):
        # check external value to set this value.
        return '0'

    def __log_operation(self):
        data = {'Date': date.today().strftime("%d/%m/%Y"), 'Time': datetime.now().strftime("%H:%M:%S"),
                'Result': self.Result, 'Op_Type': self.OpCode, 'Num_Op': self.OpNum, 'Entry_Mode': self.EntryMode,
                'Importe': self.Amount,'Ticket': self.ticketstring}
        self.ticket.registry_operation(data)
        return data
