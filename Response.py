from datetime import *
from time import strftime
from random import randint
from TicketDB import TDataBase

RESPONSE = 'PH24{0}PDI040099169993960000L{1}#20602#{2}#{3}#{4}#{5}##{6}##0000##{7}'
BASE_SIZE = 68
class Response:

    def __init__(self, logging_handler):
        self.logging = logging_handler
        self.ticket = TDataBase(logging_handler)
        self.TrameError = False
        self.Amount = 0
        self.EntryMode = ''
        self.OpCode = ''
        self.lastNSM = ''
        self.OpNum = ''
        self.authonum = ''.join(["{}".format(randint(0, 9)) for num in range(0, 6)])
        self.upload = self.__getuploadtask()

    def build_response(self, data_response):
        if not self.__import_data(data_response):
            return
        ticket, long = self.__buildticket()
        return RESPONSE.format("{0:0=5d}".format(BASE_SIZE + long),
                               self.__getresponsecode(),
                               date.today().strftime("%d%m%Y") + datetime.now().strftime("%H%M%S"),
                               self.lastNSM,
                               self.OpNum,
                               self.authonum,
                               self.upload,
                               ticket)

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
        elif 'AV' == self.OpCode and not self.TrameError:
            template += 'C'
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
                    list_placeholder.append('{:^24}'.format("VENTA"))
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
            return'E'
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
            return 'ACP'
        else:
            return 'ERR'

    def __getuploadtask(self):
        # check external value to set this value.
        return '0'
