from datetime import *
from random import randint
from TicketDB import TDataBase
import string
import random


RESPONSE = 'PH24{0}PDI{1}{2}3960000L{3}#20602#{4}#{5}#{6}#{7}##{8}##0000##{9}{10}{11}{12}{13}'
BASE_SIZE = 69


class Response:
    def __init__(self, logging_handler, databasetables):
        self.logging = logging_handler
        self.ticket = TDataBase(logging_handler, False)
        self.databasetables = databasetables
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
        self.qrcode = ''
        self.Result = ''
        self.ecouponing = ''
        self.Merchant = ''
        self.TopUp = ''
        self.track2 = ''
        self.cabeceratopup = 'FALTA CONFIG'
        self.pietopup = 'FALTA CONFIG'
        self.telephone = 'XXXXXXXXX'
        self.topupoperation = '00000000000000000000'
        self.topupanulop = 0
        self.topuprt = '2828000000000' + ''.join(["{}".format(randint(0, 9)) for num in range(7)])
        self.authonum = ''.join(["{}".format(randint(0, 9)) for num in range(0, 6)])
        self.upload = '0'
        self.opemode = 'N'
        self.movementinfo = ''
        self.movementdata = ''
        self.discount = {}

    def build_response(self, data_response, environment):
        if not self.__import_data(data_response):
            return
        if not self.__import_environment(environment):
            return
        self.__getresponsecode()
        self.ticketstring, long = self.__buildticket()
        op_data = self.__log_operation()
        long = long + len(self.chipdata) + len(self.ecouponing) + len(self.movementdata) + len(self.qrcode)
        response = RESPONSE.format("{0:0=5d}".format(BASE_SIZE + long),
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
                               self.qrcode,
                               self.ecouponing,
                               self.movementdata)
        self.logging.info("[TX] {0}".format(response))
        return op_data, response

    def __import_data(self, data_response):
        self.__reset_data()
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
        if 'track2' in data_response:
            self.track2 = data_response['track2']
        if 'TopUp' in data_response:
            self.TopUp = data_response['TopUp']
        if 'Telephone' in data_response:
            self.telephone = data_response['Telephone']
        if 'TopUpOps' in data_response:
            self.topupoperation = data_response['TopUpOps']
        if 'TopUpOpsAmount' in data_response:
            self.topupamount = float(data_response['TopUpOpsAmount']) / 100
        if 'TopUpOpsAnulaOP' in data_response:
            self.topupanulop = int(data_response['TopUpOpsAnulaOP'])
        if 'Discount' in data_response:
            self.discount = data_response['Discount']
        return True

    def __import_environment(self, environment):
        if 'QRStatus' in environment:
            if 'Qr' in environment and environment['QRStatus']:
                self.qrcode = 'SBQRSO{0}'.format(environment['Qr'])
            else:
                self.qrcode = ''
        if 'EcupStatus' in environment:
            if 'EcupImage' in environment and environment['EcupStatus']:
                self.ecouponing = '{0}####------------------------'.format(environment['EcupImage'])
            else:
                self.ecouponing = ''
        if 'TelechargeType' in environment:
            self.upload = environment['TelechargeType']
        if 'OperationMode' in environment:
            self.opemode = environment['OperationMode']
        if 'MovementeInfo' in environment:
            self.movementinfo = environment['MovementeInfo']
        return True

    def __reset_data(self):
        self.TrameError = False
        self.Amount = 0
        self.EntryMode = ''
        self.OpCode = ''
        self.lastNSM = ''
        self.OpNum = ''
        self.ProtocolVersion = ''
        self.Merchant = ''
        self.TopUp = ''
        self.telephone = 'XXXXXXXXX'
        self.topupoperation = '00000000000000000000'
        self.topupamount = 0
        self.topuprt = '2828000000000' + ''.join(["{}".format(randint(0, 9)) for num in range(7)])
        self.authonum = ''.join(["{}".format(randint(0, 9)) for num in range(0, 6)])
        self.topupanulop = 0
        self.opemode = 'N'
        self.track2 = ''
        self.discount = {}

    def __buildticket(self):
        template = ''
        ticket_raw = ''
        # Operation result
        if self.Result == 'ACP':
            template += 'A'
        else:
            template += 'D'

        # Transaction type
        if 'V ' == self.OpCode and not self.TrameError:
            template += 'S'
            self.OpString = 'VENTA'
            self.movementdata = self.__getmovementdata()
        elif 'BF' == self.OpCode and not self.TrameError:
            template += 'S' # TODO: esto hay que cambiarlo
            self.OpString = 'RESCATE'
            self.__LYTdataCepsa()
            self.movementdata = self.__getmovementdata()
        elif 'B ' == self.OpCode and not self.TrameError:
            template += 'S' # TODO: esto hay que cambiarlo
            self.OpString = 'ACUMULACION'
            self.movementdata = self.__getmovementdata()
        elif 'AV' == self.OpCode and not self.TrameError:
            template += 'C'
            self.OpString = 'DEVOLUCION'
        elif 'PAP' == self.OpCode and not self.TrameError:
            template += 'P'
            self.OpString = 'PREAUTORIZACION'
        elif 'PAC' == self.OpCode and not self.TrameError:
            template += 'K'
            self.OpString = 'VENTA CONFIRMADA'
        elif 'MRL' == self.OpCode and not self.TrameError:
            template += 'T'
            self.OpString = 'RECARGA'
            topupinfo = self.__gettopupinfo(self.TopUp)
            self.cabeceratopup = topupinfo['Cabecera']
            self.pietopup = topupinfo['Pie']
            self.movementdata = '######' + str(int(self.topupamount) * 100) + '##'
        elif 'AML' == self.OpCode and not self.TrameError:
            template += 'Q'
            self.OpString = 'RECARGA'
            topupinfo = self.__gettopupinfo(self.TopUp)
            self.cabeceratopup = topupinfo['Cabecera']
            self.pietopup = topupinfo['Pie']
            self.movementdata = '######' + str(int(self.topupamount) * 100) + '##'
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
                self.Result = 'ERR'
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
                elif 'CABEZ_TP' == placeholder:
                    list_placeholder.append(self.cabeceratopup)
                elif 'PIE_TP' == placeholder:
                    list_placeholder.append(self.pietopup)
                elif 'TELEPHONE' == placeholder:
                    list_placeholder.append(self.telephone)
                elif 'TOPUPCOMPANY' == placeholder:
                    list_placeholder.append(self.TopUp)
                elif 'TOPUPOPERATION' == placeholder:
                    list_placeholder.append(self.topupoperation)
                elif 'TOPUP_AMOUNT' == placeholder:
                    list_placeholder.append("{:10.2f}".format(self.topupamount))
                elif 'TOPUPREFOP' == placeholder:
                    list_placeholder.append(''.join(random.SystemRandom().choice(string.ascii_uppercase +
                                                                                 string.digits) for _ in range(6)))
                elif 'TOPUPRT' == placeholder:
                    list_placeholder.append(self.topuprt)
                elif 'TOPUPANULOP' == placeholder:
                    list_placeholder.append(self.topupanulop)

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
        if self.opemode == 'N':
            if not self.TrameError:
                self.Result = 'ACP'
            else:
                self.Result = 'ERR'
        elif self.opemode == 'A':
            self.Result = 'ACP'
        elif self.opemode == 'E':
            self.Result = 'ERR'

    def __getmovementdata(self):
        if self.movementinfo and 'movementdata' in self.movementinfo and self.movementinfo['movementdata']:
            movementdata = self.__getpan(self.track2) + '#' + \
                           self.movementinfo['movegroup'] + '#' + \
                           self.movementinfo['movecierre'] + '#' + \
                           self.movementinfo['movegroup'] + '#' + '#' + \
                           self.movementinfo['movedescrip'] + '#' + \
                           self.movementinfo['movedescount'] + '#' + '#' + \
                           self.movementinfo['movenumprod'] + '' + \
                           self.movementinfo['moveproductinfo']
        else:
            movementdata = ''
        return movementdata

    def __LYTdataCepsa(self):
        if 'Amount' in self.discount:
            amount = self.discount['Amount']
        else:
            amount = '0000000000'
        return 'LYT#' + random.randint(1, 99) + '#' + date.today().strftime("%Y%m%d") + \
               '#001428#201259501259503084210505070519006085#CEPSA#9997003084#999700#' + '#0000000000'

    def __log_operation(self):
        data = {'Date': date.today().strftime("%d/%m/%Y"), 'Time': datetime.now().strftime("%H:%M:%S"),
                'Result': self.Result, 'Op_Type': self.OpCode, 'Num_Op': self.OpNum, 'Entry_Mode': self.EntryMode,
                'Importe': self.Amount,'Ticket': self.ticketstring}
        self.ticket.registry_operation(data)
        return data

    def __gettopupinfo(self, topupid):
        topupinfo = self.databasetables.obtain_table_p(topupid)
        return {'Cabecera': topupinfo['Mensaje Cabec.'], 'Pie': topupinfo['Mensaje Pie']}

    def __hashpan(self, pan):
        return pan.split('=')[0][:6] + '******' + pan.split('=')[0][-4:]

    def __getpan(self, trak2):
        return trak2.split('=')[0]