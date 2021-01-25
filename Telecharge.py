from datetime import *
from time import strftime
from random import randint
from TicketDB import TDataBase


RESPONSE = 'PH24{0}PTD{1}{2}1100000LACPTELE#{3}#{4}##P{5}#1#{6}#{7}#01#00015#0#0#{8}#{8}#H24QA#P4wtY24s'
BASE_SIZE = 79
MAX_LENGTH_HEADER = 30
DC1_DELIMETER = ''
DC2_DELIMETER = ''
DC3_DELIMETER = ''
HASH_DELIMETER = '#'


class Telecharge:
    def __init__(self, logging_handler, environment):
        self.logging = logging_handler
        self.ParsePos = 0
        self.error = False
        self.Ptable = '000'
        self.TrameError = False
        self.OpCode = ''
        self.lastNSM = ''
        self.ProtocolVersion = ''
        self.Merchant = ''
        self.SWversion = '062339'

    def build_response(self, data_response, isAckFrame):
        if not isAckFrame:
            if not self.__import_data(data_response):
                return
        op_data = self.__log_operation()
        long = len(self.SWversion) * 2
        return op_data, RESPONSE.format("{0:0=5d}".format(BASE_SIZE + long),
                               self.ProtocolVersion,
                               self.Merchant,
                               date.today().strftime("%d%m%y") + datetime.now().strftime("%H%M"),
                               self.lastNSM,
                               self.Ptable,
                               date.today().strftime("%d%m%y"),
                               datetime.now().strftime("%H%M"),
                               self.SWversion)

    def __import_data(self, data_response):
        if 'Error' in data_response:
            self.TrameError = data_response['Error']
        if 'OpCode' in data_response:
            self.OpCode = data_response['OpCode']
        if 'ProtocolVersion' in data_response:
            self.ProtocolVersion = data_response['ProtocolVersion']
        return True

    def process(self, ped_request):
        # Type of Request.
        type_request, b_error = self.__type_request(ped_request)
        if b_error:
            self.logging.error("Error in Operation.")
            self.error = True
        self.Merchant, b_error = self.__business_id(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Merchant ID.")
            self.error = True
        tables_version, b_error = self.__tables_version(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in tables version.")
            self.error = True
        self.lastNSM, b_error = self.__last_NSM(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in last NSM.")
            self.error = True
        return {}

    def __type_request(self, ped_request):
        (type_request, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Type of Request\t{0}".format(type_request))
        if type_request != "DI" and type_request != "RI":
            self.logging.info("Error in Type of request. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return type_request, b_error

    def __business_id(self, ped_request):
        (merchant, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Business ID\t{0}".format(merchant))
        if (not merchant.isdigit()) or merchant == "0000000":
            self.logging.info("Error in Business. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return merchant, b_error

    def __tables_version(self, ped_request):
        (tables_version, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Tables version \t{0}".format(tables_version))
        if tables_version == '':
            self.logging.info("Error in tables version. Bad format.")
            b_error = True
        else:
            SvTable, localpos = self.__get_field('.', ped_request)
            PvTable, localpos = self.__get_field('.', ped_request[localpos:])
            self.Ptable = '{0:03d}'.format(int(PvTable[1:]) + 1)
            self.ParsePos = self.ParsePos + position
            b_error = False
        return tables_version, b_error

    def __last_NSM(self, ped_request):
        (last_NSM, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Last NSM \t{0}".format(last_NSM))
        if int(last_NSM) <= 0:
            self.logging.info("Error in last NSM. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            self.LastNSM = last_NSM
            b_error = False
        return last_NSM, b_error

    def __log_operation(self):
        data = {'Date': date.today().strftime("%d/%m/%Y"), 'Time': datetime.now().strftime("%H:%M:%S"),
                'Result': 'ACP', 'Op_Type': 'TELE', 'Num_Op': '', 'Entry_Mode': '',
                'Importe': '', 'Ticket': ''}
        return data

    @staticmethod
    def __get_field(delimeter, string):
        get_field = ""
        position = 0
        for i in string:
            position = position + 1
            if i == delimeter:
                return get_field, position
            else:
                get_field = get_field + i