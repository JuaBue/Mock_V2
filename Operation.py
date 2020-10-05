from datetime import *
from time import strftime

MAX_LENGTH_HEADER = 30
DC1_DELIMETER = ''
HASH_DELIMETER = '#'


class Operation:

    def __init__(self, logging_handler):
        self.logging = logging_handler
        self.ParsePos = 0
        self.LastNSM = 0
        self.OpNum = 0

    def process(self, ped_request):
        # Type of Request.
        type_request, b_error = self.__type_request(ped_request)
        if b_error:
            self.logging.error("Error in Operation.")
            return
        merchant, b_error = self.__business_id(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Merchant ID.")
            return
        type_machine, b_error = self.__type_machine(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Type of machine.")
            return
        id_terminal, b_error = self.__id_terminal(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in ID of terminal.")
            return
        tables_version, b_error = self.__tables_version(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in tables version.")
            return
        last_NSM, b_error = self.__last_NSM(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in last NSM.")
            return
        operation_number, b_error = self.__operation_number(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Operation number.")
            return
        mydata = self.__build_data()
        return mydata

    def __type_request(self, ped_request):
        (type_request, position) = self.__get_field(DC1_DELIMETER, ped_request)
        self.logging.info("Type of Request\t{0}".format(type_request))
        if (type_request != "M") and (type_request != "R") and (type_request != "D"):
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

    def __type_machine(self, ped_request):
        (type_machine, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Type of machine\t{0}".format(type_machine))
        if type_machine not in ['3', '8', '6', 'T', 't']:
            self.logging.info("Error in Type of machine. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return type_machine, b_error

    def __id_terminal(self, ped_request):
        (id_terminal, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Id. of terminal\t{0}".format(id_terminal))
        if id_terminal == '':
            self.logging.info("Error in ID Terminal. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return id_terminal, b_error

    def __tables_version(self, ped_request):
        (tables_version, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Tables version \t{0}".format(tables_version))
        if tables_version == '':
            self.logging.info("Error in tables version. Bad format.")
            b_error = True
        else:
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

    def __operation_number(self, ped_request):
        (operation_number, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("operation number \t{0}".format(operation_number))
        if int(operation_number) <= 0:
            self.logging.info("Error in operation number. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            self.OpNum = operation_number
            b_error = False
        return operation_number, b_error

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

    def __build_data(self):
        data = {}
        data['lastNSM'] = self.LastNSM
        data['OpNum'] = self.OpNum
        return data
