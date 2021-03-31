from Response import Response
from Telecharge import Telecharge, DC2_DELIMETER
import re
import logging

MAX_LENGTH_HEADER = 30
DC1_DELIMETER = ''
DC2_DELIMETER = ''
DC3_DELIMETER = ''
HASH_DELIMETER = '#'

ackTemplate = "^PH24[0-9]{5}PTD0300[0-9]{14}MOK[]$"
nackTemplate = "^PH24[0-9]{5}PTD0300[0-9]{14}MNOK[]$"


class TablePException(Exception):
    pass


class Transaction:

    def __init__(self, logging_handler, databasetables):
        self.logging = logging_handler
        self.databasetables = databasetables
        self.response = Response(self.logging, self.databasetables)
        self.frame_length = ""
        self.type_request = ""
        self.protocolversion = "0000"
        self.ParsePos = 0
        self.error = False
        self.operation_code = ''
        self.amount = 0
        self.entrymode = ''
        self.LastNSM = 0
        self.OpNum = 0
        self.Merchant = ''
        self.telephone = ''
        self.topupoperation = ''
        self.giftprodcode = ''
        self.topupanulope = 0
        self.topupamount = '000000000'
        self.track_2 = ''
        pass

    def process(self, ped_request, environment):
        error, request = self.__getheader(ped_request)
        if self.type_request == "PDI":
            # Operación bancaria
            # data_response = self.operation.process(request)
            data_response = self.parsetrame(request)
            data_response['ProtocolVersion'] = self.protocolversion
            op_data, response = self.response.build_response(data_response, environment)
        elif self.type_request == "PTD":
            # Operación telecarga
            telecarga = Telecharge(self.logging)
            if re.search(ackTemplate, ped_request, re.DOTALL):
                data_response = {}
                op_data, response = telecarga.build_response(data_response, True, environment)
                response = ''
            elif re.search(nackTemplate, ped_request, re.DOTALL):
                data_response = {}
                op_data, response = telecarga.build_response(data_response, True, environment)
                response = ''
            else:
                data_response = telecarga.process(request)
                data_response['ProtocolVersion'] = self.protocolversion
                if self.__checkTablePInfo(environment):
                    op_data, response = telecarga.build_response(data_response, False, environment)
                else:
                    raise TablePException
        elif self.type_request == "PPL":
            # Operación Polling
            pass
        else:
            self.logging.error("Type of request unknown: {0}".format(self.type_request))
            # Operación no conocida.
        return op_data, response

    def __getheader(self, ped_request):
        header = ped_request[:30]
        if header[:4] != "PH24":
            self.logging.error("Error in header {0}".format(header[:4]))
            return False, "Error in header {0}".format(header[:4])
        # Longitud de la trama.
        if not header[4:9].isdigit():
            self.logging.error("Error in header {0}".format(header[4:9]))
            return False, "Error in header {0}".format(header[4:9])
        self.frame_length = int(header[4:9])
        # Identificador de mensaje "PTD" para telecargas.
        if header[9:12] != "PDI" and header[9:12] != "PPL" and header[9:12] != "PTD":
            self.logging.error("Error in header {0}".format(header[9:12]))
            return False, "Error in header {0}".format(header[9:12])
        self.type_request = header[9:12]
        # Version del protocolo, 0300 para telecargas.
        if header[12:16] != "0400" and header[12:16] != "0500" and header[12:16] != "0200" and header[12:16] != "0401" \
                and header[12:16] != "0300":
            self.logging.error("Error in header {0}".format(header[12:16]))
            return False, "Error in header {0}".format(header[12:16])
        self.protocolversion = header[12:16]
        # Identificador del mensaje.
        if not header[16:27].isdigit():
            self.logging.error("Error in header {0}".format(header[16:27]))
            return False, "Error in header {0}".format(header[16:27])
        # Error Code, no usado, siempre "000".
        if header[27:] != "000":
            self.logging.error("Error in header {0}".format(header[27:]))
            return False, "Error in header {0}".format(header[27:])
        return True, ped_request[30:]

    def parsetrame(self, ped_request):
        #Reset position
        self.ParsePos = 0
        # Type of Request.
        type_request, b_error = self.__type_request(ped_request)
        if b_error:
            self.logging.error("Error in Operation.")
            self.error = True
        self.Merchant, b_error = self.__business_id(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Merchant ID.")
            self.error = True
        type_machine, b_error = self.__type_machine(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Type of machine.")
            self.error = True
        id_terminal, b_error = self.__id_terminal(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in ID of terminal.")
            self.error = True
        tables_version, b_error = self.__tables_version(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in tables version.")
            self.error = True
        last_NSM, b_error = self.__last_NSM(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in last NSM.")
            self.error = True
        operation_number, b_error = self.__operation_number(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Operation number.")
            self.error = True
        offline_transaction, b_error = self.__offline_transaction(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Offline Transaction.")
            self.error = True
        operation_mode, b_error = self.__operation_mode(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Operation Mode.")
            self.error = True
        currency, b_error = self.__currency(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Currency.")
            self.error = True
        language, b_error = self.__language(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Language.")
            self.error = True
        # This fields are not present in the command PAR
        if ped_request[self.ParsePos] is not DC2_DELIMETER:
            type_card, b_error = self.__type_card(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Language.")
                self.error = True
            track_1, b_error = self.__track_1(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Track 1.")
                self.error = True
            self.track_2, b_error = self.__track_2(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Track 2.")
                self.error = True
        else:
            self.ParsePos = self.ParsePos + 1
        extra_data_card, b_error = self.__extra_data_card(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Extra Data Card.")
            self.error = True
        chip_data, b_error = self.__chip_data(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Chip Data.")
            self.error = True
        type_payment, b_error = self.__type_payment(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Type Payment.")
            self.error = True
        operation_code, b_error = self.__operation_code(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Operation Code.")
            self.error = True
        number_products, b_error = self.__number_products(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Number of products.")
            self.error = True
        total_amount, b_error = self.__total_amount(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Total amount.")
            self.error = True
        euro_litres_point, b_error = self.__euro_l_point(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Euro Litres/Point.")
            self.error = True
        if operation_code != 'AML':
            self.giftprodcode, b_error = self.__gift_prodcode(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Gift Product Code.")
                self.error = True
            unit_price, b_error = self.__unit_price(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Unit Price.")
                self.error = True
            quantity, b_error = self.__quantity_litres(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Quantity.")
                self.error = True
            amount, b_error = self.__amount(ped_request[self.ParsePos:])
            if b_error:
                self.logging.error("Error in Amount.")
                self.error = True
        discount_product, b_error = self.__discount_product(ped_request[self.ParsePos:])
        if b_error:
            self.logging.error("Error in Discount product.")
            self.error = True
        extra_data, b_error = self.__extradata(ped_request[self.ParsePos:])
        # type of entry
        if '1' == type_card:
            self.entrymode = 'SWP'
        elif '2' == type_card:
            self.entrymode = chip_data[:3]
        elif '' == type_card and 'MRL' == self.operation_code:
            self.entrymode = 'MRL'
        elif '' == type_card and 'AML' == self.operation_code:
            self.entrymode = 'AML'
        else:
            self.error = True
            self.entrymode = 'ERR'
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
        if not operation_number.isdigit():
            self.logging.info("Error in operation number. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            self.OpNum = operation_number
            b_error = False
        return operation_number, b_error

    def __offline_transaction(self, ped_request):
        (offline_transaction, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Offline Transaction \t{0}".format(offline_transaction))
        if offline_transaction == '' or len(offline_transaction) != 3:
            self.logging.info("Error in offline transaction. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return offline_transaction, b_error

    def __operation_mode(self, ped_request):
        operation_mode_key = {'0': 'Autonomous', '1': 'Unattended', '2': 'Polling Attended', '3': 'Polling Unattended'}
        (operation_mode, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Operation Mode \t{0} : {1}".format(operation_mode, operation_mode_key[operation_mode]))
        if operation_mode == '' or len(operation_mode) > 2 or operation_mode not in ['0', '1', '2', '3']:
            self.logging.info("Error in operation mode. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return operation_mode, b_error

    def __currency(self, ped_request):
        currency_key = {'978': 'EUR', '840': 'USD', '826': 'GBP', '756': 'CHF'}
        (currency, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Currency \t{0} : {1}".format(currency, currency_key[currency]))
        if currency not in currency_key:
            self.logging.info("Error in Currency. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return currency, b_error

    def __language(self, ped_request):
        language_key = {'DA': 'Danish', 'DE': 'German', 'EL': 'Greek', 'EN': 'English', 'ES': 'Spanish',
                        'FI': 'Finnish', 'FR': 'French', 'NL': 'Dutch', 'PT': 'Portuguese', 'SW': 'Swedish'}
        (language, position) = self.__get_field(DC1_DELIMETER, ped_request)
        self.logging.info("Language \t{0} : {1}".format(language, language_key[language]))
        if language not in language_key:
            self.logging.info("Error in language. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return language, b_error

    def __type_card(self, ped_request):
        type_card_key = {'1': 'Swiped', '2': 'EMV', '': 'REC'}
        (type_card, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Type card \t{0} : {1}".format(type_card, type_card_key[type_card]))
        if type_card not in type_card_key:
            self.logging.info("Error in type card. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return type_card, b_error

    def __track_1(self, ped_request):
        (track_1, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Track 1 \t{0}".format(track_1))
        self.ParsePos = self.ParsePos + position
        b_error = False
        return track_1, b_error

    def __track_2(self, ped_request):
        (track_2, position) = self.__get_field(DC2_DELIMETER, ped_request)
        self.logging.info("Track 1 \t{0}".format(track_2))
        self.ParsePos = self.ParsePos + position
        return track_2, False

    def __extra_data_card(self, ped_request):
        (extra_data_card, position) = self.__get_field(DC3_DELIMETER, ped_request)
        self.logging.info("Extra Data Card \t{0}".format(extra_data_card))
        self.ParsePos = self.ParsePos + position
        b_error = False
        return extra_data_card, b_error

    def __chip_data(self, ped_request):
        (chip_data, position) = self.__get_field(DC1_DELIMETER, ped_request)
        self.logging.info("Chip Data \t{0}".format(chip_data))
        self.ParsePos = self.ParsePos + position
        b_error = False
        return chip_data, b_error

    def __type_payment(self, ped_request):
        (type_payment, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Type Payment \t{0}".format(type_payment))
        if (type_payment and type_payment != '1') and (type_payment and type_payment != '2'):
            self.logging.info("Error in type Payment. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return type_payment, b_error

    def __operation_code(self, ped_request):
        operation_code_key = {'V ': 'Venta', 'A ': "Anulacion Generica", 'CP': 'Consulta Puntos',
                              'AV': 'Anulacion Venta',
                              'BF': 'Bonus sale', 'AF': 'Bonus redemption', 'RF': 'Bonus balance sale',
                              'CF': 'Bonus query - CEPSA', 'PAP': 'Unattended Preauthorization',
                              'PAC': 'Preauthorization confirmation', 'PAN': 'Preauthorization cancellation',
                              'AT': 'Partial Refund operation', 'APP': 'Explicit transaction reversal',
                              'AP': 'Automatic cancellation',
                              'VY': 'DCC query', 'MRL': 'TopUps', 'AML': 'Anulacion TopUps'}
        (operation_code, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Operation Code \t{0} : {1}".format(operation_code, operation_code_key[operation_code]))
        if (operation_code not in operation_code_key) and len(operation_code) != 3:
            self.logging.info("Error in operation code. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
            self.operation_code = operation_code
        return operation_code, b_error

    def __number_products(self, ped_request):
        (number_products, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Number of products \t{0}".format(number_products))
        if (number_products.isdigit()) and (len(number_products) != 2):
            self.logging.info("Error in Number of products. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return number_products, b_error

    def __total_amount(self, ped_request):
        (total_amount, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Total amount \t{0}".format(total_amount))
        if not total_amount:
            self.ParsePos = self.ParsePos + position
            b_error = False
            self.amount = 0
        elif (total_amount.isdigit()) and (len(total_amount) < 2):
            self.logging.info("Error in total amount. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
            self.amount = float(total_amount)/100
        return total_amount, b_error

    def __euro_l_point(self, ped_request):
        (euro_l_point, position) = self.__get_field(DC2_DELIMETER, ped_request)
        self.logging.info("Euro Litres/Point \t{0}".format(euro_l_point))
        if euro_l_point and ((euro_l_point.isdigit()) or (int(euro_l_point) < 0)):
            self.logging.info("Error in Euro Litres/Point. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return euro_l_point, b_error

    def __gift_prodcode(self, ped_request):
        (gift_prodcode, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Gift Product Code \t{0}".format(gift_prodcode))
        if gift_prodcode and ((gift_prodcode.isdigit()) or (int(gift_prodcode) < 0)) and (len(gift_prodcode) > 6):
            self.logging.info("Error in Gift Product Code. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return gift_prodcode, b_error

    def __unit_price(self, ped_request):
        (unit_price, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Unit Price \t{0}".format(unit_price))
        if unit_price and (not unit_price.isdigit()):
            self.logging.info("Error in Unit Price. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return unit_price, b_error

    def __quantity_litres(self, ped_request):
        (quantity_litres, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Quantity Litres \t{0}".format(quantity_litres))
        if quantity_litres and (not quantity_litres.isdigit() or (int(quantity_litres) < 0)):
            self.logging.info("Error in Quantity Litres. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return quantity_litres, b_error

    def __amount(self, ped_request):
        (amount, position) = self.__get_field(HASH_DELIMETER, ped_request)
        self.logging.info("Amount \t{0}".format(amount))
        self.ParsePos = self.ParsePos + position
        b_error = False
        return amount, b_error

    def __discount_product(self, ped_request):
        (discount_product, position) = self.__get_field(DC1_DELIMETER, ped_request)
        self.logging.info("Discount Product \t{0}".format(discount_product))
        if discount_product and (not discount_product.isdigit() or (int(discount_product) < 0)):
            self.logging.info("Error in Discount Product. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
        return discount_product, b_error

    def __extradata(self, ped_request):
        (extra_data, position) = self.__get_field(DC2_DELIMETER, ped_request)
        self.logging.info("Extra data \t{0}".format(extra_data))
        if not extra_data:
            self.logging.info("Error in Extra data. Bad format.")
            b_error = True
        else:
            self.ParsePos = self.ParsePos + position
            b_error = False
            dataextra = extra_data.split('/')
            for value in dataextra:
                if not value:
                    pass
                elif value[0] == 'T':
                    self.telephone = value[1:]
                elif value[0] == 'X':
                    self.topupoperation = value[1:]
                elif value[0] == 'A':
                    self.topupanulope = value[1:]
                elif value[0] == 'K':
                    self.giftprodcode = value[1:]
                elif value[0] == 'P':
                    self.topupamount = value[1:]
        return extra_data, b_error

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

    def __checkTablePInfo(self, environment):
        noerror = False
        if 'TablePinfo' in environment:
            if environment['TablePinfo'].keys() >= {'swversion', 'ftpuser', 'passftp'}:
                if environment['TablePinfo']['swversion'] and environment['TablePinfo']['ftpuser'] \
                        and environment['TablePinfo']['passftp']:
                    noerror = True
        return noerror

    def __build_data(self):
        if 'MRL' == self.entrymode or 'AML' == self.entrymode:
            data = {'Error': self.error, 'Amount': self.amount, 'EntryMode': self.entrymode,
                    'OpCode': self.operation_code, 'lastNSM': self.LastNSM, 'OpNum': self.OpNum,
                    'MerchantID': self.Merchant, 'track2': self.track_2, 'TopUp': self.giftprodcode,
                    'Telephone': self.telephone, 'TopUpOps': self.topupoperation,
                    'TopUpOpsAmount': self.topupamount, 'TopUpOpsAnulaOP': self.topupanulope}
        else:
            data = {'Error': self.error, 'Amount': self.amount, 'EntryMode': self.entrymode,
                    'OpCode': self.operation_code, 'lastNSM': self.LastNSM, 'OpNum': self.OpNum,
                    'MerchantID': self.Merchant, 'track2': self.track_2}
        return data

