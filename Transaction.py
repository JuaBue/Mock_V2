from Operation import Operation
from Response import Response
from Telecharge import Telecharge
import re
import logging

ackTemplate = "^PH24[0-9]{5}PTD0300[0-9]{14}MOK[]$"
nackTemplate = "^PH24[0-9]{5}PTD0300[0-9]{14}MNOK[]$"

class Transaction:

    def __init__(self, logging_handler, environment):
        self.logging = logging_handler
        self.environment = environment
        self.frame_length = ""
        self.type_request = ""
        self.operation = Operation(self.logging)
        self.protocolversion = "0000"
        pass

    def process(self, ped_request):
        error, request = self.__getheader(ped_request)
        if self.type_request == "PDI":
            # Operación bancaria
            data_response = self.operation.process(request)
            data_response['ProtocolVersion'] = self.protocolversion
            op_data, response = Response(self.logging, self.environment).build_response(data_response)
        elif self.type_request == "PTD":
            # Operación telecarga
            telecarga = Telecharge(self.logging, self.environment)
            if re.search(ackTemplate, ped_request, re.DOTALL):
                data_response = {}
                op_data, response = telecarga.build_response(data_response, True)
                response = ''
            elif re.search(nackTemplate, ped_request, re.DOTALL):
                data_response = {}
                op_data, response = telecarga.build_response(data_response, True)
                response = ''
            else:
                data_response = telecarga.process(request)
                data_response['ProtocolVersion'] = self.protocolversion
                op_data, response = telecarga.build_response(data_response, False)
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