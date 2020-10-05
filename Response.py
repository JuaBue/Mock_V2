from datetime import *
from time import strftime

RESPUESTA = 'PH2401790PDI040099169997620000LACP#98813#{0}#708346{1}#{2}#270195##0####S#SBGENM************************!!!!!!!!ATENCION!!!!!!!!  NUNCA FACILITE EL PIN      POR TELEFONO        H24 NO ESTA REALIZANDO   NINGUNA COMPROBACION  ************************\n\nSBGENM\nSBGENM\nSBGENM\nS#SBGENM\nE.S.CRUCE DE ESPEJA, S.AA 28960649\n0135601-MADRID\n\n*************************  COPIA  -  COMERCIO  *************************\n      paysafecard       ************************30-09-2020         09:15\nNRO. DE OPER: 40498\nCPRF: 1200\nN.SERIE: 2153181655\n\n------------------------Imp. Cupon :      10,00 ------------------------A PAGAR:           10,00\n\n*******************************ATENCION********************************** NO FACILITAR COD.PIN **POR E-MAIL O TELEFONO *************************\n\nSBGENM\nSBGENM\nSBGENM\nSBGENM\nC#SBGENM\nE.S.CRUCE DE ESPEJA, S.AA 28960649\n0135601-MADRID\n\n************************ ORIGINAL PARA CLIENTE  ************************      paysafecard       ************************   YA TIENE DISPONIBLE   LOS IMPORTES DE 10, 25,50 Y 100E DE PAYSAFECARD   EN SU TERMINAL H24   ************************30-09-2020         09:15\n\nNRO. DE OPER: 40498\nCPRF: 1200\nN.SERIE: 2153181655\n------------------------Imp. Cupon :      10,00 ------------------------!!!!!!  ATENCION  !!!!!!  NUNCA DAR CODIGO PIN    POR TELEFONO O EMAIL\nPIN: \nXXXX XXXX XXXX XXXX \n\n------------------------ CUSTODIAR Y CONSERVAR       ESTE TICKET ES          RESPONSABILIDAD     EXCLUSIVA DEL CLIENTE  ------------------------A PAGAR:           10,00------------------------PRODUCTO EXENTO DE IVA  INSTRUCCIONES DE USO:\nSELECIONAR TIENDA ONLINEOPCION PAGO: PAYSAFECARDINTRODUCIR CODIGO PIN   INFO WWW.PAYSAFECARD.COMAT.CLIENTE:\nwww.paysafecard.com/help------------------------ MEDIO PAGO EMITIDO POR PAYSAFE PREPAID SERVICES  PROHIBIDA LA REVENTA  \n\n\n######1000##'
class Response:

    def __init__(self, logging_handler):
        self.logging = logging_handler
        self.lastNSM = ''
        self.OpNum = ''

    def build_response(self, data_response):
        if not self.__import_data(data_response):
            return
        DateResponse = date.today().strftime("%d%m%Y")
        TimeResponse = datetime.now().strftime("%H%M%S")
        Time = DateResponse + TimeResponse
        return RESPUESTA.format(Time, self.lastNSM, self.OpNum)


    def __import_data(self, data_response):
        if 'lastNSM' in data_response:
            self.lastNSM = data_response['lastNSM']
        if 'OpNum' in data_response:
            self.OpNum = data_response['OpNum']
        return True