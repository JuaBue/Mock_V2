from sqlalchemy import *
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey, exc, desc
from sqlalchemy.orm import relationship


class DataBase:

    def __init__(self, logging_handler, drop_database=True):
        self.logging = logging_handler
        self.engine = None
        self.metadata = None
        self.tableEE = None
        self.engine_name = 'sqlite:///telecharge.db'

        self.init_engine()
        if drop_database:
            self.dropdb()
            self.init_engine()

    def init_engine(self):
        self.engine = create_engine(self.engine_name, echo=False)
        self.metadata = MetaData(bind=None)

        # TABLA E: TABLA DE DATOS DE ESTACIÓN
        self.tableEE = Table('Table_EE', self.metadata,
                             Column('EESS', String(7), nullable=False),  # Número de estación H24.
                             Column('CIF', String(10), nullable=False),  # CIF de la estación de servicio
                             Column('NOMB', String(24), nullable=False),  # Nombre de la estación de servicio
                             Column('Provincia', String(14), nullable=False),  # Provincia
                             Column('DLG', String(2), nullable=False),  # Delegación del TPV
                             Column('HH', String(4), nullable=False),  # Hora Comunicación
                             Column('CIE', String(1), nullable=False),  # Tiene Cierre 4B (Valores 0, 1)
                             Column('CEN', String(1), nullable=False),  # Tiene centralita (Valores 0, 1)
                             Column('PASS', String(5), nullable=False),  # Password de antenimiento
                             Column('CHQO', String(2), nullable=False),  # Cheques off/cupones sin comunicar
                             Column('CHQA', String(1), nullable=False),  # Tipos de cheques activos
                             Column('LANG', String(2), nullable=False),  # Idioma del TPV
                             Column('CURR', String(3), nullable=False)  # Divisa del TPV
                             )
        # TABLA u: TABLA PARAMETROS DE COMUNICACIÓN
        self.tableu = Table('Table_u', self.metadata,
              Column('NEMO_ACCESO', String(3), nullable=False),  # Tag tipo de acceso permitido
              Column('NEMO_VALOR', String(3), nullable=False),  # Tag indicativo del parámetro
              Column('VALOR', String(50), nullable=False)  # Valor del parámetro
              )
        # TABLA F: TABLA DE BINES
        self.tableFF = Table('Table_FF', self.metadata,
              Column('Bin', String(6), nullable=False),  # Numero de Bin
              Column('Operativa', String(1), nullable=False),  # Operativa para el despliegue del menu
              Column('Datos Adicionales', String(1), nullable=False),  # Control sobre las pistas para la petición
              # de datos adicionales
              Column('Kilometros', String(1), nullable=False),  # Pedir Kilometros
              Column('Codigo de Vehiculo', String(1), nullable=False),  # Pedir codigo de vehiculo
              Column('Codigo de Conductor', String(1), nullable=False),  # Pedir codigo de conductor
              Column('Matricula', String(1), nullable=False),  # Padir Matricula
              Column('Pin', String(1), nullable=False),  # Pedir PIN
              Column('DNI', String(1), nullable=False),  # Pedir DNI
              Column('Control Especial', String(1), nullable=False),  # Dato para pedir algún control sobre la
              # tarjeta antes de realizar la transacción
              Column('Localizacion', String(1), nullable=False),  # Dato del pais
              Column('Letra de Premio', String(1), nullable=False),  # Letra para el despliegue de Menú cuando la
              # operativa es "B".
              Column('Longitud', String(2), nullable=False),  # Longitud del PAN para su validación.
              Column('Acción', String(1), nullable=False)  # Alta, Baja o Modificación.
              )
        # TABLA x: TABLA BINES PAGO EMV
        self.tablex = Table('Table_x', self.metadata,
              Column('Lognitud BIN', String(1), nullable=False),  # Longitud del BIN
              Column('BIN', String(6), nullable=False)  # BIN sobre el cual se tiene que hacer el control del código
              # de servicio.
              )
        # TABLA m: TABLA DE PARAMETROS GENERALES EMV
        self.tablem = Table('Table_m', self.metadata,
              Column('Terminal Type', String(2), nullable=False),  # Tag 9F35: según ICS
              Column('Terminal Capabilities', String(6), nullable=False),  # Tag 9F33: según ICS
              Column('Additional Terminal Capabilities', String(10), nullable=False),  # Tag 9F40 según ICS
              Column('Transaction Category Code', String(2), nullable=False),  # Tag 9F53
              Column('Merchant Category Code', String(4), nullable=False),  # Tag 9F15
              Column('Terminal Country Code', String(3), nullable=False),  # Tag 9F1A
              Column('Flags EMV', String(2), nullable=False)  # Ver tabla siguiente ¿?
              )
        # TABLA v: TABLA DE APLICACIONES EMV (AIDs)
        self.tablev = Table('Table_v', self.metadata,
              Column('AID', String(32), nullable=False),  # identificador de aplicación (Tag 4F). Hasta 32.
              Column('Application Version Number', String(4), nullable=False),  # Versión de AID (Tag 9F09).
              Column('TAC Denial', String(10), nullable=False),  # Código de acción del terminal para denegación.
              Column('TAC On-line', String(10), nullable=False),  # Código de acción del terminal para transacción
              # on-line
              Column('TAC Default', String(10), nullable=False),  # Codigo de acción del terminal por defecto.
              Column('Default TDOL', String(60), nullable=False),  # TDOL usado si la tarjeta no lo tiene.
              Column('Default DDOL', String(60), nullable=False),  # DDOL usado si la tarjeta no lo tiene.
              Column('Transaction Currency Code', String(3), nullable=False),  # Codigo de moneda (Tag 5F2A).
              Column('Terminal Floor Limit', String(4), nullable=False),  # Tag 9F1B
              Column('Target Percent', String(2), nullable=False),  # Para gestión de riesgos del terminal.
              Column('Threshold Value', String(4), nullable=False),  # Para gestión de riesgos del terminal.
              Column('Maximum Target', String(2), nullable=False),  # Para gestión de riesgos del terminal.
              Column('Etiqueta privada', String(32), nullable=False)  # Etiqueta por defecto a usar cuando el T9F12
              # o T50 no están.
              )
        # TABLA Z: Identifica la clave de forma univoca. Este dato se envía en el PIN-BLOCK de la trama Pufdi (‘/G’),
        # para saber la clave que ha usado el terminal en la encriptación
        self.tableZZ = Table('Table_ZZ', self.metadata,
              Column('Id. Clave', String(1), nullable=False),  # Identifica la clave de forma univoca. Este dato se
              # envía en el PIN-BLOCK de la trama Pufdi (‘/G’), para saber la clave que ha usado el terminal en la
              # encriptación
              Column('Clave', String(32), nullable=False),  # Clave de encriptación.
              Column('KCV', String(6), nullable=False)  # Código de validación de la clave real (no del valor
              # encriptado que se envía en la trama de telecarga) enviados en formato hexadecimal.
              )
        # TABLA e: TABLA DE CLAVES PUBLICAS
        self.tablee = Table('Table_e', self.metadata,
              Column('RID', String(10), nullable=False),  # identificador de proveedor (Visa, Mastercard, Maestro...)
              Column('Index', String(2), nullable=False),  # Indice de la clave en hexadecimal desdoblado
              Column('Modulus Length', String(2), nullable=False),  # En hexadecimal desdoblado
              Column('Modulus', String(496), nullable=False),  # Módulo de la clave desdoblado.
              Column('Exponent', String(5), nullable=False),  # Puede valer “00002” ó “00003” ó “10001”
              # (65.537).
              Column('Hash SHA1', String(40), nullable=False),  # Verificación de la clave Sha1(
              # RID+INDEX+MODULO+EXP)
              Column('Expire Date', String(6), nullable=False)  # MMDDYY
              )
        # TABLA W: TABLA BIN/MENUS
        self.tableWW = Table('Table_WW', self.metadata,
              Column('Bin', String(6), nullable=False),  # Numero de Bin
              Column('Cod. Servicio/Operativa', String(3), nullable=False),  # Código de Servicio obtenido de la
              # pista o Operativa obtenido de la Tabla F.
              Column('Cod. Menú', String(3), nullable=False),  # Código del Menú.
              Column('Tipo Menú', String(1), nullable=False),  # Tipo de Menú. Se explica mas abajo.
              Column('Num. Productos.', String(2), nullable=False),  # Numero de productos máximos por transacción.
              Column('Accion', String(1), nullable=False)  # Alta, Baja o Modificación
              )
        # TABLA M: TABLA DE MENUS PRODUCTOS
        self.tableMM = Table('Table_MM', self.metadata,
              Column('Cod. Menú', String(3), nullable=False),  # Código del Menú obtenido en la tabla W
              Column('Cod. Producto', String(3), nullable=False),  # Código del producto para acceder a la tabla V.
              Column('Accion', String(1), nullable=False)  # Alta, Baja o Modificación
              )
        # TABLA V: TABLA DE PRODUCTOS
        self.tableVV = Table('Table_VV', self.metadata,
              Column('Cod. Producto', String(3), nullable=False),  # Código de Producto
              Column('Cod. Producto AS400', String(2), nullable=False),  # Código Producto del AS400
              Column('Descripción', String(16), nullable=False),  # Descripción del producto para el Menú y tickets
              Column('Combustible', String(1), nullable=False),  # Indica si es un combustible o no para la solicitud
              # del precio unitario.
              Column('Cod. Producto Polling', String(3), nullable=False)  # Código Producto Polling.
              )
        # TABLA t: TABLA MENU TIPO PRODUCTO DE RECARGAS
        self.tablet = Table('Table_t', self.metadata,
              Column('TPR_ID', String(2), nullable=False),  # Tipo Recarga
              Column('Descripción', String(16), nullable=False)  # Descripción de la Recarga para el Menú
              )
        # TABLA c: TABLA MENU COMPAÑIAS OPERADORAS DE RECARGA
        Table('Table_c', self.metadata,
              Column('TPR_ID', String(2), nullable=False),  # Tipo Recarga
              Column('MCRO_ID_400', String(10), nullable=False),  # Código de Compañía/operadora
              Column('Descripción', String(16), nullable=False)  # Descripción de la Compañía/operadora para el Menú
              )
        # TABLA p: TABLA PRODUCTOS DE RECARGAS
        self.tablep = Table('Table_p', self.metadata,
              Column('MCRO_ID_400', String(10), nullable=False),  # Código de Compañía/operadora
              Column('Prod_id_400', String(10), nullable=False),  # Código de Producto
              Column('Descripción', String(16), nullable=False),  # Descripción de la Compañía/operadora para el Menú
              Column('CDA', String(2), nullable=False),  # Conjunto de datos adicionales.
              Column('IMP. MAX', String(5), nullable=False),  # Importe Máximo
              Column('IMP. MIN', String(5), nullable=False),  # Importe Mínimo
              Column('Multi.', String(4), nullable=False),  # Multiplo
              Column('Tipo Oper.', String(3), nullable=False),  # Tipo Operación para recargas
              Column('Tip. Op. An.', String(3), nullable=False),  # Tipo operación para anulaciones
              Column('CDDA', String(2), nullable=False),  # Conjunto de datos adicionales anulación
              Column('Umbral Min', String(2), nullable=False),  # Umbral mínimo para stock cupones (NO SE USA
              # ACTUALMENTE)
              Column('Mensaje Cabec.', String(48), nullable=False),  # Mensaje para la cabecera del Ticket de la
              # Recarga (Usado en terminales que no reciben el ticketc entero)
              Column('Mensaje Pie', String(72), nullable=False),  # Mensaje para el pie del Ticket de la Recarga (
              # Usado en terminales que no reciben el ticketc entero)
              Column('Des. Prod. Impresora', String(24), nullable=False)  # Descripcion del producto para imprimir
              # en el ticket (Usado en terminales que no reciben el ticketc entero)
              )
        # TABLA d: TABLA DE PRODUCTOS/DATOS ADICIONALES
        self.tabled = Table('Table_d', self.metadata,
              Column('CDA_ID', String(2), nullable=False),  # Código de Operacion
              Column('ID_DATO', String(16), nullable=False)  # Código del Dato adicional
              )
        # TABLA a: TABLA MENU COMPAÑIAS OPERADORAS DE RECARGA
        self.tablea = Table('Table_a', self.metadata,
              Column('ID_DATO', String(2), nullable=False),  # Código del dato adicional recuperado de la tabla d.
              Column('Tipo_Dato', String(1), nullable=False),  # Si es por teclado o lector
              Column('Tag_Asociado', String(2), nullable=False),  # Tag que se enviara en la trama de solicitud
              Column('Literal', String(16), nullable=False),  # Literal que se mostrara en pantalla
              Column('Mascara', String(30), nullable=False)  # Mascara a aplicar para cada uno de los tipos adicionales
              )
        # TABLA f: TABLA DE CONTROL ESPECIAL_NEMOS
        self.tablef = Table('Table_f', self.metadata,
              Column('Bin', String(6), nullable=False),  # Numero de Bin. De la Tabla TV_F.
              Column('Nemo', String(10), nullable=False)  # Código del Dato adicional
              )
        # TABLA N: TABLA DE PREMIOS
        self.tableNN = Table('Table_NN', self.metadata,
              Column('Tipo Fidelidad', String(1), nullable=False),  # Letra de Premio de la Tabla TV_F
              Column('Código', String(5), nullable=False),  # Código del Regalo
              Column('Descripción', String(12), nullable=False),  # Descripción del Regalo
              Column('Puntos', String(5), nullable=False),  # Puntos del Regalo
              Column('Tipo Regalo', String(1), nullable=False),  # Detalle en la Tabla. Se usa Para el Ticket, si se
              # tiene que sacar o no, etc.
              Column('Acción', String(1), nullable=False)  #
              )
        # TABLA R: TABLA COMISIONES
        Table('Table_RR', self.metadata,
              Column('CLA_ID', String(3), nullable=False),  # Clase tarjeta
              Column('Descripción', String(16), nullable=False)  # Descripción a mostrar en pantalla.
              )
        # TABLA B: TABLA BINES PREPAGO
        Table('Table_BB', self.metadata,
              Column('BIN Venta', String(6), nullable=False),  # Valor del BIN de la operación de Venta
              Column('BIN', String(10), nullable=False)  # Valor del BIN de Prepago.
              )
        # TABLA n: TABLA DE APLICACIONES SIN CONTACTOS
        self.tablen = Table('Table_n', self.metadata,
              Column('AID', String(32), nullable=False),  # identificador de aplicación (Tag 4F). Hasta 32.
              Column('Application Version Number', String(4), nullable=False),  # Versión de AID (Tag 9F09).
              Column('Application Version PayPass Number', String(4), nullable=False),  # Application Version Number
              # PayPass – Mag Stripe (tag "9F6D")
              Column('TAC Denial', String(10), nullable=False),  # Código de acción del terminal para denegación.
              Column('TAC On-line', String(10), nullable=False),  # Código de acción del terminal para transacción
              # on-line
              Column('TAC Default', String(10), nullable=False),  # Codigo de acción del terminal por defecto.
              Column('Default TDOL', String(60), nullable=False),  # TDOL usado si la tarjeta no lo tiene.
              Column('Default DDOL', String(60), nullable=False),  # DDOL usado si la tarjeta no lo tiene.
              Column('Default UDOL', String(60), nullable=False),  # UDOL usado si la tarjeta no lo tiene.
              Column('TCC', String(3), nullable=False),  # Codigo de moneda (Tag 5F2A).
              Column('Terminal Floor Limit', String(6), nullable=False),  # Tag 9F1B
              Column('Target Percent', String(2), nullable=False),  # Para gestión de riesgos del terminal.
              Column('Threshold Value', String(4), nullable=False),  # Para gestión de riesgos del terminal.
              Column('Maximum Target', String(2), nullable=False),  # Para gestión de riesgos del terminal.
              Column('Private CLess App Label', String(40), nullable=False),  # Etiqueta por defecto a usar cuando el T9F12
              Column('AFF', String(1), nullable=False),  # Allowed Form Factors
              Column('Cless Floor Limit', String(6), nullable=False),  # Reader Contactless Floor Limit
              Column('Cless Transaction Limit', String(6), nullable=False),  # Reader Contactless Transaction Limit
              Column('Cless Transaction Limit on CVM', String(6), nullable=False),  # Reader Contactless Transaction
                            # Limit on-device CVM
              Column('CVM required Limit', String(6), nullable=False),  # Reader CVM Required Limit
              Column('Cless Floor Limit - VISA program', String(6), nullable=False),  # Reader Contactless Floor Limit
                            # - VISA program
              Column('Cless Transaction Limit - VISA program', String(6), nullable=False),  # Reader
                            # Contactless Transaction Limit - VISA program
              Column('CVM Required Limit - VISA program', String(6), nullable=False),  # Reader CVM Required Limit
                            # - VISA program
              Column('Cless products processing options', String(2), nullable=False),  # Contact-less products
                            # processing options
              Column('VISA program ID.', String(32), nullable=False),  # VISA program ID.
              Column('Terminal Type', String(2), nullable=False),  # Terminal Type.
              Column('Terminal Capabilities for cardholder verification', String(6), nullable=False),  # Terminal
                            # Capabilities for cardholder verification
              Column('Terminal Capabilities for no cardholder verification', String(6), nullable=False),  # Terminal
                            # Capabilities for no cardholder verification
              Column('Additional Terminal Capabilities', String(10), nullable=False),  # Additional Terminal Capabilities
              Column('Merchant Category Code', String(4), nullable=False),  # Merchant Category Code
              Column('TTQ', String(8), nullable=False)  # TTQ
              )
        # TABLA I: TABLA BINES INCOMPATIBLES
        Table('Table_II', self.metadata,
              Column('BIN 1', String(6), nullable=False),  # Valor del BIN 1 (numérico o “X”)
              Column('BIN 2', String(6), nullable=False)  # Valor del BIN 2 (numérico o “X”)
              )
        # TABLA i: TABLA BINES MULTI-APP
        Table('Table_i', self.metadata,
              Column('BIN', String(9), nullable=False),  # BIN que debe ser procesado por H24 (capturar evento de
              # tarjeta para su procesado si coincide BIN y longitud de PAN)
              Column('Longitud del PAN', String(2), nullable=False)  # Longitud del PAN a coincidir junto con el BIN
              # para que H24 procese la tarjeta
              )
        # TABLA g: TABLA DE GRUPOS DE RECONCILIACION
        self.tableg = Table('Table_g', self.metadata,
              Column('AUT_ID', String(15), nullable=False),  # Identificador unívoco del autorizador.
              Column('AUT_NEMO', String(24), nullable=False)  # Etiqueta asociada al autorizador
              # Column('AUT_ROUTE', String(10), nullable=False)  # Identificador de enrutamiento.
              )
        # TABLA b: TABLA DE CAPACIDADES OFFLINE
        Table('Table_b', self.metadata,
              Column('Bin', String(19), nullable=False),  # Numero de Bin
              Column('Importe máximo offline', String(10), nullable=False),  # Maximo importe permitido en operativa
              # offline.
              Column('Acción', String(1), nullable=False)  # Alta, Baja ó Modificación
              )
        # TABLA j: TABLA DE TOKETIZACIÓN DE TARJETAS NO ESTANDARD - TARJETIZACIÓN
        Table('Table_j', self.metadata,
              Column('Format', String(3), nullable=False),  # Record format identifier
              Column('Entry method', String(8), nullable=False),  # String of entry methods affected by the rule
              # described in a record.
              Column('Index', String(2), nullable=False),  # Defines the order of evaluation of tokenization rules
              # when there are more than one rule affecting an entry method.
              Column('Detection rule', String(256), nullable=False),  # Standard regular expression (regex) used to
              # determine if the data supplied as payment method is affected by current rule.
              Column('Action', String(8), nullable=False),  # Action to be performed for non-standard payment data
              # matching current rule.
              Column('Extraction rule', String(128), nullable=False),  # Standard scanf-compliant expression used to
              # collect information from the non-standard payment data received.
              Column('Generation list', String(128), nullable=False)  # This field includes a list of
              # standard-payment data built using the information collected using the extraction rule above.
              )
        self.metadata.create_all(self.engine)

    def connectEngine(self):
        try:
            connection = self.engine.connect()
        except exc.InternalError as Error:
            raise Error
        return connection

    @staticmethod
    def disconnectEngine(connection_handler):
        connection_handler.close()

    def dropdb(self):
        try:
            self.metadata.drop_all(bind=self.engine)
        except exc.SQLAlchemyError as Error:
            return False, Error.__str__()
        return True, "All tables have been dropped successfully"

    def loadfile(self, pathfile):
        file = open(pathfile, 'r')
        isfirstframe = True
        self.logging.info("Starting to parse the tables...")
        for line in file:
            # Remove line break
            line = line.replace('\n', '')
            error, data = self.__parsetable(line, isfirstframe)
            isfirstframe = False
            if error:
                self.logging.info("Error in the table: {0}".format(data))
        self.logging.info("Finished to parse the tables!")

    def __parsetable(self, tableline, isfirstframe):
        status, data = self.__getheader(tableline)
        if not status:
            return status, data
        if data[:1] != "M" and data[:1] != "L":
            self.logging.error("Error in header {0}".format(data[:1]))
            return False, "Error in frame {0}".format(data[:1])
        if isfirstframe:
            data = data[29:]
        else:
            data = data[1:]
        # Table Z:
        if data[0] == "E":
            tableversion = data[1:4]
            status, data = self.__Table_EE(data[4:])
            if not status:
                return status, data
        elif data[0] == "Z":
            tableversion = data[1:4]
            status, data = self.__Table_ZZ(data[4:])
            if not status:
                return status, data
        elif data[0] == "F":
            tableversion = data[1:4]
            status, data = self.__Table_FF(data[4:])
            if not status:
                return status, data
        elif data[0] == "W":
            tableversion = data[1:4]
            status, data = self.__Table_WW(data[4:])
            if not status:
                return status, data
        elif data[0] == "M":
            tableversion = data[1:4]
            status, data = self.__Table_MM(data[4:])
            if not status:
                return status, data
        elif data[0] == "V":
            tableversion = data[1:4]
            status, data = self.__Table_VV(data[4:])
            if not status:
                return status, data
        elif data[0] == "e":
            tableversion = data[1:4]
            status, data = self.__Table_e(data[4:])
            if not status:
                return status, data
        elif data[0] == "t":
            tableversion = data[1:4]
            status, data = self.__Table_t(data[4:])
            if not status:
                return status, data
        elif data[0] == "v":
            tableversion = data[1:4]
            status, data = self.__Table_v(data[4:])
            if not status:
                return status, data
        elif data[0] == "p":
            tableversion = data[1:4]
            status, data = self.__Table_p(data[4:])
            if not status:
                return status, data
        elif data[0] == "d":
            tableversion = data[1:4]
            status, data = self.__Table_d(data[4:])
            if not status:
                return status, data
        elif data[0] == "x":
            tableversion = data[1:4]
            status, data = self.__Table_x(data[4:])
            if not status:
                return status, data
        elif data[0] == "a":
            tableversion = data[1:4]
            status, data = self.__Table_a(data[4:])
            if not status:
                return status, data
        elif data[0] == "N":
            tableversion = data[1:4]
            status, data = self.__Table_NN(data[4:])
            if not status:
                return status, data
        elif data[0] == "f":
            tableversion = data[1:4]
            status, data = self.__Table_f(data[4:])
            if not status:
                return status, data
        elif data[0] == "b":
            tableversion = data[1:4]
            status, data = self.__Table_b(data[4:])
            if not status:
                return status, data
        elif data[0] == "B":
            tableversion = data[1:4]
            status, data = self.__Table_BB(data[4:])
            if not status:
                return status, data
        elif data[0] == "c":
            tableversion = data[1:4]
            status, data = self.__Table_c(data[4:])
            if not status:
                return status, data
        elif data[0] == "g":
            tableversion = data[1:4]
            status, data = self.__Table_g(data[4:])
            if not status:
                return status, data
        elif data[0] == "i":
            tableversion = data[1:4]
            status, data = self.__Table_i(data[4:])
            if not status:
                return status, data
        elif data[0] == "I":
            tableversion = data[1:4]
            status, data = self.__Table_II(data[4:])
            if not status:
                return status, data
        elif data[0] == "j":
            tableversion = data[1:4]
            status, data = self.__Table_j(data[4:])
            if not status:
                return status, data
        elif data[0] == "m":
            tableversion = data[1:4]
            status, data = self.__Table_m(data[4:])
            if not status:
                return status, data
        elif data[0] == "n":
            tableversion = data[1:4]
            status, data = self.__Table_n(data[4:])
            if not status:
                return status, data
        elif data[0] == "R":
            tableversion = data[1:4]
            status, data = self.__Table_RR(data[4:])
            if not status:
                return status, data
        elif data[0] == "u":
            tableversion = data[1:4]
            status, data = self.__Table_u(data[4:])
            if not status:
                return status, data
        return False, ""


    def __getheader(self, tableline):
        header = tableline[:30]
        if header[:4] != "PH24":
            self.logging.error("Error in header {0}".format(header[:4]))
            return False, "Error in header {0}".format(header[:4])
        # Longitud de la trama.
        if not header[4:9].isdigit():
            self.logging.error("Error in header {0}".format(header[4:9]))
            return False, "Error in header {0}".format(header[4:9])
        self.framelength = int(header[4:9])
        # Identificador de mensaje "PTD" para telecargas.
        if header[9:12] != "PTD":
            self.logging.error("Error in header {0}".format(header[9:12]))
            return False, "Error in header {0}".format(header[9:12])
        # Version del protocolo, 0300 para telecargas.
        if header[12:16] != "0300":
            self.logging.error("Error in header {0}".format(header[12:16]))
            return False, "Error in header {0}".format(header[12:16])
        # Identificador del mensaje.
        if not header[16:27].isdigit():
            self.logging.error("Error in header {0}".format(header[16:27]))
            return False, "Error in header {0}".format(header[16:27])
        # Error Code, no usado, siempre "000".
        if header[27:] != "000":
            self.logging.error("Error in header {0}".format(header[27:]))
            return False, "Error in header {0}".format(header[27:])
        return True, tableline[30:]

    def __Table_EE(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_json = {}
        table_column_names = self.tableEE.columns.keys()
        for index in range(len(table_column_names)):
            table_json[table_column_names[index]] = field_list[index]
        connection.execute(self.tableEE.insert(), table_json)
        self.disconnectEngine(connection)
        error_msg = "Table E parsed OK"
        return True, error_msg

    def __Table_ZZ(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0], field[1:33], field[33:]]
            table_column_names = self.tableZZ.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableZZ.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table Z parsed OK"
        return True, error_msg

    def __Table_FF(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0:5], field[6], field[7], field[8], field[9],
                     field[10], field[11], field[12], field[13], field[14],
                     field[15], field[16], field[17], field[18]]
            table_column_names = self.tableFF.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableFF.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table F parsed OK"
        return True, error_msg

    def __Table_WW(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0:5], field[6:8], field[9:11], field[12], field[13:14], field[15]]
            table_column_names = self.tableWW.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableWW.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table W parsed OK"
        return True, error_msg

    def __Table_MM(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0:3], field[3:6], field[6]]
            table_column_names = self.tableMM.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableMM.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table M parsed OK"
        return True, error_msg

    def __Table_VV(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0:3], field[3:5], field[5:21], field[21], field[22:25]]
            table_column_names = self.tableVV.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableVV.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table V parsed OK"
        return True, error_msg

    def __Table_e(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            elif field[0] == '9':
                continue
            clave = [field[0:10], field[10:12], field[12:14], field[14:-51], field[-51:-46],
                     field[-46:-6], field[-6:]]
            table_column_names = self.tablee.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablee.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table e parsed OK"
        return True, error_msg

    def __Table_t(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            elif field[0] == '9':
                continue
            clave = [field[0:2], field[3:18]]
            table_column_names = self.tablet.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablet.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table t parsed OK"
        return True, error_msg

    def __Table_v(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            elif field[0] == '9':
                continue
            subfield = field.split('#')
            clave = [subfield[0],
                     subfield[1][0:4], subfield[1][4:14], subfield[1][14:24], subfield[1][24:34], subfield[1][34:],
                     subfield[2],
                     subfield[3][:3], subfield[3][3:7], subfield[3][7:9], subfield[3][9:13], subfield[3][13:15],
                     subfield[3][15:]]
            table_column_names = self.tablev.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablev.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table v parsed OK"
        return True, error_msg

    def __Table_p(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = field.split('#')
            table_column_names = self.tablep.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablep.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table p parsed OK"
        return True, error_msg

    def __Table_d(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = field.split('#')
            table_column_names = self.tabled.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tabled.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table d parsed OK"
        return True, error_msg

    def __Table_x(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0], field[1:]]
            table_column_names = self.tablex.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablex.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table x parsed OK"
        return True, error_msg

    def __Table_a(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = field.split('#')
            table_column_names = self.tablea.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablea.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table a parsed OK"
        return True, error_msg

    def __Table_NN(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[0], field[1:6], field[6:18], field[18:23], field[23], field[24]]
            table_column_names = self.tableNN.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableNN.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table N parsed OK"
        return True, error_msg

    def __Table_f(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = [field[:6], field[6:]]
            table_column_names = self.tablef.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablef.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table f parsed OK"
        return True, error_msg

    def __Table_b(self, table_record):
        error_msg = "Table b parsed OK"
        return True, error_msg

    def __Table_BB(self, table_record):
        error_msg = "Table B parsed OK"
        return True, error_msg

    def __Table_c(self, table_record):
        error_msg = "Table c parsed OK"
        return True, error_msg

    def __Table_g(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            clave = field.split('\x1F')
            table_column_names = self.tableg.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tableg.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table g parsed OK"
        return True, error_msg

    def __Table_i(self, table_record):
        error_msg = "Table i parsed OK"
        return True, error_msg

    def __Table_II(self, table_record):
        error_msg = "Table I parsed OK"
        return True, error_msg

    def __Table_j(self, table_record):
        error_msg = "Table j parsed OK"
        return True, error_msg

    def __Table_m(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_json = {}
        if not field[0]:
            return False, "Error en contenido Table m."
        clave = [field[0][0:2], field[0][2:8], field[0][8:18], field[0][18:20],
                 field[0][20:24], field[0][24:27], field[0][27:]]
        table_column_names = self.tablem.columns.keys()
        for index in range(len(table_column_names)):
            table_json[table_column_names[index]] = clave[index]
        connection.execute(self.tablem.insert(), table_json)
        self.disconnectEngine(connection)
        error_msg = "Table m parsed OK"
        return True, error_msg

    def __Table_n(self, table_record):
        if table_record[0] != "#":
            error_msg = "Error in Frame {0}".format(table_record[0])
            self.logging.error(error_msg)
            return False, error_msg
        connection = self.connectEngine()
        # Remove last chars of string (DC2 and EM)
        table_record = table_record[1:-2]
        # Split the record by DC1
        field_list = table_record.split('\x11')
        # Build the JSON to insert the data into DB
        table_list = []
        for field in field_list:
            # Build the JSON to insert the data into DB
            table_json = {}
            if not field:
                break
            elif field[0] == '9':
                continue
            subfield = field.split('#')
            clave = [subfield[0],
                     subfield[1][0:4], subfield[1][4:8], subfield[1][8:18], subfield[1][18:28], subfield[1][28:38],
                     subfield[1][38:],
                     subfield[2],
                     subfield[3][:6], subfield[3][6:9], subfield[3][9:15], subfield[3][15:17], subfield[3][17:23],
                     subfield[3][23:25], subfield[3][25:],
                     subfield[4][0], subfield[4][1:7], subfield[4][7:13], subfield[4][13:19], subfield[4][19:25],
                     subfield[4][25:31], subfield[4][31:37], subfield[4][37:43], subfield[4][43:45], subfield[4][45:],
                     subfield[5][0:2], subfield[5][2:8], subfield[5][8:14], subfield[5][14:24], subfield[5][24:28],
                     subfield[5][28:38], subfield[5][38:]]
            table_column_names = self.tablen.columns.keys()
            for index in range(len(table_column_names)):
                table_json[table_column_names[index]] = clave[index]
            table_list.append(table_json)
        connection.execute(self.tablen.insert(), table_list)
        self.disconnectEngine(connection)
        error_msg = "Table n parsed OK"
        return True, error_msg

    def __Table_RR(self, table_record):
        error_msg = "Table R parsed OK"
        return True, error_msg

    def __Table_u(self, table_record):
        error_msg = "Table u parsed OK"
        return True, error_msg

    def __obtain(self, table_name, table_id):
        try:
            table_object = self.metadata.tables[table_name]
        except KeyError:
            return "Table required %s not found" % table_name

        query = select([table_object]).where(table_object.columns.Prod_id_400 == table_id)
        connection = self.connectEngine()
        result = connection.execute(query).fetchall()
        self.disconnectEngine(connection)
        if len(result) == 0:
            return ''
        row_dict = dict(result[0].items())
        return row_dict

    def obtain_table_p(self, type):
        try:
            table_object = self.metadata.tables['Table_p']
        except KeyError:
            return "Table required %s not found" % 'Table_p'

        query = select([table_object]).where(table_object.columns.Prod_id_400 == type)
        connection = self.connectEngine()
        result = connection.execute(query).fetchall()
        self.disconnectEngine(connection)
        if len(result) == 0:
            return ''
        row_dict = dict(result[0].items())
        return row_dict

