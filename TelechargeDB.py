from sqlalchemy import *
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey, exc, desc
from sqlalchemy.orm import relationship


class DataBase:

    def __init__(self):
        engine_name = 'sqlite:///telecharge.db'
        self.engine = create_engine(engine_name, echo=True)
        self.metadata = MetaData(bind=None)
        # TABLA E: TABLA DE DATOS DE ESTACIÓN
        Table('Table_E', self.metadata,
              Column('EESS', String(7), nullable=False),   # Número de estación H24.
              Column('CIF', String(10), nullable=False),   # CIF de la estación de servicio
              Column('NOMB', String(24), nullable=False),  # Nombre de la estación de servicio
              Column('Provincia', String(14), nullable=False),   # Provincia
              Column('DLG', String(2), nullable=False),  # Delegación del TPV
              Column('HH', String(4), nullable=False),  # Hora Comunicación
              Column('CIE', String(1), nullable=False),  # Tiene Cierre 4B (Valores 0, 1)
              Column('CEN', String(1), nullable=False),  # Tiene centralita (Valores 0, 1)
              Column('PASS', String(5), nullable=False),  # Password de Mantenimiento
              Column('CHQO', String(2), nullable=False),  # Cheques off/cupones sin comunicar
              Column('CHQA', String(1), nullable=False),  # Tipos de cheques activos
              Column('LANG', String(2), nullable=False),  # Idioma del TPV
              Column('CURR', String(3), nullable=False)   # Divisa del TPV
              )
        # TABLA u: TABLA PARAMETROS DE COMUNICACIÓN
        Table('Table_u', self.metadata,
              Column('NEMO_ACCESO', String(3), nullable=False),   # Tag tipo de acceso permitido
              Column('NEMO_VALOR', String(3), nullable=False),   # Tag indicativo del parámetro
              Column('VALOR', String(50), nullable=False)  # Valor del parámetro
              )
        # TABLA F: TABLA DE BINES
        Table('Table_F', self.metadata,
              Column('Bin', String(6), nullable=False),   # Numero de Bin
              Column('Operativa', String(1), nullable=False),   # Operativa para el despliegue del menu
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
        Table('Table_x', self.metadata,
              Column('Lognitud BIN', String(1), nullable=False),   # Longitud del BIN
              Column('BIN', String(6), nullable=False)   # BIN sobre el cual se tiene que hacer el control del código
              # de servicio.
              )
        # TABLA m: TABLA DE PARAMETROS GENERALES EMV
        Table('Table_m', self.metadata,
              Column('Terminal Type', String(2), nullable=False),   # Tag 9F35: según ICS
              Column('Terminal Capabilities', String(6), nullable=False),  # Tag 9F33: según ICS
              Column('Additional Terminal Capabilities', String(10), nullable=False),  # Tag 9F40 según ICS
              Column('Transaction Category Code', String(2), nullable=False),  # Tag 9F53
              Column('Merchant Category Code', String(4), nullable=False),  # Tag 9F15
              Column('Terminal Country Code', String(3), nullable=False),  # Tag 9F1A
              Column('Flags EMV', String(2), nullable=False)   # Ver tabla siguiente ¿?
              )
        # TABLA v: TABLA DE APLICACIONES EMV (AIDs)
        Table('Table_v', self.metadata,
              Column('AID', String(32), nullable=False),   # identificador de aplicación (Tag 4F). Hasta 32.
              Column('Separador', String(1), nullable=False),   # "/#" (Hex. 23).
              Column('Application Version Number', String(4), nullable=False),   # Versión de AID (Tag 9F09).
              Column('TAC Denial', String(10), nullable=False),   # Código de acción del terminal para denegación.
              Column('TAC On-line', String(10), nullable=False),   # Código de acción del terminal para transacción
              # on-line
              Column('TAC Default', String(10), nullable=False),   # Codigo de acción del terminal por defecto.
              Column('Default TDOL', String(60), nullable=False),   # TDOL usado si la tarjeta no lo tiene.
              Column('Separador', String(1), nullable=False),   # "/#" (Hex. 23)
              Column('Default DDOL', String(60), nullable=False),   # DDOL usado si la tarjeta no lo tiene.
              Column('Separador', String(1), nullable=False),   # "/#" (Hex. 23)
              Column('Transaction Currency Code', String(3), nullable=False),   # Codigo de moneda (Tag 5F2A).
              Column('Terminal Floor Limit', String(4), nullable=False),   # Tag 9F1B
              Column('Target Percent', String(2), nullable=False),   # Para gestión de riesgos del terminal.
              Column('Threshold Value', String(4), nullable=False),   # Para gestión de riesgos del terminal.
              Column('Maximum Target', String(2), nullable=False),   # Para gestión de riesgos del terminal.
              Column('Etiqueta privada', String(32), nullable=False)    # Etiqueta por defecto a usar cuando el T9F12
              # o T50 no están.
              )
        # TABLA Z: Identifica la clave de forma univoca. Este dato se envía en el PIN-BLOCK de la trama Pufdi (‘/G’),
        # para saber la clave que ha usado el terminal en la encriptación
        Table('Table_Z', self.metadata,
              Column('Id. Clave', String(1), nullable=False),   # Identifica la clave de forma univoca. Este dato se
              # envía en el PIN-BLOCK de la trama Pufdi (‘/G’), para saber la clave que ha usado el terminal en la
              # encriptación
              Column('Clave', String(32), nullable=False),  # Clave de encriptación.
              Column('KCV', String(6), nullable=False)   # Código de validación de la clave real (no del valor
              # encriptado que se envía en la trama de telecarga) enviados en formato hexadecimal.
              )
        # TABLA e: TABLA DE CLAVES PUBLICAS
        Table('Table_e', self.metadata,
              Column('RID', String(10), nullable=False),   # identificador de proveedor (Visa, Mastercard, Maestro...)
              Column('Index', String(2), nullable=False),  # Indice de la clave en hexadecimal desdoblado
              Column('Modulus Length', String(2), nullable=False),  # En hexadecimal desdoblado
              Column('Modulus', String(496), nullable=False),  # Módulo de la clave desdoblado.
              Column('Exponent', String(5), nullable=False),  # Puede valer “00002” ó “00003” ó “10001”
              # (65.537).
              Column('Hash SHA1', String(40), nullable=False),  # Verificación de la clave Sha1(
              # RID+INDEX+MODULO+EXP)
              Column('Expire Date', String(6), nullable=False)   # MMDDYY
              )
        # TABLA W: TABLA BIN/MENUS
        Table('Table_W', self.metadata,
              Column('Bin', String(6), nullable=False),   # Numero de Bin
              Column('Cod. Servicio/Operativa', String(3), nullable=False),  # Código de Servicio obtenido de la
              # pista o Operativa obtenido de la Tabla F.
              Column('Cod. Menú', String(3), nullable=False),  # Código del Menú.
              Column('Tipo Menú', String(1), nullable=False),  # Tipo de Menú. Se explica mas abajo.
              Column('Num. Productos.', String(2), nullable=False),  # Numero de productos máximos por transacción.
              Column('Accion', String(1), nullable=False)   # Alta, Baja o Modificación
              )
        # TABLA M: TABLA DE MENUS PRODUCTOS
        Table('Table_M', self.metadata,
              Column('Cod. Menú', String(3), nullable=False),   # Código del Menú obtenido en la tabla W
              Column('Cod. Producto', String(3), nullable=False),  # Código del producto para acceder a la tabla V.
              Column('Accion', String(1), nullable=False)   # Alta, Baja o Modificación
              )
        self.metadata.create_all(self.engine)
