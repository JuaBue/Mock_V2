from sqlalchemy import *
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey, exc, desc
from sqlalchemy.orm import relationship


class DataBase:

    def __init__(self):
        engine_name = 'sqlite:///telecharge.db'
        self.engine = create_engine(engine_name, echo=True)
        self.metadata = MetaData(bind=None)
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
        Table('Table_u', self.metadata,
              Column('NEMO_ACCESO', String(3), nullable=False),   # Tag tipo de acceso permitido
              Column('NEMO_VALOR', String(3), nullable=False),   # Tag indicativo del parámetro
              Column('VALOR', String(50), nullable=False)  # Valor del parámetro
              )
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
        Table('Table_x', self.metadata,
              Column('Lognitud BIN', String(1), nullable=False),   # Longitud del BIN
              Column('BIN', String(6), nullable=False)   # BIN sobre el cual se tiene que hacer el control del código
              # de servicio.
              )
        self.metadata.create_all(self.engine)
