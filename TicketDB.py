from sqlalchemy import *
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey, exc, desc
from sqlalchemy.orm import relationship


class TDataBase:
    def __init__(self, logging_handler, drop_database=True):
        self.logging = logging_handler
        self.engine = None
        self.metadata = None
        self.tableEE = None
        self.engine_name = 'sqlite:///ticket.db'

        self.init_engine()
        if drop_database:
            self.dropdb()
            self.init_engine()

    def init_engine(self):
        self.engine = create_engine(self.engine_name, echo=False)
        self.metadata = MetaData(bind=None)

        # TABLA E: TABLA DE DATOS DE ESTACIÓN
        self.templates = Table('Templates', self.metadata,
                               Column('Name', String(5), nullable=False),  # Identificador bloque
                               Column('Bloques', String(50), nullable=False)  # CIF de la estación de servicio
                               )
        self.bloques = Table('Bloques', self.metadata,
                             Column('Name', String(10), nullable=False),  # Identificador bloque
                             Column('Contenido', String(100), nullable=False),  # CIF de la estación de servicio
                             Column('Placeholder', String(100), nullable=False)  # CIF de la estación de servicio
                             )
        self.operaciones = Table('Operaciones', self.metadata,
                                 Column('Date', String(10), nullable=False),  # Fecha operación
                                 Column('Time', String(10), nullable=False),  # Hora operación
                                 Column('Result', String(3), nullable=False),  # Resultado operación
                                 Column('Op_Type', String(3), nullable=False),  # Tipo de operación
                                 Column('Num_Op', String(3), nullable=False),  # Número de operación
                                 Column('Entry_Mode', String(3), nullable=False),  # Modo de entrada de la operación
                                 Column('Importe', String(6), nullable=False),  # Importe de la operación
                                 Column('Ticket', String(200), nullable=False)  # Tiquet de la operación
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
        for line in file:
            if line[0] == '-':
                self.__parsetemplate(line[1:])
            elif line[0] == '#':
                self.__parsebloque(line[1:])
            else:
                pass

    def __parsetemplate(self, line):
        field_list = line.split('=')
        connection = self.connectEngine()
        table_json = {}
        table_column_names = self.templates.columns.keys()
        table_json[table_column_names[0]] = field_list[0]
        table_json[table_column_names[1]] = field_list[1]
        connection.execute(self.templates.insert(), table_json)
        self.disconnectEngine(connection)

    def __parsebloque(self, line):
        field_list = line.split('=')
        connection = self.connectEngine()
        table_json = {}
        table_column_names = self.bloques.columns.keys()
        table_json[table_column_names[0]] = field_list[0]
        table_json[table_column_names[1]] = field_list[1]
        if len(field_list) > 2:
            table_json[table_column_names[2]] = field_list[2]
        else:
            table_json[table_column_names[2]] = ""
        connection.execute(self.bloques.insert(), table_json)
        self.disconnectEngine(connection)

    def obtain_template(self, type):
        return self.__obtain('Templates', type)

    def obtain_bloque(self, type):
        return self.__obtain('Bloques', type)

    def registry_operation(self, data):
        return self.__registry('Operaciones', data)

    def __obtain(self, table_name, table_id):
        try:
            table_object = self.metadata.tables[table_name]
        except KeyError:
            return "Table required %s not found" % table_name

        query = select([table_object]).where(table_object.columns.Name == table_id)
        connection = self.connectEngine()
        result = connection.execute(query).fetchall()
        self.disconnectEngine(connection)
        if len(result) == 0:
            return ''
        row_dict = dict(result[0].items())
        return row_dict

    def __registry(self, table_name, data):
        try:
            table_object = self.metadata.tables[table_name]
        except KeyError:
            return "Table required %s not found" % table_name
        connection = self.connectEngine()
        result = connection.execute(table_object.insert(), data)
        self.disconnectEngine(connection)
