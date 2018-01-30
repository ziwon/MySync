import re
from schemaobject.collections import OrderedDict
from schemaobject.option import SchemaOption

REGEX_MULTI_SPACE = re.compile('\s\s+')


def ProcedureSchemaBuilder(database):
    """
    Returns a dictionary loaded with all of the tables available in the database.
    ``database`` must be an instance of DatabaseSchema.

    .. note::
      This function is automatically called for you and set to
      ``scheme.databases[name].procedures`` when you create an instance of SchemaObject
    """
    conn = database.parent.connection

    sp = OrderedDict()
    sql = """
       SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT,
       SECURITY_TYPE, SQL_MODE,
       CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION
       FROM information_schema.`ROUTINES`
       WHERE ROUTINE_SCHEMA = '%s'
       AND ROUTINE_TYPE ='procedure'
    """
    procedures = conn.execute(sql % database.name)

    if not procedures:
        return sp

    for procedure_info in procedures:
        name = procedure_info['ROUTINE_NAME']

        if "COLLATION_CONNECTION" not in procedure_info:
            charset = None

        pos = procedure_info['COLLATION_CONNECTION'].find('_')

        if not pos:
            charset = procedure_info['COLLATION_CONNECTION']
        else:
            charset = procedure_info['COLLATION_CONNECTION'][:pos]

        procedure = ProcedureSchema(name=name, parent=database)
        procedure.options['definition'] = SchemaOption('DEFINITION', procedure_info['ROUTINE_DEFINITION'])
        procedure.options['charset'] = SchemaOption('COLLATE', charset)
        procedure.options['comment'] = SchemaOption('COMMENT', procedure_info['ROUTINE_COMMENT'])
        sp[name] = procedure

    return sp


class ProcedureSchema(object):

    def __init__(self, name, parent):
        self.parent = parent
        self.name = name
        self._options = None

    @property
    def options(self):
        if self._options == None:
            self._options = OrderedDict()
        return self._options

    def create(self):
        cursor = self.parent.parent.connection
        result = cursor.execute("SHOW CREATE PROCEDURE `%s`.`%s`" % (self.parent.name, self.name))
        sql = result[0]['Create Procedure'] + ';' if result[0]['Create Procedure'] else ''
        sql = sql.replace('\n', '')
        return REGEX_MULTI_SPACE.sub(' ', sql)

    def drop(self):
        return "DROP PROCEDURE `%s`" % (self.name)

    def __eq__(self, other):
        if not isinstance(other, ProcedureSchema):
            return False

        return ((self.options['comment'] == other.options['comment'])
                and (self.options['definition'] == other.options['definition'])
                and (self.name == other.name))

    def __ne__(self, other):
        return not self.__eq__(other)
