from __future__ import annotations
import copy
import warnings

class Schema(object):

    default_name:str = 'col{}'

    @staticmethod
    def gen_field_name(field_pos:int):
        return Schema.default_name.format(field_pos)

    def __init__(self
        , name:str):
        self._name = name
        self._schema = []
        self._pks = []
        self._alias = {}
        self.gen_field_name = self._instance_gen_field_name

    def __str__(self):
        return self.to_str()

    class _Field(object):
        """
        Class to represent the 
        caracteristics of a field.
        """
        def __init__(self
            , name:str
            , ftype=str
            , size:int=50
            , default=None
            , auto_increment:int=0
            , optional:bool=False):
            self._f = {'size':size
                , 'default': ftype(default)
                , 'auto_increment':auto_increment
                , 'optional':optional}
            self.set_name(name)
            self.set_type(ftype)

        def set_type(self, ftype):
            """
            Define o tipo do campo. Necessita
            receber tipos especificos caso
            contrario retorna erro.
            """
            if ftype not in (str, int, float, bool):
                raise Exception("Invalid type.")
            self._f['ftype'] = ftype
            return self

        def set_schema(self
            , schema:Schema):
            self._schema = schema

        def pk(self):
            """
            Add the field in the primary key 
            list of schema.
            """
            self._schema.set_primary(self)
            return self

        def set_name(self, name:str):
            self._f['name'] = name
            return self

        # TODO: caso o nome for nulo, fazer com que retorne um nome padrao
        def get_name(self, use_alias=True):
            """
            Gets the field name. If field has 
            an alias, return the alias.
            """
            if use_alias and len(self._schema._alias)>0:
                alias, fields = self._schema._alias.items()
                if self in fields:
                    return self._alias[fields.index(self)]
            # if not self._f['name']:
            #     return schema._get_default_name()
            return self._f['name']

    def to_str(self)->str:
        fields = self.get_names()
        return "{" + ','.join(fields) + "}"

    def len(self):
        """
        Informa a quantidade de colunas
        existentes no schema. Basicamente
        conta a quantidade de posicoes da
        lista onde os campos sao definidos.
        """
        return len(self._schema)

    def to_sql(self
        , tablename:str
        , cmd_prefix=''
        , cmd_sufix=''
        , max_limit=400
        , min_limit=6
        , exclude_zero_lenght=True)->str:
        return self.sql_create_table(tablename=tablename
            , cmd_prefix=cmd_prefix
            , cmd_sufix=cmd_sufix
            , max_limit=max_limit
            , min_limit=min_limit
            , exclude_zero_lenght=exclude_zero_lenght)

    def sql_create_table(self
        , cmd_prefix=''
        , cmd_sufix=''
        , max_limit=400
        , min_limit=6
        , exclude_zero_lenght=True)->str:
        """
        Cria comando SQL para create table.
        """
        def sql_cast_type(ftype):
            sql_type=''
            if ftype is str:
                sql_type = 'VARCHAR'
            elif ftype is int:
                sql_type = 'INT'
            else:
                raise Exception("Unknow type.")
            return sql_type
            

        def sql_field_schema(field:self._Field
            , max_limit:int
            , min_limit:int)->str:
            """
            Cria a parte de definicao dos campos.
            """
            ftype = sql_cast_type(field._f.get('ftype', str))
            sql = "[{}] {}".format(field.get_name(), ftype)
            if ftype!='DATETIME':
                size = field._f.get('size', 0)
                primary_key = field in self._pks #._f.get('primary_key', False)
                auto_increment = field._f.get('auto_increment', 0)
                if not auto_increment:
                    # if size<=0:
                    if size<min_limit:
                        size = min_limit
                    sql += "({})".format('MAX' if size>max_limit else str(size))
                if primary_key:
                    sql += ' PRIMARY KEY'
                if auto_increment:
                    sql += ' IDENTITY(1,{})'.format(auto_increment)
            if not field._f.get('optional', True):
                sql += ' NOT NULL'
            return sql
            
        field_list = []
        for f in self._schema:
            size = f._f.get('size', 0)
            ftype = sql_cast_type(f._f.get('ftype', str))
            # field_schema = _default_schema(field_schema)
            # ser for para excluir campos que tem tamanho 0
            # e o tipo deste campo nao for DATETIME
            if exclude_zero_lenght and (size<=0 and ftype!='DATETIME'):
                continue
            field_list.append(sql_field_schema(field=f
                , max_limit=max_limit
                , min_limit=min_limit) + "\n")
        return cmd_prefix + "CREATE TABLE {} (\n\t  ".format(self._name) + "\t, ".join(field_list) + ")" + cmd_sufix

    def copy(self)->Schema:
        """
        Faz uma copia do objeto de Schema.
        Eh usado quando se faz copias
        de Datasets.
        """
        cp = Schema()
        cp._schema = copy.copy(self._schema)
        cp._pks = copy.copy(self._pks)
        cp._alias = copy.copy(self._alias)
        return cp

    # TODO: remover este methodo
    def _get_default_name(self):
        """
        """
        warnings.warn('Method "Schema._get_default_name" will be discontinued. Use "Schema.gen_field_name" instead.')
        # return Schema.default_name.format(len(self._schema))
        return self.gen_field_name()

    def _instance_gen_field_name(self):
        """
        Gera um novo nome para um campo.
        Usa o formato padrão da classe
        para gerar o nome.
        @see Schema.gen_field_name
        @see Schema.default_name
        """
        return Schema.gen_field_name(len(self._schema))

    def _gen_field(self
        , name:str=None
        , ftype:str='string'
        , size:int=50
        , default=None
        , primary_key:bool=False
        , auto_increment:int=0
        , optional:bool=False
        # TODO remover a propriedade "null". "optional" entra no lugar.
        , null:bool=True):
        if not name:
            name = self.gen_field_name()
        if ftype=='string':
            warnings.warn('Use Python types to define the data type. Instead of using "string" use "str".')
            ftype = str
        """
        f = {'name':name
            , 'ftype':ftype
            , 'size':size
            , 'default':default
            , 'primary_key':primary_key
            , 'auto_increment':auto_increment
            , 'null':null}
        return f
        """
        f = self._Field(name=name
            , ftype=ftype
            , size=size
            , default=default
            , auto_increment=auto_increment
            , optional=optional)
        # TODO: alterar o metodo add_field para receber um objeto do tipo _Field
        # self._schema.append(f)
        # if primary_key:
        #     self.set_primary(f)
        return f

    def Field(self
        , name:str=None
        , ftype=str
        , size:int=50
        , default=None
        , primary_key:bool=False
        , auto_increment:int=0
        , optional:bool=False
        , col_ref:int=None
        , pos:str='a')->_Field:
        """
        Interface para instaciar a classe _Field
        e adicionar novos campos ao schema.
        """
        # se nao foi passado um nome, gera um nome
        return self.add_field(name=name
            , ftype=ftype
            , size=size
            , default=default
            , primary_key=primary_key
            , auto_increment=auto_increment
            , optional=optional
            , col_ref=col_ref
            , pos=pos)

    def add_field(self
        , name:str=None
        , ftype=str
        , size:int=50
        , default=None
        , primary_key:bool=False
        , auto_increment:int=0
        , optional:bool=False
        , col_ref:int=None
        , pos:str='a'
        # TODO remover a propriedade "null". "optional" entra no lugar.
        , null:bool=None)->Schema:
        if null is not None:
            warnings.warn('The "null" property will be deprecated. Use "optional" instead.')
            # optional = null
        f = self._gen_field(name=name
            , ftype=ftype
            , size=size
            , default=default
            , primary_key=primary_key
            , auto_increment=auto_increment
            , optional=optional
            # TODO remover a propriedade "null". "optional" entra no lugar.
            , null=null)
        f.set_schema(self)
        """
        # substitui por __append_field
        if col_ref is None:
            self._schema.append(f)
        else:
            if pos=='a':
                col_ref += 1
            self._schema[col_ref:col_ref] = [f]
        """
        self.__append_field(field=f, col_ref=col_ref, pos=pos)
        if primary_key:
            self.set_primary(f)
        return self

    def rm_field(self, name)->Schema:
        pos = self.get_field_pos(name)
        if pos is not None:
            self._schema.pop(pos)
        return self

    def rename_field(self, name, new_name:str)->Schema:
        pos = self.get_field_pos(name)
        if pos is not None:
            self.set_alias(self._schema[pos], new_name)
        return self

    def get_all_field_pos(self)->dict:
        allpos = {}
        for i, f in enumerate(self._schema):
            allpos[f.get_name()]=i
        return allpos

    def get_field_pos(self, name)->int:
        if type(name) is int:
            if name>=len(self._schema):
                # print(str(self))
                raise Exception("'%s' eh uma posicao alem da quantidade %s de campos no esquema do dataset." % (name, len(self._schema)))
            return name
        elif type(name) is str:
            allpos = self.get_all_field_pos()
            pos = allpos.get(name, -1)
            if pos<0:
                raise Exception("'%s' eh um campo que nao existe do esquema do dataset." % name)
            return pos
        else:
            raise Exception("Tipo nao identificado.")

    def __append_field(self
        , field:_Field
        , col_ref=None
        , pos='a'):
        """
        Metodo para adicionar um campo
        ao schema na posicao indicada.
        """
        # self._schema.append(f)
        if col_ref is None:
            self._schema.append(field)
        else:
            if pos=='a':
                col_ref += 1
            self._schema[col_ref:col_ref] = [field]
        return self



    def pk(self
        , name:str=None
        , ftype=int
        , size:int=50
        , default=None
        , auto_increment:int=0
        , col_ref:int=None
        , pos:str='a')->_Field:
        """
        Cria um campo do tipo primary key.
        """
        f = self.Field(name=name
            , ftype=ftype
            , size=size
            , default=default
            , auto_increment=auto_increment
            , col_ref=col_ref
            , pos=pos
            , primary_key=True
            , optional=False)
        return f

    def get_names(self)->list:
        """
        Retorna a lista de nomes (ou alias)
        de todos os campos do schema.
        """
        name = []
        for f in self._schema:
            name.append(f.get_name())
        return name

    def optional(self
        , name:str=None
        , ftype=str
        , size:int=50
        , col_ref:int=None
        , pos:str='a')->_Field:
        """
        Cria um campo opcional.
        """
        return self.Field(name=name
            , ftype=ftype
            , size=size
            , col_ref=col_ref
            , pos=pos
            , default=None
            , auto_increment=0
            , primary_key=False
            , optional=True)
        


    def set_primary(self, field:_Field):
        """
        Adiciona um campo como parte
        da chave primaria do schema
        caso o campo faca parte do schema.
        """
        # se o campo nao existe, retorna erro
        if not self.field_exists(field):
            raise Exception("It's not possible to define as key for a schema if field that does not make part of it.")
        if field not in self._pks:
            self._pks.append(field)
        return self

    # metodos de relacionamento

    def has(self
        , schema:Schema
        , target=None
        , alias=None):
        """
        """
        pass

    def belongs(self
        , schema:Schema
        , identified=True
        , target:list=None
        , alias:list=None)->Schema:
        """
        Propaga os campos do relacionamento
        para o schema fonte.
        """
        # se nao foi indicado quais
        # colunas propagar, usar as pks
        if not target:
            target = []
            for pk in schema._pks:
                target.append(pk)
        # ...
        i:int = 0
        for f in target:
            if not self.field_exists(f):
                self._schema.append(f)
            # se for um relacionamento identificado
            # coloca esse campo na lista de pks
            if identified:
                self.set_primary(f)
            if i<=(len(alias)-1):
                self.set_alias(f, alias[i])
            i += 1
        # ...
        return self

    def field_exists(self
        , field:_Field)->bool:
        """
        Check if a field is part of schema.
        """
        for f in self._schema:
            if f==field:
                return True 
        return False
        
    def set_alias(self
        , field:_Field
        , alias:str)->Schema:
        """
        Defines an alias for a field.
        """
        # se o campo nao existe, retorna erro
        if not self.field_exists(field):
            raise Exception("It's not possible to define a alias for a field that does not exist in the schema.")
        self._alias[alias] = field
        return self

if __name__ == "__main__":
    # a classe gera nome de colunas
    print(Schema.gen_field_name(10))

    schema = Schema("teste")
    schema.add_field('nome')
    schema.add_field()
    print(schema)
    print(schema.len())
    print(schema.sql_create_table())
    