from __future__ import annotations
import copy
import re

class Schema(object):

    default_name:str = 'col{}' 

    def __init__(self):
        self._schema = []

    def __str__(self):
        return self.to_str()

    def to_str(self)->str:
        fields = self.get_names()
        return ''.join(fields)

    def len(self):
        """
        Informa a quantidade de colunas
        existentes no schema. Basicamente
        conta a quantidade de posicoes da
        lista onde os campos sao definidos.
        """
        return len(self._schema)

    def sql_create_table(self
        , tablename:str
        , cmd_prefix=''
        , cmd_sufix=''
        , max_limit=400
        , min_limit=6
        , exclude_zero_lenght=True)->str:
        """
        Cria comando SQL para create table.
        """
        def sql_field_schema(field_schema:dict
            , max_limit:int
            , min_limit:int)->str:
            """
            Cria a parte de definicao dos campos.
            """
            ftype = field_schema.get('ftype', 'VARCHAR').upper()
            null = field_schema.get('null', True)
            sql = "[{}] {}".format(field_schema['name'], ftype)
            if ftype!='DATETIME':
                size = field_schema.get('size', 0)
                primary_key = field_schema.get('primary_key', False)
                auto_increment = field_schema.get('auto_increment', 0)
                if not auto_increment:
                    # if size<=0:
                    if size<min_limit:
                        size = min_limit
                    sql += "({})".format('MAX' if size>max_limit else str(size))
                if primary_key:
                    sql += ' PRIMARY KEY'
                if auto_increment:
                    sql += ' IDENTITY(1,{})'.format(auto_increment)
            if not null:
                sql += ' NOT NULL'
            return sql
        field_list = []
        for field_schema in self._schema:
            size = field_schema.get('size', 0)
            ftype = field_schema.get('ftype', 'VARCHAR')
            # field_schema = _default_schema(field_schema)
            # ser for para excluir campos que tem tamanho 0
            # e o tipo deste campo nao for DATETIME
            if exclude_zero_lenght and (size<=0 and ftype!='DATETIME'):
                continue
            field_list.append(sql_field_schema(field_schema=field_schema
                , max_limit=max_limit
                , min_limit=min_limit) + "\n")
        return cmd_prefix + "CREATE TABLE {} (\n\t  ".format(tablename) + "\t, ".join(field_list) + ")" + cmd_sufix

    def copy(self)->Schema:
        """
        Faz uma copia do objeto de Schema.
        Eh usado quando se faz copias
        de Datasets.
        """
        cp = Schema()
        cp._schema = copy.copy(self._schema)
        return cp

    def _get_default_name(self):
        return Schema.default_name.format(len(self._schema))

    def _gen_field(self
        , name:str=None
        , ftype:str='string'
        , size:int=50
        , default=None
        , primary_key:bool=False
        , auto_increment:int=0
        , null:bool=True):
        if not name:
            name = self._get_default_name()
        f = {'name':name
            , 'ftype':ftype
            , 'size':size
            , 'default':default
            , 'primary_key':primary_key
            , 'auto_increment':auto_increment
            , 'null':null}
        return f
    
    def add_field(self
        , name:str=None
        , ftype:str='string'
        , size:int=50
        , default=None
        , primary_key:bool=False
        , auto_increment:int=0
        , null:bool=True
        , col_ref:int=None
        , pos='a')->Schema:
        f = self._gen_field(name=name
            , ftype=ftype
            , size=size
            , default=default
            , primary_key=primary_key
            , auto_increment=auto_increment
            , null=null)
        if col_ref is None:
            self._schema.append(f)
        else:
            if pos=='a':
                col_ref += 1
            self._schema[col_ref:col_ref] = [f]
        return self

    def rm_field(self, name)->Schema:
        pos = self.get_field_pos(name)
        if pos is not None:
            self._schema.pop(pos)
        return self

    def rename_field(self, name, new_name:str)->Schema:
        pos = self.get_field_pos(name)
        if pos is not None:
            self._schema[pos]['name'] = new_name
        return self

    def get_all_field_pos(self)->dict:
        allpos = {}
        for i, f in enumerate(self._schema):
            allpos[f['name']]=i
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

    def get_names(self)->list:
        name = []
        for f in self._schema:
            name.append(f['name'])
        return name
        
class Dataset(object):

    def __init__(self, schema:Schema=None):
        """
        """
        self._data = []
        self._idx = 0
        if not schema:
            schema = Schema()
        self._schema = schema

    def __str__(self)->str:
        return self.to_str()

    def get_fields_size(self, rate:float=0.0)->dict:
        """
        """
        field_list = self._schema.get_all_field_pos()
        sizes = dict(zip(field_list.keys(), [0]*len(field_list)))
        #print(sizes)
        for row in self._data:
            for f, p in field_list.items():
                s = len(str(row[p]))
                if sizes[f]<s:
                    sizes[f] = s
        if rate>0:
            for f, s in sizes.items():
                sizes[f] = int(s + (s*rate))
        return sizes

    def columns(self):
        return self._schema.get_names()

    def to_str(self, length:int=None)->str:
        fields = self._schema.get_names()
        if not fields:
            return '| * No fields * |'
        if not length:
            length = int(80/len(fields))
        r = '| ' + ' | '.join([name.ljust(length, ' ')[:length] for name in fields]) + ' |\n'
        for _, row in enumerate(self._data):
            r += '| ' + ' | '.join([str(i).ljust(length, ' ')[:length] for i in row]) + ' |\n'
        return r

    def get_schema(self):
        """
        Pega o objeto de schema.
        """
        return self._schema

    def _insert(self, row, idx:int=None)->Dataset:
        """
        Insere dados no dataset.
        """
        if type(row) is dict:
            nrow = [None]*len(self._schema.get_names())
            for k, v in row.items():
                pos = self._schema.get_field_pos(k)
                if pos==-1:
                    raise Exception("O campo %s onde se está tentando inserir não existe." % k)
                nrow[pos] = v
            row = nrow
            self._insert(row, idx)
        elif type(row) is list:
            if len(row)!=len(self._schema.get_names()):
                print((len(row), len(self._schema.get_names()), row))
                raise Exception("Número de colunas a inserir difere do número de colunas do schema definido.")
            if not idx:
                self._data.append(copy.copy(row))
            else:
                self._data[idx] = copy.copy(row)
        elif type(row) is tuple:
            self._insert(list(row), idx)
        else:
            print(type(row))
            raise Exception("Tipo não reconhecido.")
        return self

    def insert(self, row)->Dataset:
        """
        Insere dados no dataset.
        """
        return self._insert(row)

    def update(self, row, idx:int)->Dataset:
        """
        Atualiza uma linha no dataset.
        """
        return self._insert(row, idx)

    def delete(self, idx)->Dataset:
        """
        Deleta uma determinada posicao.
        """
        del self._data[idx]
        return self

    def current_update(self, row)->Dataset:
        """
        Atualiza a linha na posicao do
        cursor atual do dataset.
        """
        return self._insert(row, self._idx)

    def current_delete(self)->Dataset:
        """
        Deleta a linha onde o cursor estah
        """
        del self._data[self._idx]
        return self

    def current_insert(self, row)->Dataset:
        """
        Insere dados no dataset.
        """
        return self.update(row, self._idx)

    def get_index(self):
        """
        Pega a posicao atual do indice.
        """
        return self._idx

    def rownumber(self):
        """
        Pega a posicao atual do indice.
        """
        return self._idx

    def add_field(self
        , name:str
        , ftype:str='string'
        , size:int=50
        , default=None
        , primary_key:bool=False
        , auto_increment:int=0
        , null:bool=True
        , col_ref:int=None
        , pos='a'):
        self._schema.add_field(name=name
            , ftype=ftype
            , size=size
            , default=default
            , primary_key=primary_key
            , auto_increment=auto_increment
            , null=null
            , col_ref=col_ref
            , pos=pos)
        pos = self._schema.get_field_pos(name)
        for _, row in enumerate(self._data):
            row[pos:pos] = [default]
        return self

    def remove_col(self, col):
        pos = self._schema.get_field_pos(col)

        if pos is not None:
            self._schema.rm_field(pos)
            for row in self._data:
                del row[pos]
        return self

    def copy(self, copy_data=True)->Dataset:
        """
        Faz uma copia do dataset. Usa-se
        para fazer transformações sem
        destruir o original.
        """
        cp = Dataset(copy.deepcopy(self._schema))
        if copy_data:
            cp._data = copy.deepcopy(self._data)
        return cp

    def cut(self, start:int=0, end:int=None)->Dataset:
        """
        Faz um corte no dataset retornando
        um intevalo selecionando
        """
        if end is None:
            end = len(self._data)
        self._data = self._data[start:end]
        return self

    def is_empty(self)->bool:
        """
        Diz se o dataset estah vazio.
        """
        return len(self._data)==0

    def count(self)->int:
        return self.len()

    def len(self)->int:
        """
        Quantidade de linhas do Dataset
        """
        return len(self._data)

    def rewind(self)->Dataset:
        """
        """
        self._idx = 0
        return self

    def first(self)->Dataset:
        """
        """
        self.rewind()
        return self.current()

    def current(self):
        """
        """
        if self._idx<0:
            return None
        try:
            return self._transform_row(self._data[self._idx])
        except:
            return None

    def next(self):
        """
        """
        self._idx += 1
        return self.current()

    def prev(self):
        """
        """
        self._idx = self._idx - 1
        return self.current()

    def last(self):
        """
        """
        self._idx = self.len() - 1
        return self.current()

    def truncate(self)->Dataset:
        """
        Limpa a base de dados.
        """
        self.rewind()
        self._data = []
        return self

    def _transform_row(self, row):
        """
        """
        return row

    def where(self, filter):
        filter_result = self.filter(filter)
        data = [v for i, v in enumerate(self._data) if filter_result[i]]
        #new_dataset = copy.copy(self)
        #new_dataset._data = data
        self._data = data
        # return new_dataset
        return self

    def filter(self, filter)->list:
        """
        equal usa-se um dicionário: {'[campo]':2}
        diff usa-se um dicionário: {'[campo]diff':2}
        in o valor deve ser um array: {'campo':['Valor1', 'Teste']}
        and deve-se se usar uma lista: [{}, {}] ou um dicionario com várias posicoes  {'campo':2, 'campo2':'Valor2'}
        or deve-se usar uma tupla: ({}, {})
        """
        def parse_name(name):
            m = re.findall(r'\[([a-zA-Z0-9\_]+)\]([a-zA-Z]*)', name)
            if m:
                return m[0]
        filter_result = [True]*len(self._data)
        for i, row in enumerate(self._data):
            if type(filter) is dict:
                for k, v in filter.items():
                    mod = ''
                    if type(k) is str:
                        parsed = parse_name(k)
                        if parsed:
                            k = parsed[0]
                            mod = parsed[1].lower()
                        k = self._schema.get_all_field_pos()[k]
                    if not mod:
                        mod = 'equal'
                    if (mod=='equal' and row[k]!=v)\
                        or (mod=='diff' and row[k]==v)\
                        or (mod=='like' and v not in row[k])\
                        or (mod=='notlike' and v in row[k]):
                        filter_result[i] = False
                        continue
        return filter_result
