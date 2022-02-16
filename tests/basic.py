from memdb.dataset import Dataset
from schemy import Schema

if __name__ == "__main__":
    schema = Schema()
    schema.add_field('nome')
    schema.add_field()
    dataset = Dataset(schema)
    dataset.insert({'nome':'Jhon Silver', 'col1':'Address 1'})
    dataset.insert({'nome':'Silvia Santos', 'col1':'Address 2'})
    # create a result set
    f = dataset.filter({'[nome]diff':'Samuel Corradi'})
    print(f)
    # print a formated dataset
    print(dataset)
    # create a new dataset after filter
    rows = dataset.where({'[nome]diff':'Samuel Corradi'})
    print(rows)