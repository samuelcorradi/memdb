# MemDB

An in-memory agnostic database for Python.

# Change history

**0.0.4**

- There was an attempt to make the Dataset schemaless. But usage has shown that there is no advantage to using schemaless dataset.

- Added private method `__set_schema` to set the schema in the dataset. If the method does not receive a schema, it takes care of instantiating one so that there is always a schema in the dataset.