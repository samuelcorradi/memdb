# MemDB

An in-memory agnostic database for Python.

# Change history

**0.0.5**

- The `Dataset.to_str()` method now has the limit parameter to define the number of lines that should be displayed.

- Added the `Dataset.all()` method to return all data, without applying any filters.

- Added the `format` parameter in the `add_field()` method. This parameter is passed to the method of the `Schema` class and is used by it to define a format for the new field created.

- Added `date_format` attribute to Dataset class. This attribute is used whenever values are entered in fields of type `date` or `datetime` to store the value in the intended format.

- Validation of values at insertion time. Fields of type `date` or `datetime`, when receiving a value of type `str`, check the string format. If it's a valid date, it converts to the date format defined in the field (as an field attribute of the `Schema` object) or defined in the `date_format` attribute of the dataset.

- Dataset now works as an iterator. Use foreach to loop through the lines.

- Dataset now has a `date_out_format` property where we define the format in which date-type fields should display their values when converted to string.

- Fixed the method that calculates the size of fields. Before I was calculating the size of the fields without considering the output formats when converting the fields to string.

**0.0.4**

- There was an attempt to make the Dataset schemaless. But usage has shown that there is no advantage to using schemaless dataset.

- Added private method `Dataset.__set_schema()` to set the schema in the dataset. If the method does not receive a schema, it takes care of instantiating one so that there is always a schema in the dataset.