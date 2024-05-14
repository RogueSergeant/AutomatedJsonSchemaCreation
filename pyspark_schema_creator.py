def prepare_from_object(obj):
    if isinstance(obj, str):
        return "StringType()"
    elif isinstance(obj, int):
        return "IntegerType()"
    elif obj is None:
        return "StringType()"
    elif not isinstance(obj, dict):
        return None

    schema_str = []
    for k, v in obj.items():
        if isinstance(v, str):
            schema_str.append(f"StructField('{k.lower()}',StringType(),True)")
        elif isinstance(v, bool):
            schema_str.append(f"StructField('{k.lower()}',BooleanType(),True)")
        elif isinstance(v, int):
            schema_str.append(f"StructField('{k.lower()}',IntegerType(),True)")
        elif isinstance(v, list):
            array_schema = f"StructField('{k.lower()}',ArrayType({prepare_from_object(v[0])}),True)"
            schema_str.append(array_schema)
        elif isinstance(v, dict):
            obj_schema = prepare_from_object(v)
            schema_str.append(f"StructField('{k.lower()}',{obj_schema},True)")

    schema = f"StructType([{','.join(schema_str)}])"
    return schema

def generate_string_only_schema(columns):
    schema = [f"StructField('{col.lower()}',StringType(),True)" for col in columns]
    schema_string = f"StructType([{','.join(schema)}])"
    return "schema = " + schema_string

def get_spark_type(type_name):
    if type_name == "numeric":
        return "IntegerType()"
    elif type_name == "date":
        return "TimestampType()"
    elif type_name == "string":
        return "StringType()"
    else:
        return "StringType()"

def generate_type_based_schema(columnDetails):
    schema = [f"StructField('{val['name'].lower()}',{get_spark_type(val['type'])},True)" for val in columnDetails]
    schema_string = f"StructType([{','.join(schema)}])"
    return "schema = " + schema_string

def get_column_details(csvData):
    lines = csvData.strip().split("\n")
    header = lines[0].split(",")
    columnDataTypes = []
    for columnIndex, columnName in enumerate(header):
        columnName = columnName.strip()
        columnValues = [line.split(",")[columnIndex].strip() for line in lines[1:]]
        isNumeric = all(value.isdigit() for value in columnValues)
        isDate = all(value.replace("-", "").isdigit() for value in columnValues)
        if isNumeric:
            columnDataTypes.append({"name": columnName, "type": "numeric"})
        elif isDate:
            columnDataTypes.append({"name": columnName, "type": "date"})
        else:
            columnDataTypes.append({"name": columnName, "type": "string"})
    return columnDataTypes

def generate_schema(input_json):
    return prepare_from_object(input_json)