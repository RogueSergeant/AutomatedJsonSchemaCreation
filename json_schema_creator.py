from deepmerge import Merger
import json

def generate_schema(data):
    if isinstance(data, dict):
        return {k: generate_schema(v) for k, v in data.items()}
    elif isinstance(data, list) and data:
        return [generate_schema(data[0])]
    else:
        return type(data).__name__



def replace_values(schema):
    if isinstance(schema, dict):
        return {k: replace_values(v) for k, v in schema.items()}
    elif isinstance(schema, list):
        return [replace_values(v) for v in schema]
    elif schema == "int":
        return 1
    elif schema == "float":
        return 3.14
    else:
        return schema

def main(data):
    
    # Define a custom Merger that uses the first list when merging lists
    custom_merger = Merger(
        # pass in a list of tuple, with the
        # strategies you are looking to apply
        # from least to most important
        [(list, "override"), (dict, "merge")],
        # next, choose the fallback strategies,
        # applied to all other types
        ["override"],
        # then, choose the strategies in
        # the case where the types conflict
        ["override"]
    )

    master_schema = {}
    
    if type(data) == list:

        for line in data:
            schema = generate_schema(line)
            master_schema = custom_merger.merge(master_schema, schema)
            
    else:
        schema = generate_schema(data)
        master_schema = custom_merger.merge(master_schema, schema)

    print(json.dumps(master_schema, indent=4))

    master_schema_adjusted = replace_values(master_schema)

    print(json.dumps(master_schema_adjusted, indent=4))
    
if __name__ == "__main__":
    data = json.load("data.json")
    
    dataType = type(data)
    
    main(data, dataType)