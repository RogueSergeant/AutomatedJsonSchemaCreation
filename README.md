# JSON to PySpark Schema Generator
This repository contains Python scripts that can generate JSON and PySpark schemas from JSON data. The scripts are designed to handle complex, nested JSON structures and output a PySpark schema that can be used to read the JSON data into a PySpark DataFrame.

## Files in this Repository
- pyspark_schema_creator.py: This script contains functions to generate PySpark schemas from Python objects. It includes functions to handle different data types (string, integer, boolean, array, and nested objects).

- json_schema_creator.py: This script uses the functions from pyspark_schema_creator.py to generate a PySpark schema from JSON data. It first generates a Python schema from the JSON data, then converts this to a PySpark schema. The script also includes a function to replace values in the schema with example values.

- sample_data.json: This is a sample JSON file that can be used to test the schema generation scripts. It contains data for two companies, each with multiple departments and employees.

- requirements.txt: This file lists the Python packages that are required to run the scripts. The required packages are deepmerge and black.

## How to Use
1. Install the required Python packages by running:
   '''
   pip install -r requirements.txt
   '''

2. Update the file_name variable in json_schema_creator.py to the path of your JSON file (don't include the .json extension).

3. Run json_schema_creator.py. This will generate two files: a JSON schema and a PySpark schema. The JSON schema will be saved as {file_name}_json_schema.json and the PySpark schema will be saved as {file_name}_spark_schema.txt.

Please note that the scripts are designed to handle JSON data that is structured as a list of dictionaries, where each dictionary can contain nested dictionaries and lists. If your JSON data is structured differently, you may need to modify the scripts.

## Credit where credit is due
The pyspark_schema_creator.py script is based on the fantastic work done by [preetranjan](https://preetranjan.github.io/pyspark-schema-generator/) who created a page with JavaScript to enable JSON data to be converted into a PySpark schema. I have modified it to work as Python, and of course the main function of my script is to take complex JSON data, flatten the schema and then convert it to a PySpark schema.
