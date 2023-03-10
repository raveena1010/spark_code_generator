operations_dict = { 
            "bf082da2-a0d9-4335-a62f-9804217a1436" : "write_dataframe",
            "1a3b32f0-f56d-4c44-a396-29d2dfd43423" : "read_dataframe",
            "6cba4400-d966-4a2a-8356-b37f37b4c73f" : "sql_transformation",
            "012876d9-7a72-47f9-98e4-8ed26db14d6d" : "sql_column_transformation",
            "6534f3f4-fa3a-49d9-b911-c213d3da8b5d" : "filter_columns",
            "7d7eddfa-c9be-48c3-bb8c-5f7cc59b403a" :  "filter_rows",
            "9c3225d8-d430-48c0-a46e-fa83909ad054" :  "projection",
            "d5f4e717-429f-4a28-a0d3-eebba036363a" :  "handle_missing_value",
            "06374446-3138-4cf7-9682-f884990f3a60" :  "join",
            "1fa337cc-26f5-4cff-bd91-517777924d66" :  "sort",
            "90fed07b-d0a9-49fd-ae23-dd7000a1d8ad" :  "union",

            }

separator = { "comma" : ',',
              "semicolon" : ";",
              "colon" : ":",
               "space" : " ",
               "tab" : "\t" }

numeric_type = { "int" : "numeric",
             "integer" : "numeric",
              "numeric" : "numeric",
              "float" : "numeric",
              "double" : "numeric",
              "number" : "numeric"}

string_type  = { "string" : "string",
              "varchar" : "string",
              "string"  : "string",
              "char"   : "string"}

timestamp_type = { "date" : "timestamp",
              "datetime" : "timestamp",
              "timestamp": "timestamp",
               "time"  : "timestamp"}

boolean_type =  {"boolean" : "boolean",
               "bool" : "boolean"}