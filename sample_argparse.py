# def python_file(input_file, output_file):
#     with open(input_file, 'r') as open_file_1:
#         with open(output_file, 'w') as open_file_2:
#             for content in open_file_1.readlines():
#                 open_file_2.write(content)
#     print("Output file with function has been created")


# import argparse

# parse = argparse.ArgumentParser()
# parse.add_argument('input', type=str, nargs=1, help= 'Provide the path of input file')
# parse.add_argument('output', type=str, nargs=1, help= 'Provide the path of output file')
# args = parse.parse_args()

# python_file(args.input[0], args.output[0])

# with open(args.input[0], "r") as open_file_1:
#     with open(args.output[0], "w") as open_file_2:
#         for content in open_file_1.readlines():
#             content = content.replace(",", "|")
#             open_file_2.write(content)
# print("Output File has been created.")

# # sql_filepath = "/Users/tanvi_rajkumar/Library/DBeaverData/workspace6/General/Scripts/Script-2.sql"
# # output_filepath = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/sql_output.txt"

# import psycopg2
# import argparse

# def SQL_connection(database1: str, user1: str, password1: str, host1: str, port1: str):
#     open_connection = psycopg2.connect(database = database1, user = user1, password = password1, host = host1, port = port1)
#     open_cursor = open_connection.cursor()
    
#     parser3 = argparse.ArgumentParser()
#     parser3.add_argument('input', type= str, nargs= 1, help= 'provide file path to input SQL script')
#     parser3.add_argument('output', type= str, nargs= 1, help= 'provide file path to output file')
#     args3 = parser3.parse_args()

#     sql_script = open(args3.input[0], 'r')
#     open_cursor.execute(sql_script.read())
#     contents3 = open_cursor.fetchall()

#     output_file = open(args3.output[0], 'w')
#     for x in contents3:
#         output_file.write(str(x))
    
#     print('Output file has been created')

#     sql_script.close()
#     output_file.close()
    
#     open_cursor.close()
#     open_connection.close()


if __name__ == "__main__":

    # SQL_connection('postgres', 'tanvi_rajkumar', '', 'localhost', '5432')
    import argparse
    from postgres_connector import PostgresConnector

    parser4 = argparse.ArgumentParser()
    parser4.add_argument('databaseInfo', type= str, nargs= 5, help= 'provide database information in the following order: database, user, password, host, port')
    parser4.add_argument('filepaths', type= str, nargs= 2, help= 'provide file path to input SQL script')
    args4 = parser4.parse_args()

    with PostgresConnector(database= args4.databaseInfo[0], user= args4.databaseInfo[1], password= args4.databaseInfo[2], host= args4.databaseInfo[3], port= args4.databaseInfo[4]) as cursor2:
        with open(args4.filepaths[0], 'r') as input_file:
            with open(args4.filepaths[1], 'w') as output_file:
                for x in input_file.readlines():
                    script4 = cursor2.execute(x)
                    contents4 = cursor2.fetchall()
                    output_file.write("\n"+str(x)+"\n"+str(contents4)+"\n")
            
    
    print("output file has been created")