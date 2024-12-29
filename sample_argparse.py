def python_file(input_file, output_file):
    with open(input_file, 'r') as open_file_1:
        with open(output_file, 'w') as open_file_2:
            for content in open_file_1.readlines():
                open_file_2.write(content)
    print("Output file with function has been created")


import argparse

parse = argparse.ArgumentParser()
parse.add_argument('input', type=str, nargs=1, help= 'Provide the path of input file')
parse.add_argument('output', type=str, nargs=1, help= 'Provide the path of output file')
args = parse.parse_args()

python_file(args.input[0], args.output[0])

# with open(args.input[0], "r") as open_file_1:
#     with open(args.output[0], "w") as open_file_2:
#         for content in open_file_1.readlines():
#             content = content.replace(",", "|")
#             open_file_2.write(content)
# print("Output File has been created.")




