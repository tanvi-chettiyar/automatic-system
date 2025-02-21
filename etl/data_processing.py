import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from argparse import ArgumentParser
from typing import List, Dict, Any, Final
from time import sleep
from datetime import datetime
from base.etl_classes import UsingCopyExpert


ITERATION: Final[str] = "2"

def parse_arguments() -> ArgumentParser:
    """
    This function is to pass command line arguments. 
    """
    parse = ArgumentParser()
    parse.add_argument("-p", "--path", type= str, help= 'filepath')
    arguments = parse.parse_args()
    return arguments

def file_watcher(args: ArgumentParser, counter: int) -> str | List[str]:
    """
    This functions is to look for a file
    """
    
    if os.path.exists(args.path) and os.path.isdir(args.path):
        print("path is a directory")
        files_list = []
        # files.extend(args.path + os.listdir(args.path))
        # print(files)
        files_list = os.listdir(args.path)
        files = [ os.path.join(args.path, file) for file in files_list]
        # print(files)

        if not files and counter <= int(ITERATION):
            # raise FileNotFoundError(f"No files found in this directory {args.path} ")
            sleep(5)
            print(f'waited for 5 seconds {datetime.now()}')
            counter += 1
            file_watcher(args, counter)
        else:
            raise FileNotFoundError(f"file_watcher waited for {ITERATION} iterations and exited afterwards")

    elif os.path.exists(args.path) and os.path.isfile(args.path):
        print("path is a file")
        files = args.path

    else:
        print("Path doesn't exist")
        raise FileNotFoundError("Path doesn't exist")

    return files

def main():
    """
    This program is to load a file to a database table
    Usage: python data_processing.py -p "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt"
    """
    postgres_parameters: Dict[str, str] = {"database": 'postgres', 
                       "user":'tanvi_rajkumar', 
                       "password": '', 
                       "host": 'localhost', 
                       "port": "5432",
                       }
    
    args = parse_arguments()
    file_object = UsingCopyExpert(**postgres_parameters)

    files = file_watcher(args, 2)
    
    if files is not None and isinstance(files, str):
        file_object.execute('copy_from_file', files)
    elif files is not None and isinstance(files, List):
        for file in files:
            file_object.execute('copy_from_file', file)
    else:
        # print(type(files))
        pass


    

if __name__ == "__main__":
    
    main()
