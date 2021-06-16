# ----------------------------------------------------------------------------------
# File:				main.py
# Author:			Brian Martinez
# Date Created:		June 15, 2021

# Description: This script will gather the file names on a folder and output a variable to be used on main.py
# ----------------------------------------------------------------------------------

import json
from posix import listdir
import sys
import os

FOLDER_PATH = "/Users/bm1120/Documents/Projects/UDP/ParsedFilesFolders/71"

def main():

    file_names = [f for f in os.listdir(FOLDER_PATH) if os.path.isfile(os.path.join(FOLDER_PATH, f))]
    python_var = str(file_names)
    java_var = python_var.replace("'", '"').replace('[','{').replace(']','}') + ";"
    #print(files)
    stop = True



    pass






if __name__ == '__main__':
    sys.exit(main())

