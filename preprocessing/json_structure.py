#!/usr/bin/env python3
import json, sys
from os import listdir
from os.path import isfile, join

def printt(text, indent, new_line=True):
    for _ in range(indent):
        print("\t", end="")
    if new_line:
        print(text)
    else:
        print(text, end="")

def parse_keys(data):
    root_dict = {}
    if isinstance(data, dict):
        for key in data.keys():
            root_dict[key] = parse_keys(data[key])
    elif isinstance(data, list):
        root_dict = parse_list(data)

    return root_dict

def parse_list(data):
    patterns = []
    for element in data:
        if isinstance(element, dict):
            element_dict = {}
            for key in element.keys():
                element_dict[key] = parse_keys(element[key])
            if element_dict not in patterns:
                patterns.append(element_dict)
        elif isinstance(element, list):
            patterns.append([parse_list(element)])

    return patterns

def print_structure(data, indent):
    if isinstance(data, dict):
        for key in data.keys():
            is_leaf = isinstance(data[key], str)
            printt(key, indent)
            if is_leaf:
                print_structure(data[key], 0)
            else:
                print_structure(data[key], indent+1)
    elif isinstance(data, list):
        printt("[", indent)
        for element in data:
            print_structure(element, indent+1)
            printt("-------------------------", indent+1)
        printt("]", indent)
    else:
        printt(data, indent)

'''
Parse JSON structure of files in a given directory.
Does not look for files recursively.
'''
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: json_structure.py <json files directory>")
        sys.exit(0)

    # Get files in dir
    dir = sys.argv[1]
    files = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

    # Iterate over files
    dict_list = []
    for file in files:
        json_file = open(file)
        data = json.load(json_file)
        file_dict = parse_keys(data)
        if file_dict not in dict_list:
            dict_list.append(file_dict)

    print("Number of different structures: " + str(len(dict_list)))
    print_structure(dict_list, 0)
