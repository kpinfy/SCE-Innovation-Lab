
################################################################################
# Script Name: infosys_silo_weekly_report_validation_script.py
# Purpose: This is the script to validate compare data two .csv files
#
# Source datasets: output_reference.csv, solution2_output.csv
# Output: Text specifying if two files match
# Last Modified By: Infosys Team
# Last Modified Date: Feb 25, 2024
# Version: 1.0
# Version Comment: Final version
################################################################################

import csv

def match_csv_header(file1, file2):
    try:
        with open(file1, 'r',encoding='UTF-8') as f1, open(file2, 'r',encoding='UTF-8') as f2:
            reader1 = csv.reader(f1)
            reader2 = csv.reader(f2)
        #convert csv data to list
            csv1 = list(reader1)
            csv2 = list(reader2)
            try:
                assert csv1[0] == csv2[0]
                # print("Headers are different")
                # return
            except AssertionError as msg:
                print ("csv header match ~Failed")
            else:
                print ("csv header match ~Passed")
    except Exception as e:
            print(e)

def match_csv_record_count(file1, file2):
    try:
        with open(file1, 'r') as f1, open(file2, 'r') as f2:
            reader1 = csv.reader(f1)
            reader2 = csv.reader(f2)
        #convert csv data to list
            csv1 = list(reader1)
            csv2 = list(reader2)

            try:
                assert len(csv1[0]) == len(csv2[0])
            
            except AssertionError as msg:
                print ("csv record count match ~Failed")
            else:
                print ("csv record count match ~Passed")
    except Exception as e:
            print(e)

def match_csv_data(file1, file2):
    difference_found = False
    try:
        with open(file1, 'r') as f1, open(file2, 'r') as f2:
            reader1 = csv.reader(f1)
            reader2 = csv.reader(f2)
        #convert csv data to list
            csv1 = list(reader1)
            csv2 = list(reader2)
            for i in range(1, min(len(csv1), len(csv2))):
                row1 = csv1[i]
                row2 = csv2[i]
                if row1 != row2:
                    print(f"Difference found in row {i+1}:")
                    print(f"File1: {row1}")
                    print(f"File2: {row2}")

                    print()
                    difference_found = True
        try:
            assert not difference_found
            
        except AssertionError as msg:
            print ("csv data match ~Failed")
        else:
            print ("csv data match ~Passed")
    except Exception as e:
            print(e)
        

file1_path = 'output_reference.csv'
file2_path = '../output/solution2_output.csv'

# Unit test cases        
match_csv_header(file1_path, file2_path)
match_csv_record_count(file1_path, file2_path)
match_csv_data(file1_path, file2_path)