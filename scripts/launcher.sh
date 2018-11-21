#!/bin/bash

#load python3 virtual environment
cd /home/ubuntu/CEDUS/test_env/
source bin/activate

#launch python script
cd /home/ubuntu/CEDUS/scripts/
python fromCygnusToDIT.py > output_fromCygnusToDIT.txt