# stocks

Implements a basic live and historical feeder using Yahoo API.
Requires python3 and pip install requests.

Example usage:

historical:
./main.py -s "2015-04-01" -e "2015-05-01" GLEN.L ARM.L

live:
./main.py GLEN.L ARM.L
