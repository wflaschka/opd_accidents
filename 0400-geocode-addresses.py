#!/usr/bin/env python
# coding: utf-8
"""

Geocode the addresses in our collected tweets, preserving dates of tweets.

"""
import pandas as pd
import geocoder
import requests



df = pd.read_csv("data-summary/all_accidents.csv")

##########################################
## Process the incoming data

### Remove the 'blah-blah reported accident' stuff from addresses
df['addrtmp'] = df['text'].str.replace('^\d\d:\d\d Reported( Auto)? Accident at/near ,?', '', regex=True)

### Add default Oxford address text
df['address'] = [f"{a}, Oxford MS 38655" for a in df['addrtmp']]
df.drop(['addrtmp'], axis=1, inplace=True)


### Set time index and then sort
df.set_index('ts', inplace=True)
df.sort_index(axis=0, level=None, ascending=True, inplace=True)


### Check everything
print(df.head(3))
print(df.tail(3))

####################################################################################
####################################################################################
## Geocode what we need to geocode
##
## PARAMETERS:

output_file_path = "geocoded/geocodes-"

# Where the program starts processing the addresses in the input file
# Useful in case we have to resume from a certain point, after a crash or when augmenting data.
start_index = 0
start_index = 8500 # We're adding to existing geocoded addresses

# How often the program prints the status of the running program
status_rate = 250

# How often the program saves a backup file
write_data_rate = 250

# How many times the program tries to geocode an address before it gives up
attempts_to_geocode = 2

# Time it delays each time it does not find an address
# Note that this is added to itself each time it fails so it should not be set to a large number
wait_time = 2

##########################################
## Functions & classes

# Creates request sessions for geocoding
class GeoSessions:
    def __init__(self):
        self.Arcgis = requests.Session()
        self.Komoot = requests.Session()

# Class that is used to return 3 new sessions for each geocoding source
def create_sessions():
    return GeoSessions()

# Main geocoding function that uses the geocoding package to covert addresses into lat, longs
def geocode_address(address, s):
    g = geocoder.arcgis(address, session=s.Arcgis)
    if (g.ok == False):
        g = geocoder.komoot(address, session=s.Komoot)

    return g

def try_address(address, s, attempts_remaining, wait_time):
    g = geocode_address(address, s)
    if (g.ok == False):
        time.sleep(wait_time)
        s = create_sessions()  # It is not very likely that we can't find an address so we create new sessions and wait
        if (attempts_remaining > 0):
            try_address(address, s, attempts_remaining-1, wait_time+wait_time)
    return g

# Write data to the output file
def write_data(my_df, index):
    file_name = (output_file_path + str(index) + ".csv")
    print("Created the file: " + file_name)
    my_df.to_csv(file_name, sep=',', encoding='utf8')
    

##########################################
## Working vars

s = create_sessions()
results = []
failed = 0
total_failed = 0
result_df = pd.DataFrame()
i = 0

##########################################
## OUR LOOP
## Doesn't hack up the `PythonBatchGeocoder.py` sample code too much
for timestamp, row in df.iloc[start_index:].iterrows():

    address = row['address']

    try:
        g = try_address(address, s, attempts_to_geocode, wait_time)
        if (g.ok == False):
            print("Gave up on address: " + address)
            failed += 1
        else:
            row['Lat'] = g.latlng[0]
            row['Long'] = g.latlng[1]
            row['provider'] = g.provider
            result_df = result_df.append(row)

    # If we failed with an error like a timeout we will try the address again after we wait 5 secs
    except Exception as e:
        print(f"Failed with error {e} on address '{address}'. Will try again.")
        try:
            time.sleep(5)
            s = create_sessions()
            g = geocode_address(address, s)
            if (g.ok == False):
                print("    ...Did not find address.")
                failed += 1
            else:
                print("    ...Successfully found address.")
                row['Lat'] = g.latlng[0]
                row['Long'] = g.latlng[1]
                row['provider'] = g.provider
                result_df = result_df.append(row)
        except Exception as e:
            print(f"    ...Failed with error {e} on address '{address}'. Will try again.")
            failed += 1

    # Writing what has been processed so far to an output file
    if (i % write_data_rate == 0 and i != 0):
        write_data(result_df, i + start_index)

    i += 1


# Finished
write_data(result_df, i + start_index + 1)

print()
print("Finished")
print()
print()

