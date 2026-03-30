# 2026-03-26 - https://github.com/andrew-siwko/gemini-hr-aggregation
# The intent of this code is to extract heart rate and related data from Garmin FIT files and save it to a CSV file for further analysis.

# download the zip files from the Garmin web interface and place them in the /data/fit directory before running this code.
# if you're on linux, make sure to put a .zip extension on the files.

 # to parse the garmin files
import fitparse

# to aggregate and export the data
import pandas 

# to list the zip files and import files
import os 

# to exttract the fit files frmthe zip files
import zipfile

import datetime
import pytz

# I'm in Eastern time.  Before I made the time zone conversion, Gemini was unable to correlate my activities with the local time.
my_tz = pytz.timezone('America/New_York')

# Garmin timestamps are a bit complicates.  I got the garmin epoch algoritm from Stack Overflow: https://stackoverflow.com/questions/57774180/how-to-handle-timestamp-16-in-garmin-devices
garmin_epoch = int(datetime.datetime.timestamp(datetime.datetime(1989, 12, 31, tzinfo=datetime.timezone.utc)))

# get a list of the zip files in /data/fit.
zip_files=sorted(['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.zip')])

# extract all the fit files from the zip files into the /data/fit directory.
for zip_file in zip_files:
    print('extracting',zip_file)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall('/data/fit')


# now list the fit files.
fit_files=sorted(['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.fit')])

# During development I tracked message formats that I hadn't seen before looking for interesting information.
message_fields_seen=[]

# and here are the columns I decided I wanted.  The code seeds a dictionary with all columns so that it will import into a DataFrame easily.
interesting_columns=['message_date','heart_rate','activity_type','resting_metabolic_rate', 'activity_type', 'intensity','stress_level_value', 
                     'steps', 'active_time','activity_type','moderate_activity_minutes','vigorous_activity_minutes' ]

def parse_fit_hr(file_path):
    # Load the FIT file
    fitfile = fitparse.FitFile(file_path)
    # Garmin timestamp_16s are relative to the last timestamp, so we need to keep track of the last timestamp.
    last_timestamp = None
    # print(file_path)
    # a list of dictionaries with interesting_column data.
    hr_data = []

    time_created=None
    for message in fitfile.messages:
        # initialize a dictionary with all interesting columns.
        data_point={}
        for column in interesting_columns:
            data_point[column]=''

        # get a list of fields in the message?
        field_names=[x.name for x in message.fields]

        # if we havn't seen this combination of fields before, print it out to see whether it has any interesting data.
        if str(field_names) not in message_fields_seen:
            message_fields_seen.append(str(field_names))
            print('******* new message fields',field_names)

        # if three's a timesttamp, save it and compute the message_date.
        if 'timestamp' in field_names:
            # print('got timestamp',message.get_raw_value("timestamp"))
            last_timestamp = message.get_raw_value("timestamp")
            message_date = datetime.datetime.fromtimestamp(garmin_epoch + last_timestamp, tz=datetime.timezone.utc).astimezone(my_tz)
            data_point['message_date']=message_date
            # print('last timestamp',last_timestamp)
        # if there's a timestamp_16, compute the message_date.
        elif 'timestamp_16' in field_names:
            timestamp16 = message.get_raw_value("timestamp_16")
            timestamp = last_timestamp
            timestamp += (timestamp16 - (last_timestamp & 0xFFFF)) & 0xFFFF
            message_date = datetime.datetime.fromtimestamp(garmin_epoch + timestamp, tz=datetime.timezone.utc).astimezone(my_tz)
            data_point['message_date']=message_date
            # print('message_date',message_date)
        # stress readings hav their oen time source, so compute the message_date from that.
        elif 'stress_level_time' in field_names:
            stress_level_time = message.get_raw_value("stress_level_time")
            message_date = datetime.datetime.fromtimestamp(garmin_epoch + stress_level_time, tz=datetime.timezone.utc).astimezone(my_tz)
            data_point['message_date']=message_date
            # print('message_date',message_date)
        
        # let's check all the columns we got fromthe message to see whether there's anything interesting
        interesting_message=False
        for data in message:
            for column in interesting_columns:
                if column in data.name:
                    data_point[column]=data.value
                    interesting_message=True
        # if we did get an interesting column, then add to the list of data points.
        if interesting_message==True:
            hr_data.append(data_point)

    # convert the list of data points to a DataFrame and return it.
    df = pandas.DataFrame(hr_data)
    return df

# this aggregates the DataFrame from each fit file.
all_hr_data=[]


for fit_file in fit_files:
    # my daughter had activity files, I don't so I'm skipping them.
    if 'activity' not in fit_file.lower():
        # append the data from one file.
        all_hr_data.append(parse_fit_hr(fit_file))

# take the list of DataFrames from every file and concatenate them into one big DataFrame and save it to a CSV file.
pandas.concat(all_hr_data).to_csv('/data/fit/hr_data.csv',index=False)

