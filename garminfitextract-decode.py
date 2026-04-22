# this code intends to reimplement the heart rate extraction process with fitdecode instead of fitparse
# 
import fitdecode
import pandas
import os
import zipfile
import pprint
import datetime
import time
import pytz 

start_time=time.time()

def etime(start_time):
    return(str(datetime.timedelta(seconds=time.time()-start_time)))

my_tz = pytz.timezone('America/New_York')

print(etime(start_time),'starting')

zip_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.zip')]

for zip_file in zip_files:
    # print(etime(start_time),'extracting',zip_file)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall('/data/fit')


fit_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.fit')]

# Garmin timestamps are a bit complicates.  I got the garmin epoch algoritm from Stack Overflow: https://stackoverflow.com/questions/57774180/how-to-handle-timestamp-16-in-garmin-devices
garmin_epoch = int(datetime.datetime.timestamp(datetime.datetime(1989, 12, 31, tzinfo=datetime.timezone.utc)))

interesting_columns=['active_calories', 'active_time', 'activity_type', 'auto_activity_detect_duration', 'auto_activity_detect_start_timestamp', 'calibrated_data', 'current_activity_type_intensity', 'current_day_resting_heart_rate', 
'cycles', 'cycles_to_calories', 'cycles_to_distance', 'data', 'data16', 'distance', 'duration_min', 'enabled', 'event', 'event_type', 'garmin_product', 'heart_rate', 'hr_source', 'intensity', 'local_timestamp', 
'manufacturer', 'max_met_category', 'moderate_activity_minutes', 'number', 'product', 'respiration_rate', 'resting_heart_rate', 'resting_metabolic_rate', 'serial_number', 'sleep_level', 'software_version', 
'speed_source', 'sport', 'steps', 'stress_level_value', 'sub_sport', 'time_created', 'type', 'update_time', 'version', 'vigorous_activity_minutes', 'vo2_max']

# During development I tracked message formats that I hadn't seen before looking for interesting information.
message_fields_seen=[]


def parse_fit_hr(file_path):
    hr_data = []
    
    with fitdecode.FitReader(file_path) as fit:
        last_timestamp = None
        for frame in fit:
            if frame.frame_type == fitdecode.FIT_FRAME_DATA:
                data_point = {col: '' for col in interesting_columns}
                
                field_names = [x.name for x in frame.fields]
                field_values = [x.value for x in frame.fields]
                if str(field_names) not in message_fields_seen:
                    message_fields_seen.append(str(field_names))
                    print(etime(start_time),'******* new message fields',field_names)
                    print(etime(start_time),'******* new message values',field_values)


                if frame.has_field('timestamp'):
                    last_timestamp = frame.get_value('timestamp')
                    # print('got timestamp',type(last_timestamp), last_timestamp)
                elif frame.has_field('timestamp_16') and last_timestamp is not None:
                    ts16 = frame.get_value('timestamp_16')
                    
                    # 1. Convert datetime to a numeric timestamp (seconds)
                    last_ts_numeric = int(last_timestamp.timestamp())
                    
                    # 2. Perform the 16-bit rollover calculation
                    # This finds the difference between the new 16-bit value and the old one
                    diff = (ts16 - (last_ts_numeric & 0xFFFF)) & 0xFFFF
                    
                    # 3. Add that difference back to the original datetime object
                    
                    last_timestamp = last_timestamp + datetime.timedelta(seconds=diff)
                elif frame.has_field('stress_level_time'):
                    last_timestamp = frame.get_value('stress_level_time')
                    # print('got stress timestamp',type(last_timestamp), last_timestamp)

                if last_timestamp:
                    if isinstance(last_timestamp, datetime.datetime):
                        data_point['message_date'] = last_timestamp.astimezone(my_tz)
                    else:
                        data_point['message_date'] = datetime.datetime.fromtimestamp(garmin_epoch + last_timestamp, tz=datetime.timezone.utc).astimezone(my_tz)

                interesting_found = False
                for field in frame.fields:
                    if field.name in interesting_columns:
                        data_point[field.name] = field.value
                        interesting_found = True
                
                if interesting_found:
                    hr_data.append(data_point)

    return pandas.DataFrame(hr_data)

all_hr_data=[]
for fit_file in fit_files:
    if 'activity' not in fit_file.lower():
        all_hr_data.append(parse_fit_hr(fit_file))

df=pandas.concat(all_hr_data)
df.to_csv('/data/fit/hr_data-2.csv',index=False)

