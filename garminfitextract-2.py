import fitdecode
import pandas
import os
import zipfile
import pprint
import datetime
import pytz 

zip_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.zip')]

for zip_file in zip_files:
    # print('extracting',zip_file)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall('/data/fit')


fit_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.fit')]

interesting_columns=['active_calories', 'active_time', 'activity_type', 'auto_activity_detect_duration', 'auto_activity_detect_start_timestamp', 'calibrated_data', 'current_activity_type_intensity', 'current_day_resting_heart_rate', 
'cycles', 'cycles_to_calories', 'cycles_to_distance', 'data', 'data16', 'distance', 'duration_min', 'enabled', 'event', 'event_type', 'garmin_product', 'heart_rate', 'hr_source', 'intensity', 'local_timestamp', 
'manufacturer', 'max_met_category', 'moderate_activity_minutes', 'number', 'product', 'respiration_rate', 'resting_heart_rate', 'resting_metabolic_rate', 'serial_number', 'sleep_level', 'software_version', 
'speed_source', 'sport', 'steps', 'stress_level_value', 'sub_sport', 'time_created', 'type', 'update_time', 'version', 'vigorous_activity_minutes', 'vo2_max']

def parse_fit_hr(file_path):

    with fitdecode.FitReader(file_path) as fit:
        for frame in fit:
            data_point={}
            for column in interesting_columns:
                data_point[column]=''
            
            if frame.frame_type == fitdecode.FIT_FRAME_DATA:
                if 'unknown' not in frame.name:
                    print(frame.name)
                    print([(x.name,x.value) for x in frame.fields if 'unknown' not in x.name])
                # for field in frame.fields:
                #     print(f"  {field.name}: {field.value}") 

                field_names=[x.name for x in frame.fields]
                
                if 'timestamp' in field_names:
                    # print('got timestamp',message.get_raw_value("timestamp"))
                    last_timestamp = message.get_raw_value("timestamp")
                    message_date = datetime.datetime.fromtimestamp(garmin_epoch + last_timestamp, tz=datetime.timezone.utc).astimezone(my_tz)
                    data_point['message_date']=message_date
                    # print('last timestamp',last_timestamp)
                elif 'timestamp_16' in field_names:
                    timestamp16 = message.get_raw_value("timestamp_16")
                    timestamp = last_timestamp
                    timestamp += (timestamp16 - (last_timestamp & 0xFFFF)) & 0xFFFF
                    message_date = datetime.datetime.fromtimestamp(garmin_epoch + timestamp, tz=datetime.timezone.utc).astimezone(my_tz)
                    data_point['message_date']=message_date
                    # print('message_date',message_date)
                elif 'stress_level_time' in field_names:
                    stress_level_time = message.get_raw_value("stress_level_time")
                    message_date = datetime.datetime.fromtimestamp(garmin_epoch + stress_level_time, tz=datetime.timezone.utc).astimezone(my_tz)
                    data_point['message_date']=message_date
                    # print('message_date',message_date)
                interesting_message=False
                for data in message:
                    for column in interesting_columns:
                        if column in data.name:
                            data_point[column]=data.value
                            interesting_message=True
                if interesting_message==True:
                    hr_data.append(data_point)


    return pandas.DataFrame()

all_hr_data=[]
for fit_file in fit_files:
    if 'activity' not in fit_file.lower():
        all_hr_data.append(parse_fit_hr(fit_file))

df=pandas.concat(all_hr_data)
df.to_csv('/data/fit/hr_data-2.csv',index=False)

