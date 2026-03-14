import fitparse
import pandas
import os
import zipfile
import pprint
import datetime

garmin_epoch = int(datetime.datetime.timestamp(datetime.datetime(1989, 12, 31, tzinfo=datetime.timezone.utc)))

zip_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.zip')]

for zip_file in zip_files:
    print('extracting',zip_file)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall('/data/fit')


fit_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.fit')]

message_fields_seen=[]
interesting_columns=['message_date','heart_rate','activity_type','resting_metabolic_rate', 'activity_type', 'intensity','stress_level_value', 
                     'steps', 'active_time','activity_type','moderate_activity_minutes','vigorous_activity_minutes' ]

def parse_fit_hr(file_path):
    # Load the FIT file
    fitfile = fitparse.FitFile(file_path)
    last_timestamp = None
    # print(file_path)
    hr_data = []

    time_created=None
    for message in fitfile.messages:
        data_point={}
        for column in interesting_columns:
            data_point[column]=''

        field_names=[x.name for x in message.fields]

        if str(field_names) not in message_fields_seen:
            message_fields_seen.append(str(field_names))
            print('******* new message fields',field_names)

        heart_rate=None
        if 'timestamp' in field_names:
            # print('got timestamp',message.get_raw_value("timestamp"))
            last_timestamp = message.get_raw_value("timestamp")
            message_date = datetime.datetime.fromtimestamp(garmin_epoch + last_timestamp, tz=datetime.timezone.utc)
            data_point['message_date']=message_date
            # print('last timestamp',last_timestamp)
        elif 'timestamp_16' in field_names:
            timestamp16 = message.get_raw_value("timestamp_16")
            timestamp = last_timestamp
            timestamp += (timestamp16 - (last_timestamp & 0xFFFF)) & 0xFFFF
            message_date = datetime.datetime.fromtimestamp(garmin_epoch + timestamp, tz=datetime.timezone.utc)
            data_point['message_date']=message_date
            # print('message_date',message_date)
        elif 'stress_level_time' in field_names:
            stress_level_time = message.get_raw_value("stress_level_time")
            message_date = datetime.datetime.fromtimestamp(garmin_epoch + stress_level_time, tz=datetime.timezone.utc)
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

    df = pandas.DataFrame(hr_data)
    return df

all_hr_data=[]
for fit_file in fit_files:
    if 'activity' not in fit_file.lower():
        all_hr_data.append(parse_fit_hr(fit_file))

pandas.concat(all_hr_data).to_csv('/data/fit/hr_data.csv',index=False)


#['timestamp', 'distance', 'steps', 'active_time', 'active_calories', 'duration_min', 'activity_type']
#['cycles', 'active_time', 'active_calories', 'timestamp_16', 'activity_type', 'intensity', 'current_activity_type_intensity']
#['stress_level_time', 'stress_level_value', 'unknown_2', 'unknown_3', 'unknown_4']


# ******* new message fields ['serial_number', 'time_created', 'manufacturer', 'garmin_product', 'number', 'unknown_6', 'type']
# ******* new message fields ['timestamp', 'serial_number', 'manufacturer', 'garmin_product', 'software_version']
# ******* new message fields ['version']
# ******* new message fields ['unknown_0', 'unknown_1', 'unknown_2']
# ******* new message fields ['timestamp', 'local_timestamp', 'cycles_to_distance', 'cycles_to_calories', 'unknown_7', 'resting_metabolic_rate', 'activity_type', 'unknown_8']
# ******* new message fields ['unknown_2']
# ******* new message fields ['timestamp', 'unknown_37', 'unknown_38']
# ******* new message fields ['unknown_253', 'enabled']
# ******* new message fields ['timestamp', 'data', 'data16', 'event', 'event_type']
# ******* new message fields ['timestamp', 'activity_type', 'intensity', 'current_activity_type_intensity']
# ******* new message fields ['stress_level_time', 'stress_level_value', 'unknown_2', 'unknown_3', 'unknown_4']
# ******* new message fields ['unknown_253', 'unknown_0']
# ******* new message fields ['timestamp_16', 'heart_rate']

