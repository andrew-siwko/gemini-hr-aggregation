import fitparse
import pandas
import os
import zipfile

zip_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.zip')]

for zip_file in zip_files:
    print('extracting',zip_file)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall('/data/fit')


fit_files=['/data/fit/'+x for x in os.listdir('/data/fit') if x.endswith('.fit')]

def parse_fit_hr(file_path):
    # Load the FIT file
    fitfile = fitparse.FitFile(file_path)
    print(file_path)
    hr_data = []

    time_created=None
    for message in fitfile.messages:
        heart_rate=None
        timestamp_16=None
        for data in message:
            if 'heart_rate' in data.name:
                heart_rate=data.value
            if 'timestamp_16' in data.name:
                timestamp_16=data.value
            if 'time_created' in data.name:
                print(f"  {message.name}: {data.name}: {data.value}")
                time_created=data.value
        if timestamp_16 and heart_rate:
            data_point={'file_created':time_created,'timestamp_16':timestamp_16,'heart_rate':heart_rate}
            hr_data.append(data_point)
    df = pandas.DataFrame(hr_data)
    return df

all_hr_data=[]
for fit_file in fit_files:
    if 'activity' not in fit_file.lower():
        all_hr_data.append(parse_fit_hr(fit_file))

pandas.concat(all_hr_data).to_csv('/data/fit/hr_data.csv',index=False)