import pandas as pd
import numpy as np

file_path = input('Enter file path: ')
columns_names = ['timestamp', 'req_id', 'event_type', 'backend_group', 'other_params']
log_df = pd.read_csv(file_path, sep='\t', names=columns_names, header=None)

# time diff between StartSendResult and FinishRequest types for requests
timedelta_data = log_df.query("event_type in ['StartSendResult', 'FinishRequest']") \
    .groupby('req_id')['timestamp'] \
    .apply(list) \
    .apply(np.diff) \
    .values

timedelta_data = np.concatenate(timedelta_data, axis=0)
timedelta_series = pd.Series(timedelta_data)
percentile_95 = round(timedelta_series.quantile(0.95), 2)

# Data Frame with count of 'BackendOk' response
event_type_data = log_df.groupby('req_id').event_type \
    .apply(list) \
    .apply(lambda x: x.count('BackendOk')) \
    .to_frame()

# Data Frame with count of backends in each request
backend_group_data = log_df.groupby('req_id').backend_group.nunique().to_frame()

# Result Data Frame
request_count_data = event_type_data.merge(backend_group_data, how='outer', on='req_id') \
    .rename(columns={'backend_group': 'group_count', 'event_type': 'OK_count'})

# New boolean column with target
request_count_data['is_all_group_ok'] = request_count_data['group_count'] == request_count_data['OK_count']
all_request_count = request_count_data.is_all_group_ok.count()
not_all_ok_request_group_count = request_count_data.query('is_all_group_ok == False').is_all_group_ok.count()


with open('output.txt', 'w') as file:
    file.write('95 percentile: {}\n'.format(percentile_95))
    file.write('bad requests: {} from {}\n'.format(not_all_ok_request_group_count, all_request_count))
print('the result is written to output.txt')
