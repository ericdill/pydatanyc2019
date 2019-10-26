import argparse
import dask.dataframe as dd
from datetime import datetime
import os
from os.path import dirname, exists
from os import listdir, makedirs
from datetime import timedelta, date
import pandas as pd

data_path = '/mnt/storage/anaconda-parsed-logs'
output_path = 's3://edill-data/cleaned-logs'
data_path = data_path.split(os.path.sep)
output_path = output_path.split(os.path.sep)


def make_folder(year, month):
    folder_path = os.path.sep.join(output_path+[year]+[month])
    if not exists(folder_path):
        makedirs(folder_path, exist_ok=True)
        
        
def package_data_processing(data_path, output_path, year, month, day):
    
    raw_data = dd.read_parquet(
        os.path.sep.join(data_path+[year]+[month]+['{0}-{1}-{2}.parquet'.format(year, month, day)]),
        categories = {'uri':500000, 'source':5})

    #create column date
    raw_data['timestamp'] = raw_data.index
    raw_data['time']= raw_data.timestamp.map(lambda t: t.replace(minute=0, second=0, microsecond=0))

    #exclude third-party channels that are not conda-forge and bioconda
    cond = raw_data.source.isin(['cloudflare_conda','cloudflare_repo_anaconda'])
    try:
        cond |= raw_data.pkg_channel.isin(['conda-forge','bioconda','pyviz'])
    except AttributeError:
        raw_data['pkg_channel'] = None
        print("Upstream dataset missing `pkg_channel` column. Caught the following error and continuing execution:")
        import traceback
        traceback.print_exc()
        
    raw_data = raw_data[cond]
    
    #create data_source column with value anaconda and conda-forge
    #raw_data['data_source'] = raw_data['pkg_channel']\
    #    .apply(lambda x: 'conda-forge' if x == 'conda-forge' else 'bioconda' if x =='bioconda' else 'anaconda', meta='str')
    raw_data['data_source'] = raw_data.pkg_channel.where(raw_data.pkg_channel.isin(['conda-forge', 'bioconda','pyviz']), 'anaconda')

    #recode pkg_python e.g., 27 -> 2.7
    raw_data['pkg_python'] = raw_data['pkg_python']\
        .apply(lambda x: float(x)/10 if len(x)>0 else '', meta='str').astype(str)
    
    #combine pkg_platform with pkg_arch
    raw_data['pkg_platform'] = raw_data['pkg_platform'].astype(str)+'-'+raw_data['pkg_arch'].astype(str)
    raw_data['pkg_platform'] = raw_data['pkg_platform']\
        .apply(lambda x: '' if x=='-' else x, meta='str').astype(str) #if platform and arch are both blank
    
    raw_data['day_name'] = raw_data.timestamp.dt.day_name()
    raw_data['hour'] = raw_data.timestamp.dt.hour
    #groupby 
    columns = ['time','data_source','pkg_name','pkg_version','pkg_platform','pkg_python', 'bytes', 'day_name', 'hour']
    data = raw_data\
        .groupby(columns)\
        .size()\
        .reset_index()

    data.columns = columns + ['counts']
    
    #save to .parquet file
    output = data.compute()
    
    for col in ['data_source','pkg_name','pkg_version','pkg_platform','pkg_python']:
        output[col]=output[col].astype('category')
        
    output_path = os.path.sep.join(output_path+[year]+[month]+['{0}-{1}-{2}.parquet'.format(year, month, day)])
    print(f'writing to {output_path')
    output.to_parquet(
        output_path,
        compression='SNAPPY',
        #file_scheme='simple',
        engine='pyarrow'
    )

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

        
def str_to_dt(datestr):
        return datetime.strptime(datestr, '%Y-%m-%d')
    
def main():
    parser = argparse.ArgumentParser()
    # data starts at 2016-10-28
    
    parser.add_argument("--start",
                        help="start date - YYYY-MM-DD",
                        type=str_to_dt,
                       )
    # data ends at 2019-09-02
    parser.add_argument("--end",
                        help="end date - YYYY-MM-DD",
                        type=str_to_dt,
                       )
    parser.add_argument("--date",
                        help="date to process - YYYY-MM-DD",
                        action="append",
                        type=str_to_dt,
                        default=[])
    
    args = parser.parse_args()
    start = args.start
    end = args.end
    if start is None and end is None:
        print("Cant do a daterange because start or end was not passed in")
        dates = []
    else:
        dates = list(daterange(args.start, args.end))
    dates.extend(args.date)
    dates = sorted(set(dates))
    print(f"processing {len(dates)} dates")                    
        
    for single_date in dates:   
        print(single_date)
        year = str(single_date.year)
        month = str(single_date.strftime('%m'))
        day = str(single_date.strftime('%d'))
    
        make_folder(year, month)
        try:
            package_data_processing(data_path, output_path, year, month, day)
        except Exception:
            import traceback
            traceback.print_exc()
            continue

if __name__ == '__main__':
    main()