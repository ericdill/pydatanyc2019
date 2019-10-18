import argparse
import dask.dataframe as dd
from datetime import datetime
import os
from os.path import dirname, exists
from os import listdir, makedirs
from datetime import timedelta, date
import pandas as pd

data_path = '/mnt/bigdisk/parsed_data/merged/packages'
output_path = '/home/shooper/notebooks/anaconda-package-data/conda/hourly'
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
    raw_data = raw_data[
        (raw_data.source.isin(['cloudflare_conda','cloudflare_repo_anaconda']))|
        (raw_data.pkg_channel.isin(['conda-forge','bioconda','pyviz']))]

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

    #groupby
    data = raw_data\
        .groupby(['time','data_source','pkg_name','pkg_version','pkg_platform','pkg_python'])\
        .size()\
        .reset_index()

    data.columns = ['time','data_source','pkg_name','pkg_version','pkg_platform','pkg_python','counts']

    #save to .parquet file
    output = data.compute()

    for col in ['data_source','pkg_name','pkg_version','pkg_platform','pkg_python']:
        output[col]=output[col].astype('category')


    output.to_parquet(
        os.path.sep.join(output_path+[year]+[month]+['{0}-{1}-{2}.parquet'.format(year, month, day)]),
        compression='SNAPPY',
        file_scheme='simple')

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date",
                        help="start date - YYYY-MM-DD",
                        type=lambda d: datetime.strptime(d, '%Y-%m-%d')
                       )
    parser.add_argument("end_date",
                        help="end date - YYYY-MM-DD",
                        type=lambda d: datetime.strptime(d, '%Y-%m-%d')
                       )
    args = parser.parse_args()

    for single_date in daterange(args.start_date, args.end_date):
        print(single_date)
        year = str(single_date.year)
        month = str(single_date.strftime('%m'))
        day = str(single_date.strftime('%d'))

        make_folder(year, month)
        package_data_processing(data_path, output_path, year, month, day)

if __name__ == '__main__':
    main()

