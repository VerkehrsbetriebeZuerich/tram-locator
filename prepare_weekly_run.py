import os
import datetime
import requests
import pandas as pd
from zipfile import ZipFile
import shutil
import pathlib
from distutils.dir_util import copy_tree

# script sould be run every wednesday on a weekly basis to get the latest version of the static timetable
# the timetable is available after 9am MEZ, but consider a later time to not overwhelm the server. 


def load_new_files_from_server(new_gtfs_path):
    today = datetime.datetime.now()
    offset = (today.weekday() - 2) % 7

    # new data always releases every wednesday
    last_wednesday = today - datetime.timedelta(days=offset)
    last_wednesday_str = last_wednesday.strftime("%Y-%m-%d")

    # EDIT HERE
    # today_str = '2022-03-30'  # only for test and specifig dates
    url = r'https://opentransportdata.swiss/dataset/timetable-2022-gtfs2020/resource_permalink/gtfs_fp2022_' + last_wednesday_str + r'_04-15.zip'
    new_gtfs_file = os.path.join(new_gtfs_path, last_wednesday_str + '.zip')

    r = requests.get(url)
    # print(r.status_code)
    if r.status_code == 404:
        print('No file {} on server found'.format(url))
    elif r.status_code == 200:
        print('File found for today: {}'.format(last_wednesday_str))
        with open(new_gtfs_file, 'wb') as f:
            f.write(r.content)
        print('Unzip files...')
        with ZipFile(new_gtfs_file, 'r') as zipObj:
            zipObj.extractall(path=new_gtfs_mod_path)
            print('All files unpacked')


def get_selected_trip_ids(agency_id):
    df_routs = pd.read_csv(os.path.join(new_gtfs_mod_path, 'routes.txt'),
                           index_col='route_id')
    # print(len(df_routs))
    df = df_routs.loc[(df_routs['agency_id'].isin(agency_id)) & (df_routs['route_desc'] == 'T')]
    tram_route_ids = list(df.index)
    # print(len(tram_route_ids))

    df_trips = pd.read_csv(os.path.join(new_gtfs_mod_path, 'trips.txt'), index_col='trip_id')
    df_trips_selected = df_trips.where(df_trips['route_id'].isin(tram_route_ids)).dropna()
    df_trips_selected
    selected_trip_ids = list(df_trips_selected.index)
    print('Found {} unique and relevant TripIDs'.format(len(selected_trip_ids)))
    #     print(len(trip_ids))
    return selected_trip_ids


def extract_selected_trip_ids(selected_trip_ids):
    print('Start loading stop times file')
    df_stop_times = pd.read_csv(os.path.join(new_gtfs_mod_path, 'stop_times.txt'))
    # df_stop_times.info()
    print('Stop times file loading finished')
    df_stop_times_selected = df_stop_times.where(df_stop_times['trip_id'].isin(selected_trip_ids)).dropna()
    # df_stop_times_selected.info()

    print('Saving Selectet stop times')
    df_stop_times_selected.to_csv(os.path.join(new_gtfs_mod_path, 'selected_stop_times.csv'), index=False)

    print('Removing old files')
    os.remove(os.path.join(new_gtfs_mod_path, 'stop_times.txt'))
    print('Saved all files local')


def copy_files_to_live_gtfs(live_gtfs_path):
    print('Start copy files to live gtfs')
    if not os.path.exists(live_gtfs_path):
        os.makedirs(live_gtfs_path)

    copy_tree(new_gtfs_mod_path, live_gtfs_path)
    print('copy to live gtfs finished')

if __name__ == '__main__':

    # specifie a path for cache and live data
    cwd_path = os.getcwd()
    new_gtfs_path = os.path.join(cwd_path, 'cached_gtfs')
    live_gtfs_path = os.path.join(cwd_path, 'live_gtfs')
    if not os.path.exists(new_gtfs_path):
        os.makedirs(new_gtfs_path)
    new_gtfs_mod_path = os.path.join(new_gtfs_path, 'mod_gtfs')
    load_new_files_from_server(new_gtfs_path)

    # here we strip the huge dataset of data not relevant to the selected agency_id
    # We only want data from the two VBZ agency_ids
    selected_trip_ids = get_selected_trip_ids(agency_id=['849', '3849'])
    extract_selected_trip_ids(selected_trip_ids)
    copy_files_to_live_gtfs(live_gtfs_path)
