import logging
import pandas as pd
import numpy as np
import logging
import datetime
from google.transit import gtfs_realtime_pb2
import time
import os
import requests

APIKEY = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
INTERVAL_S = 60.0


def request_from_server():
    r = requests.get('https://api.opentransportdata.swiss/gtfsrt2020', headers={'Authorization': APIKEY})
    
    if bytes('disallowed', 'utf-8') in r.content:
        print('ERROR: Please check API key. Server returned {}'.format(r.content))
        exit()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    return feed

def get_todays_service_ids():
    df_calendar = pd.read_csv(os.path.join(os.getcwd(), 'live_gtfs', 'calendar.txt'),
                              index_col='service_id')
    now = datetime.datetime.now()
    today_int = int(now.strftime("%Y%m%d"))
    weekday_str = now.strftime('%A').lower()
    #     print(today_int, weekday_str)

    df_calendar_today = df_calendar.where((df_calendar[weekday_str] == 1) &
                                          (today_int > df_calendar.start_date) &
                                          (today_int < df_calendar.end_date)).dropna()

    df_calendar_dates = pd.read_csv(
        os.path.join(os.getcwd(), 'live_gtfs', 'calendar_dates.txt'),
        index_col='service_id')
    df_calendar_dates_today_remove = df_calendar_dates.where((df_calendar_dates.date == today_int) &
                                                             (df_calendar_dates.exception_type == 2)).dropna().index
    df_calendar_dates_today_added = df_calendar_dates.where((df_calendar_dates.date == today_int) &
                                                            (df_calendar_dates.exception_type == 1)).dropna().index

    service_ids = set(df_calendar_today.index)
    #     print('Today from calendar.txt {}'.format(len(service_ids)))
    #     print('Today removed by calendar_dates {}'.format(len(df_calendar_dates_today_remove)))
    #     print('Today added by calendar_dates {}'.format(len(df_calendar_dates_today_added)))
    service_ids.update(set(df_calendar_dates_today_added))
    service_ids = service_ids.difference(set(df_calendar_dates_today_remove))
    #     print('Return service_ids for today {}'.format(len(service_ids)))
    return list(service_ids)

def get_stop_df():
    df_stops = pd.read_csv(os.path.join(os.getcwd(), 'live_gtfs', 'stops.txt'))
    df_stops.index = pd.to_numeric(df_stops['stop_id'], errors='coerce')
    return df_stops

def get_todays_trip_ids(service_ids):
    df_routs = pd.read_csv(os.path.join(os.getcwd(), 'live_gtfs', 'routes.txt'),
                           index_col='route_id')
    search_values = ['849', '3849']
    df = df_routs.loc[(df_routs['agency_id'].isin(search_values)) & (df_routs['route_desc'] == 'T')]
    tram_route_ids = list(df.index)
    # print('DEBUG: tram_route_ids {}'.format(len(tram_route_ids)))

    df_trips = pd.read_csv(os.path.join(os.getcwd(), 'live_gtfs', 'trips.txt'),
                           index_col='trip_id')
    # print('DEBUG: df_trips {}'.format(len(df_trips)))
    # print(df_trips.info())
    df_trips_today = df_trips.where(
        (df_trips['route_id'].isin(tram_route_ids)) & (df_trips['service_id'].isin(service_ids))).dropna()
    # print('DEBUG: df_trips_today {}'.format(len(df_trips_today)))
    # df_trips_today
    trip_ids = list(df_trips_today.index)
    # print('DEBUG: trip_ids {}'.format(len(trip_ids)))
    #     print(len(trip_ids))
    return trip_ids

def get_todays_stop_times(trip_ids):
    df_stop_times = pd.read_csv(os.path.join(os.getcwd(), 'live_gtfs', 'selected_stop_times.csv'))
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")

    df_stop_times = df_stop_times.where((df_stop_times['trip_id'].isin(trip_ids))).dropna()

    df_stop_times['departure_time'] = pd.to_datetime(date_str+' '+df_stop_times['departure_time'],
                                                     format= '%Y-%m-%d %H:%M:%S', errors='coerce')
    df_stop_times['arrival_time'] = pd.to_datetime(date_str+' '+df_stop_times['arrival_time'],
                                                   format= '%Y-%m-%d %H:%M:%S', errors='coerce')
    df_stop_times['departure_timestamp'] = (df_stop_times['departure_time'] - datetime.datetime(1970,1,1)).dt.total_seconds()
    df_stop_times['arrival_timestamp'] = (df_stop_times['arrival_time'] - datetime.datetime(1970,1,1)).dt.total_seconds()
    return df_stop_times

def get_modified_tramtrip_dict(feed):
    start = time.time()
    modified_tramtrip_dict = {}

    for entity in feed.entity[0:-1]:
      if entity.HasField('trip_update'):
    #     print (entity.trip_update.trip.trip_id)
        if entity.trip_update.trip.trip_id in trip_ids:
            trip_id = entity.trip_update.trip.trip_id
            tram_nr = trip_id.split('-')[1]
    #         print('Tram '+tram_nr)
    #         print (entity.trip_update)
            df = pd.DataFrame()
    #         print (trip_id, df_trips.loc[trip_id, 'trip_headsign'], df_trips.loc[trip_id, 'trip_short_name'])
            for sequence in entity.trip_update.stop_time_update:
                stop_id = float(sequence.stop_id)
                df.at[stop_id, 'unscheduled_trip'] = entity.trip_update.trip.schedule_relationship
                df.at[stop_id, 'stop_sequence'] = sequence.stop_sequence
                df.at[stop_id, 'stop_skiped'] = sequence.schedule_relationship
                try:
                    df.at[stop_id, 'arrival_delay'] = sequence.arrival.delay
                    df.at[stop_id, 'departure_delay'] = sequence.departure.delay
                except Exception as e:
                    logging.error(e)
            modified_tramtrip_dict[trip_id] = df
#     print('Dynamic currently running {}'.format(len(modified_tramtrip_dict.keys())))
    stop = time.time()
    # print('Took {}s'.format(stop-start))
    return modified_tramtrip_dict

def merge_static_and_dynamic():
    start = time.time()
    now = datetime.datetime.now()
    now_epoch = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
    time_window_s = 60 * 5
    df = df_stop_times.loc[(now_epoch - df_stop_times['departure_timestamp']).abs() < time_window_s]
    current_trip_ids = df['trip_id'].unique()
    trip_ids_to_mod = list(set(current_trip_ids).intersection(modified_tramtrip_dict.keys()))
    pd.options.mode.chained_assignment = None  # default='warn'
    current_trips_dict = {}
    df = df_stop_times[df_stop_times['trip_id'].isin(current_trip_ids)]
    df.set_index('trip_id', inplace=True)

    df['stop_name'] = df_stops.loc[df.stop_id, 'stop_name'].values

    for trip_id in trip_ids_to_mod:
        #     print('-------------------')
        #     print(trip_id)
        #     modified_tramtrip_dict[trip_id]
        for stop_id in modified_tramtrip_dict[trip_id].index:
            index = (df.index == trip_id) & (df.stop_id == stop_id)

            departure_delay = modified_tramtrip_dict[trip_id].loc[stop_id]['departure_delay']
            df.loc[index, 'departure_delay'] = departure_delay
            departure_time = df.loc[index, 'departure_time']
            df.loc[index, 'departure_time'] = departure_time + datetime.timedelta(seconds=departure_delay)

            arrival_delay = modified_tramtrip_dict[trip_id].loc[stop_id]['arrival_delay']
            df.loc[index, 'arrival_delay'] = arrival_delay
            arrival_time = df.loc[index, 'arrival_time']
            df.loc[index, 'arrival_time'] = arrival_time + datetime.timedelta(seconds=arrival_delay)

            stop_skiped = modified_tramtrip_dict[trip_id].loc[stop_id]['stop_skiped']
            df.loc[index, 'stop_skiped'] = stop_skiped

    print('Static currently running {}'.format(len(current_trip_ids)))
    print('Dynamic currently running {}'.format(len(modified_tramtrip_dict.keys())))
    print('In both {}'.format(len(trip_ids_to_mod)))
    stop = time.time()
    # print('Took {}s'.format(stop - start))
    return df

def export_current_status(df, silent=True):
    start = time.time()
    now = datetime.datetime.now()
    df_export = pd.DataFrame()
    # stop_id_dict = collections.OrderedDict()
    now_epoch = (datetime.datetime.now()-datetime.datetime(1970,1,1)).total_seconds()
    for trip_id in df.index.unique():
#         print('----------------------------------')
#         print(trip_id)
        df_one = df[df.index.isin([trip_id])]
        df_one.set_index('stop_id', inplace = True)
#         print(df_one)
        tramnr= trip_id.split('-')[1]
        at_station_id = (now_epoch - df_one['departure_timestamp']).abs().idxmin()
    #     print(at_station_id)
        at_station_name = df_stops.loc[at_station_id, 'stop_name']
        at_station_dtime = df_one.loc[at_station_id, 'departure_time']

        try:
            if not np.isnan(df_one.loc[at_station_id, 'departure_delay']):
                delay_min = (df_one.loc[at_station_id, 'departure_delay']) / 60.0
            else:
                delay_min = 0.0
        except:
            delay_min = 0.0

        df_export.at[trip_id, 'at_station_id'] = at_station_id
        df_export.at[trip_id, 'at_station_name'] = at_station_name
        df_export.at[trip_id, 'tram_nr'] = tramnr
        df_export.at[trip_id, 'at_epoch'] = at_station_dtime
        df_export.at[trip_id, 'at_station_dtime'] = at_station_dtime.strftime("%H:%M")
        df_export.at[trip_id, 'destination_name'] = df_one.iloc[-1]['stop_name']
        df_export.at[trip_id, 'delay'] = delay_min
        if not silent:
            if delay_min != 0.0:
                print('Tram {} at station {} depating at {} ({}min delay)'.format(
                    tramnr, at_station_name, at_station_dtime.strftime("%H:%M"), delay_min))
            else:
                print('Tram {} at station {} depating at {}'.format(
                    tramnr, at_station_name, at_station_dtime.strftime("%H:%M")))
    print('Exported {} vehicles'.format(len(df_export)))
    df_export.to_csv('current_status.csv')

    stop = time.time()
    # print('Took {}s'.format(stop-start))

if __name__ == '__main__':
    
    last_plot_time = 0.0


    print('Start preloading...')

    service_ids = get_todays_service_ids()
    print('Found {} Service IDs for today'.format(len(service_ids)))
    df_stops = get_stop_df()
    print('Found {} Stops for today'.format(len(df_stops)))
    trip_ids = get_todays_trip_ids(service_ids)
    print('Found {} Trip IDs for today'.format(len(trip_ids)))
    df_stop_times = get_todays_stop_times(trip_ids)
    print('Found {} Stop Times for today'.format(len(df_stop_times)))

    print('Preload completed')

    time.sleep(5)
    # repeat this stuff

    while True:
        if (time.time() - last_plot_time > INTERVAL_S):
            print('------------------------------------------')
            last_plot_time = time.time()
            # analyse modified data
            print('Load data from server...')
            feed = request_from_server()
            print('Analyse data from server...')
            modified_tramtrip_dict = get_modified_tramtrip_dict(feed)
            print(len(modified_tramtrip_dict))
            df_merged = merge_static_and_dynamic()
            # print(len(df_merged))
            export_current_status(df_merged, silent=True)
            now = time.time()
            print('Took {}s'.format(now-last_plot_time))

        else:
            time.sleep(10.0)
