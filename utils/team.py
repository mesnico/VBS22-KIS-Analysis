from collections import OrderedDict
from gc import get_referents
from pathlib import Path
import pandas as pd
import os
import tqdm
import json
import numpy as np

from utils.task import TaskCount

class TeamLogParser():
    def __init__(self, version, team, v3c_videos) -> None:
        self.version = version
        self.v3c_videos = v3c_videos
        if team == 'visione':
            self.get_results = self.get_results_visione_2022 if version == '2022' else self.get_results_visione_2021
            self.get_events = self.get_events_visione

    def get_results_visione_2022(self, result):
        result = result.rename(columns={'item': 'videoId'})
        result['shotId'] = result.apply(lambda x: self.v3c_videos.get_shot_from_video_and_frame(x['videoId'], x['frame'], unit='milliseconds'), axis=1)
        result = result.filter(['shotId', 'videoId', 'rank'])
        result = result.astype(int)
        return result

    def get_results_visione_2021(self, result):
        result = result.rename(columns={'frame': 'shotId', 'item': 'videoId'})
        result = result.filter(['shotId', 'videoId', 'rank'])
        result = result.astype(int)
        return result

    def get_events_visione(self, events):
        events = events.filter(['timestamp', 'category', 'type', 'value'])
        return events
        
    

class TeamLogs:
    def __init__(self, data, team, max_records=10000, use_cache=False, cache_path='cache/team_logs'):
        self.v3c_videos = data['v3c_videos']
        self.runreader = data['runreader']

        # some caching logic for results
        cache_path = Path(cache_path) / data['version'] # append the version to the cache_path
        if not cache_path.exists():
            cache_path.mkdir(parents=True, exist_ok=True)
        results_cache_file = cache_path / '{}_results.pkl'.format(team)
        events_cache_file = cache_path / '{}_events.pkl'.format(team)
        if use_cache and (results_cache_file.exists() and events_cache_file.exists()):
            self.df_results = pd.read_pickle(results_cache_file)
            self.df_events = pd.read_pickle(events_cache_file)
        else:
            self.df_results, self.df_events = self.get_data(data, team, max_records)
            self.df_results.to_pickle(results_cache_file)
            self.df_events.to_pickle(events_cache_file)

    def get_data(self, data, team, max_records):
        """
        retrieve all the data
        """
        results_dfs = []
        events_dfs = []
        team_log = data['teams_metadata'][team]['log_path']

        user_idx = 0
        log_parser = TeamLogParser(data['version'], team, self.v3c_videos)
        for root, _, files in os.walk(team_log):
            for file in tqdm.tqdm(files, desc="Processing {} logs".format(team)):
                path = os.path.join(root, file)
                if file == '.DS_Store':
                    continue
                with open(path) as f:
                    ranked_list = json.load(f)

                    # assume that every team has the timestamp in the filename
                    timestamp = int(os.path.splitext(file)[0])

                    # retrieve the task we are in at the moment
                    task_name = self.runreader.get_taskname_from_timestamp(timestamp)
                    if task_name is None:
                        # the logs outside task ranges are not important for us
                        continue

                    # do the magic and grab relevant infos from different team log files
                    if len(ranked_list['results']) > 0:
                        results_df = self.get_teams_results(ranked_list['results'], log_parser.get_results, max_records)
                        results_df['timestamp'] = timestamp # note that in events the timestamp should be already present
                        results_df['user'] = user_idx
                        results_df['task'] = task_name
                        results_df['team'] = team
                        results_dfs.append(results_df)

                    if len(ranked_list['events']) > 0:
                        events_df = self.get_teams_events(ranked_list['events'], log_parser.get_events)
                        events_df['user'] = user_idx
                        events_df['task'] = task_name
                        events_df['team'] = team
                        events_dfs.append(events_df)

            if Path(root) != Path(team_log):
                user_idx += 1   # number of user is the number of folders

        assert user_idx <= 2

        # prepare the final dataframe
        results_df = pd.concat(results_dfs, axis=0).reset_index()
        events_df = pd.concat(events_dfs, axis=0).reset_index()

        # sort by timestamp, important for filter_by_timestamp
        # final_df = final_df.sort_values(by=['timestamp'])

        return results_df, events_df

    def get_teams_events(self, events, events_fn):
        # categories = []
        # types = []
        # values = []

        # for index, event in enumerate(events):
        #     category, typ, value = events_fn(event)
        events = pd.DataFrame(events)
        return events_fn(events)

    def get_teams_results(self, results, results_fn, max_records):
        results = results[:max_records]
        results = pd.DataFrame(results)
        return results_fn(results)
        # shotIds = np.zeros(len(results), dtype=int)
        # videoIds = np.zeros(len(results), dtype=int)
        # ranks = np.zeros(len(results), dtype=int)

        # for index, result in enumerate(results[:max_records]):
        #     shotId, videoId, rank = results_fn(result)
        #     assert rank <= max_records

        #     shotIds[index] = shotId
        #     videoIds[index] = videoId
        #     ranks[index] = rank

        # return {
        #     "videoId": videoIds,
        #     "shotId": shotIds,
        #     "rank": ranks
        # }

    def filter_by_timestep(self, start_timestep, end_timestep):
        # easy but expensive solution
        t1 = self.df_results[self.df_results['timestamp'].between(start_timestep, end_timestep)].copy()
        
        # efficient implementation using bisect
        # timestamps = self.df['timestamp'].to_list()
        # start_idx = bisect.bisect_right(timestamps, start_timestep)
        # end_idx = bisect.bisect_right(timestamps, end_timestep)
        # t2 = self.df.iloc[start_idx:end_idx].copy()
        return t1

    def filter_by_task_name(self, task_name):
        t1 = self.df_results[self.df_results['task'] == task_name].copy()
        return t1


class Team:
    
    def __init__(self, teamId, name):
        self.teamId = teamId
        self.name = name
        self.tasksDicts = dict()
        self.tasksDicts['KIS-Visual'] = dict()
        self.tasksDicts['KIS-Textual'] = dict()
        self.tasksDicts['AVS'] = dict()
        self.avsTasksList = dict()
        
    def get_name(self):
        return self.name
        
    def add_avs_task(self, task, index):
        self.avsTasksList[index] = task
        
    def get_avs_task(self, index):
        return self.avsTasksList[index]
    
    def add_avs_submission(self, index, memberId, status, teamId, uid, timestamp, itemName):
        self.avsTasksList[index].add_submission(memberId, status, teamId, uid, timestamp, itemName)
    
    def add_task(self, position, status, task_type):
        if position in self.tasksDicts[task_type]:
            self.tasksDicts[task_type][position].add_status(status)
        else:
            tc = TaskCount()
            tc.add_status(status)
            self.tasksDicts[task_type][position] = tc
            
    def to_df(self):
        data_dict = OrderedDict()
        total_sum = 0
        sumup = 0
        for key in sorted(self.tasksDicts['KIS-Textual']):
            value = self.tasksDicts['KIS-Textual'][key].get_incorrect()
            str_val = str(value)
            if value < 0:
                value = 0
            sumup += value
            if str_val == '-1':
                str_val = ' '
            if str_val == '0':
                str_val = '-1'
            data_dict['T_' + str(key)] = str_val
        data_dict['Sigma1'] = sumup
        total_sum += sumup
        sumup = 0
        for key in sorted(self.tasksDicts['KIS-Visual']):
            value = self.tasksDicts['KIS-Visual'][key].get_incorrect()
            str_val = str(value)
            if value < 0:
                value = 0
            sumup += value
            if str_val == '-1':
                str_val = ' '
            if str_val == '0':
                str_val = '-1'
            data_dict['V_' + str(key)] = str_val
        data_dict['Sigma2'] = sumup
        total_sum += sumup
        sumup = 0
        for key in sorted(self.tasksDicts['AVS']):
            value = self.tasksDicts['AVS'][key].get_incorrect()
            str_val = str(value)
            if value < 0:
                value = 0
            sumup += value
            if str_val == '-1':
                str_val = ' '
            if str_val == '0':
                str_val = '-1'
            data_dict['A_' + str(key)] = str_val
        data_dict['Sigma3'] = sumup
        total_sum += sumup
        data_dict['Sigma4'] = total_sum
        df = pd.DataFrame(data=data_dict, index=[self.name])
        return df