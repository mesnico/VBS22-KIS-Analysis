from collections import OrderedDict
from pathlib import Path
import pandas as pd
import os
import tqdm
import json
import numpy as np

from utils.task import TaskCount


class TeamLogs:
    def __init__(self, data, team, max_records=10000, use_cache=False, cache_path='cache/logs'):
        self.v3c_videos = data['v3c_videos']
        self.runreader = data['runreader']
        self.get_info_fn_dict = {
            'visione': self.get_infos_visione,
            'viret': self.get_infos_viret
        }

        # some caching logic
        cache_path = Path(cache_path)
        if not cache_path.exists():
            cache_path.mkdir(parents=True, exist_ok=True)
        cache_file = cache_path / '{}.pkl'.format(team)
        if use_cache and cache_file.exists():
            self.df = pd.read_pickle(cache_file)
        else:
            self.df = self.get_data(data, team, max_records)
            self.df.to_pickle(cache_file)

    def get_infos_visione(self, result):
        shotId = result['frame']
        videoId = result['item']
        if result['rank'] is None:
            rank = 0
        else:
            rank = result['rank']
        return shotId, videoId, rank

    def get_infos_viret(self, result):
        videoId = result['item']
        shotId = self.v3c_videos.get_shot_from_video_and_frame(videoId, result['frame'], unit='frames')
        videoId = videoId
        if result['rank'] is None:
            rank = 0
        else:
            rank = result['rank']
        return shotId, videoId, rank

    def get_data(self, data, team, max_records):
        """
        retrieve all the data
        """
        dfs = []
        team_log = data['teams_metadata'][team]['log_path']

        user_idx = 0
        for root, _, files in os.walk(team_log):
            for file in tqdm.tqdm(files, desc="Reading {} logs".format(team)):
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
                    infos = self.get_teams_info(ranked_list['results'], self.get_info_fn_dict[team], max_records)

                    d = {'user': user_idx, 'task': task_name, 'team': team, 'timestamp': timestamp}
                    d.update(infos)
                    df = pd.DataFrame(d)
                    dfs.append(df)

                    user_idx += 1

        # prepare the final dataframe
        final_df = pd.concat(dfs, axis=0).reset_index()

        # sort by timestamp, important for filter_by_timestamp
        # final_df = final_df.sort_values(by=['timestamp'])

        return final_df

    def get_teams_info(self, results, info_fn, max_records):
        shotIds = np.zeros(len(results), dtype=int)
        videoIds = np.zeros(len(results), dtype=int)
        ranks = np.zeros(len(results), dtype=int)

        for index, result in enumerate(results[:max_records]):
            shotId, videoId, rank = info_fn(result)
            assert rank <= max_records

            shotIds[index] = shotId
            videoIds[index] = videoId
            ranks[index] = rank

        return {
            "videoId": videoIds,
            "shotId": shotIds,
            "rank": ranks
        }

    def filter_by_timestep(self, start_timestep, end_timestep):
        # easy but expensive solution
        t1 = self.df[self.df['timestamp'].between(start_timestep, end_timestep)].copy()
        
        # efficient implementation using bisect
        # timestamps = self.df['timestamp'].to_list()
        # start_idx = bisect.bisect_right(timestamps, start_timestep)
        # end_idx = bisect.bisect_right(timestamps, end_timestep)
        # t2 = self.df.iloc[start_idx:end_idx].copy()
        return t1

    def filter_by_task_name(self, task_name):
        t1 = self.df[self.df['task'] == task_name].copy()
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