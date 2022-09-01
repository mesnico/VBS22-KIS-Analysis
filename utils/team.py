from collections import OrderedDict
import pandas as pd
import os
import tqdm
import json

from utils.task import TaskCount


class TeamLogs:
    def __init__(self, data, team):
        self.v3c_ids_shots = data['v3c_ids_shots']
        self.df = self.get_data(data, team)

    def get_data(self, data, team):
        """
        retrieve all the data
        """
        dfs = []
        team_log = data['teams_metadata'][team]['log_path']

        for root, _, files in os.walk(team_log):
            for file in tqdm.tqdm(files):
                path = os.path.join(root, file)
                if file == '.DS_Store':
                    continue
                with open(path) as f:
                    ranked_list = json.load(f)

                    # assume that every team has the timestamp in the filename
                    timestamp = int(os.path.splitext(file)[0])

                    # do the magic and grab relevant infos from different team log files
                    infos = self._get_relevant_info(team, ranked_list['results'])

                    d = {'team': team, 'timestamp': timestamp}
                    d.update(infos)
                    df = pd.DataFrame(d)
                    dfs.append(df)

        # prepare the final dataframe
        final_df = pd.concat(dfs, axis=0).reset_index()
        return final_df

    def filter_by_timestep(self, start_timestep, end_timestep):
        return self.df[self.df['timestamp'].between(start_timestep, end_timestep)].copy()

    def _get_relevant_info(self, team, results):
        """
        retrieves shotId, videoId, ranks or other info from the logs of the given team
        """

        shotIds = []
        videoIds = []
        ranks = []

        for index, result in enumerate(results):
            if team=='vitrivr' or team=='divexplore':
                videoId = result['item'][2:]
            else:
                videoId = result['item']
            
            if team=='visione':
                shotId = result['frame']
            elif team=='somHunter' or team=='viret' or team=='diveXplore':
                for segment in self.v3c_ids_shots[videoId]:
                    if segment.isWithin(int(result['frame'])):
                        shotId = segment.get_segmentId()
                        break
            else:
                shotId = result['segment']

            if result['rank'] is None:
                rank = 0
            else:
                rank = result['rank']

            if team=='somhunter':
                rank = index

            shotIds.append(shotId)
            videoIds.append(videoId)
            ranks.append(rank)

        return {
            "shotId": shotIds, 
            "videoId": videoIds, 
            "rank": ranks
        }


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