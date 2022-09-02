
import numpy as np
import pandas as pd
from utils.task import Task
from utils.team import TeamLogs
import tqdm

def get_team_values_df(data, team):
        run, v3c_videos = data['run'], data['v3c_videos']

        # collect team log
        team_logs = TeamLogs(data, team)
        tasks = []
        teamId = ''

        team_meta = data['teams_metadata'][team]
        teamName = team_meta['name_in_logs']
        rank_zero = team_meta['rank_zero']
        
        for t in run['description']['teams']:
            if t['name'] == teamName:
                teamId = t['uid']
                break

        for t in run['tasks']:
            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                task = Task(t['started'], t['ended'], t['duration'], t['position'], t['uid'], t['description']['taskType']['name'])
                task.add_name(t['description']['name'])
                cst = -1
                for s in t['submissions']:
                    if s['status'] == 'CORRECT' and s['teamId'] == teamId:
                        cst = s['timestamp']
                        break
                task.add_correct_submission_time(cst)
                videoId = t['description']['target']['item']['name']
                timeshot = int(t['description']['target']['temporalRange']['start']['value'] * 1000)
                shotId = v3c_videos.get_shot_from_video_and_frame(videoId, timeshot, unit='milliseconds')

                # sorted_list = list(map(int, v3c_ids[videoId].keys()))
                # next_key = 0
                # for k in sorted_list:
                #     iter_key = next_key
                #     next_key = k
                #     if timeshot >= iter_key and timeshot < next_key:
                #         shotId = v3c_ids[videoId][str(iter_key)]
                #         break
                task.add_correct_shot_and_video(shotId, videoId)
                tasks.append(task)

        # for each task, accumulate the statistics for this team
        for task in tqdm.tqdm(tasks, desc='Accumulating statistics for {}'.format(team)):
            df = team_logs.filter_by_timestep(task.started, task.ended)
            df['adj_logged_time'] = task.get_logged_time(df['timestamp'])
            task.add_new_ranking(df)            

        # prepare the output dataframe
        calc_dict = dict()
        calc_dict['task'] = []
        calc_dict['team'] = team
        calc_dict['r_s'] = []
        calc_dict['r_v'] = []
        calc_dict['t'] = []
        calc_dict['t_cs'] = []

        for task in tasks:
            name = task.get_name()
            r_s, r_v, t, t_cs = task.get_rel_info(rank_zero)
            calc_dict['task'].append(name)
            calc_dict['r_s'].append(r_s)
            calc_dict['r_v'].append(r_v)
            calc_dict['t'].append(t)
            calc_dict['t_cs'].append(t_cs)
        df = pd.DataFrame(calc_dict)
        df.set_index(['team', 'task'])
        df.replace([np.inf, -np.inf], -1, inplace=True)
        return df