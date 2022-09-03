
import numpy as np
import pandas as pd
from utils.runreaders import RunReader2021
from utils.task import Task, TaskResult
from utils.team import TeamLogs
import tqdm

def get_team_values_df(data, team, team_logs):
        runreader = data['runreader']

        team_meta = data['teams_metadata'][team]
        teamName = team_meta['name_in_logs']
        rank_zero = team_meta['rank_zero']

        tasks = runreader.get_tasks()
        csts = runreader.get_correct_submission_times()
        task_results = []

        # for each task, accumulate the statistics for this team
        for task in tqdm.tqdm(tasks, desc='Accumulating statistics for {}'.format(team)):
            task_result = TaskResult(team, task, csts)
            # df = team_logs.filter_by_timestep(task.started, task.ended)
            df = team_logs.filter_by_task_name(task.get_name())
            df['adj_logged_time'] = task.get_logged_time(df['timestamp'])
            task_result.add_new_ranking(df)
            task_results.append(task_result)

        # prepare the output dataframe
        calc_dict = dict()
        calc_dict['task'] = []
        calc_dict['team'] = team
        calc_dict['r_s'] = []
        calc_dict['r_v'] = []
        calc_dict['t'] = []
        calc_dict['t_cs'] = []

        for task_result in task_results:
            name = task_result.get_task_name()
            r_s, r_v, t, t_cs = task_result.get_rel_info(rank_zero)
            calc_dict['task'].append(name)
            calc_dict['r_s'].append(r_s)
            calc_dict['r_v'].append(r_v)
            calc_dict['t'].append(t)
            calc_dict['t_cs'].append(t_cs)
        df = pd.DataFrame(calc_dict)
        df.set_index(['team', 'task'])
        df.replace([np.inf, -np.inf], -1, inplace=True)
        return df