
import numpy as np

def get_team_values_df(data, team_logs, split_users=False, max_rank=10000):
        runreader = data['runreader']

        df = team_logs.get_events_dataframe()

        if(not split_users):
                df['user'] = 0
        #remove ranks bigger than max_rank
        replace_large_ranks = lambda x: np.inf if x > max_rank  else x
        df['rank_video'] = df['rank_video'].apply(replace_large_ranks)
        df['rank_shot_margin_0'] = df['rank_shot_margin_0'].apply(replace_large_ranks)
        df['rank_shot_margin_5'] = df['rank_shot_margin_5'].apply(replace_large_ranks)

        # for each (team, user, task), find the minimum ranks and the timestamps
        df=df.sort_values('timestamp')
        best_video_df = df.loc[df.groupby(['team', 'user', 'task'])['rank_video'].idxmin()]
        best_shot_df = df.loc[df.groupby(['team', 'user', 'task'])['rank_shot_margin_0'].idxmin()]
        best_shot_df_5secs = df.loc[df.groupby(['team', 'user','task'])['rank_shot_margin_5'].idxmin()]

        best_video_df = best_video_df.filter(['team', 'user', 'task', 'rank_video', 'timestamp']).rename(
                columns={'timestamp': 'timestamp_best_video'})
        best_shot_df = best_shot_df.filter(['team', 'user','task', 'rank_shot_margin_0', 'timestamp']).rename(
                columns={'timestamp': 'timestamp_best_shot'})
        best_shot_df_5secs = best_shot_df_5secs.filter(
                ['team', 'user', 'task', 'rank_shot_margin_5', 'timestamp']).rename(
                columns={'timestamp': 'timestamp_best_shot_5secs'})

        #setting best timestamp to np.inf if there is not a best video/shot
        best_video_df.loc[df['rank_video'].isin([np.inf, -np.inf]), 'timestamp_best_video'] = -1
        best_shot_df.loc[df['rank_shot_margin_0'].isin([np.inf, -np.inf]), 'timestamp_best_shot']=-1
        best_shot_df_5secs.loc[df['rank_shot_margin_5'].isin([np.inf, -np.inf]), 'timestamp_best_shot_5secs'] = -1

        df = best_video_df.merge(best_shot_df, on=['team', 'user', 'task'])
        df = df.merge(best_shot_df_5secs, on=['team', 'user','task'])

        # convert timestamps in actual seconds from the start of the task
        df['task_start'] = df['task'].apply(lambda x: runreader.tasks.get_task_from_taskname(x)['started'])
        df['time_best_video'] = (df['timestamp_best_video'] - df['task_start'])
        df['time_best_shot'] = (df['timestamp_best_shot'] - df['task_start'])
        df['time_best_shot_margin5'] = (df['timestamp_best_shot_5secs'] - df['task_start'])
        df['time_correct_submission'] = df.apply(lambda x: runreader.get_csts()[x['team']][x['task']] -
                                                           runreader.tasks.get_task_from_taskname(x['task'])[
                                                                   'started'], axis=1)
        fix_time_fun=lambda x: x/ 1000 if x > 0 else np.inf
        df['time_best_video'] = df['time_best_video'].apply(fix_time_fun)
        df['time_best_shot'] = df['time_best_shot'].apply(fix_time_fun)
        df['time_best_shot_margin5'] = df['time_best_shot_margin5'].apply(fix_time_fun)
        df['time_correct_submission'] = df['time_correct_submission'].apply(fix_time_fun)


        df = df.filter(['team', 'user', 'task', 'task_start', 'time_correct_submission', 'time_best_video', 'time_best_shot',
                        'time_best_shot_margin5', 'rank_video', 'rank_shot_margin_0', 'rank_shot_margin_5',
                        'rank_shot_margin_10'])

        df.replace([np.inf, -np.inf], -1, inplace=True)
        return df