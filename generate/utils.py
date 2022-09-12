
import numpy as np

def get_team_values_df(data, team, team_logs):
        runreader = data['runreader']

        df = team_logs.get_events_dataframe()

        # for each (team, task), find the minimum ranks and the timestamps
        best_video_df = df.loc[df.groupby(['team', 'task'])['rank_video'].idxmin()]
        best_shot_df = df.loc[df.groupby(['team', 'task'])['rank_shot_margin_0'].idxmin()]
        
        best_video_df = best_video_df.filter(['team', 'task', 'rank_video', 'timestamp']).rename(columns={'timestamp': 'timestamp_best_video'})
        best_shot_df = best_shot_df.filter(['team', 'task', 'rank_shot_margin_0', 'rank_shot_margin_5', 'rank_shot_margin_10', 'timestamp']).rename(columns={'timestamp': 'timestamp_best_shot'})

        df = best_video_df.merge(best_shot_df, on=['team', 'task'])

        # convert timestamps in actual seconds from the start of the task
        df['task_start'] = df['task'].apply(lambda x: runreader.tasks.get_task_from_taskname(x)['started'])
        df['time_best_video'] = (df['timestamp_best_video'] - df['task_start']) / 1000
        # df['time_best_shot'] = (df['timestamp_best_shot'] - df['task_start']) / 1000
        df['time_correct_submission'] = df.apply(lambda x: runreader.get_csts()[x['team']][x['task']] - runreader.tasks.get_task_from_taskname(x['task'])['started'], axis=1)
        df['time_correct_submission'] = df.apply(lambda x: x['time_correct_submission'] / 1000 if x['time_correct_submission'] > 0 else np.inf, axis=1)

        # df.set_index(['team', 'task'])
        df = df.filter(['team', 'task', 'time_correct_submission', 'time_best_video', 'rank_video', 'rank_shot_margin_0', 'rank_shot_margin_5', 'rank_shot_margin_10'])
        df.replace([np.inf, -np.inf], -1, inplace=True)
        return df