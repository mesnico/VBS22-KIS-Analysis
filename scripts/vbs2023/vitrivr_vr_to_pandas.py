from pathlib import Path
import pandas as pd
import numpy as np
import argparse

from common.load import load_competition_data

def main(args):
    # load competition data for CVHunter
    competition_data = load_competition_data(args.config)
    runreader = competition_data['runreader']

    def get_task_start(taskname):
        x = runreader.tasks.get_task_from_taskname(taskname)
        if x is None:
            return np.nan
        else:
            return x['started']
    def get_task_name(timestamp):
        x = runreader.tasks.get_task_from_timestamp(timestamp)
        if x is None:
            return np.nan
        else:
            return x['name']

    events_df = pd.read_csv(args.input_file)

    # discard AVS
    events_df = events_df[~events_df['task'].str.contains('avs')]

    events_df = events_df.rename(columns = {
        'bestCorrectRank': 'rank_shot_margin_0',
        'bestItemRank':'rank_video',
        'client_time': 'timestamp',
        'session': 'user',
        'task_time': 'elapsed_since_task_start_ms'
        })
    events_df['rank_shot_margin_5'] = events_df['rank_shot_margin_0'] # FIXME
    events_df['team'] = 'vitrivr-VR'
    events_df['additionals'] = ""
    events_df.loc[events_df['user'] == 'vitrivr-vr-florian','user'] = 0
    events_df.loc[events_df['user'] == 'vitrivr-vr-ralph','user'] = 1
    # events_df['timestamp']=events_df['timestamp']*1000
    # events_df['task'] = events_df['timestamp'].apply(get_task_name)
    # events_df = events_df[events_df['task'].notna()]
    task_start=events_df['task'].apply(get_task_start)

    #
    #task_start = events_df['task'].apply(lambda x: runreader.tasks.get_task_from_taskname(x)['started'])
    #events_df['elapsed_since_task_start_ms'] = events_df['timestamp'] - task_start

    #get correct submission time
    csts = runreader.get_csts()
    correct_submission_time = events_df['task'].apply(lambda x: csts['vitrivr-VR'][x])
    correct_submission_time = correct_submission_time.replace(-1, np.nan)


    #check if the locally recorded submission timestamps match the submission timestamps from DRES - there are some inconsistencies..we use data from DRES!
    #events_df['correct_submission_timestamp'] = correct_submission_time
    #submission_df= events_df[events_df['additionals'].str.contains('CORRECT').fillna(False)]
    #submission_df['diff']=(submission_df['correct_submission_timestamp'] - submission_df['timestamp'])/1000
    #assert  max(abs( submission_df['diff']))<=1

    events_df['correct_submission_time_ms'] = correct_submission_time - task_start
    events_df = events_df.filter(['task', 'team', 'user', 'timestamp', 'elapsed_since_task_start_ms', 'correct_submission_time_ms', 'rank_video', 'rank_shot_margin_0', 'rank_shot_margin_5', 'category', 'type', 'value','additionals' ])

    # replace -1 with infs
    events_df[['rank_shot_margin_0','rank_shot_margin_5','rank_video']] = events_df[['rank_shot_margin_0','rank_shot_margin_5','rank_video']].replace(-1, np.inf)

    # transform to 1-based ranks
    events_df[['rank_shot_margin_0','rank_shot_margin_5','rank_video']] += 1

    events_df[['type', 'value','additionals']] = events_df[['type', 'value','additionals']].replace(np.nan, "")
    events_df['max_rank'] = np.nan  # we do not know their maximum logged rank

    out_path = Path(args.output_path)
    out_path.mkdir(parents=True, exist_ok=True)
    events_df.to_csv(out_path / 'vitrivr-VR_events.csv', index=False)

    # write also an empty result dataframe (so that this output is consistent with the cache system used in the TeamLog class)
    results_df = pd.DataFrame()
    results_df.to_csv(out_path / 'vitrivr-VR_results.csv', index=False)
    print("done")
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Read already processed logs and transform in the common pandas dataframe format')
    parser.add_argument('--input_file', default='data/2023/team_logs/vitrivr-vr/result_log_ranks_vitrivr_vr.csv')
    parser.add_argument('--output_path', default='processed/team_logs/2023')
    parser.add_argument('--config', default='config_vbs2023.yaml', help='config file to generate the graph')

    args = parser.parse_args()
    main(args)

