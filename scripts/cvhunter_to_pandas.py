from pathlib import Path
import pandas as pd
import numpy as np
import argparse
import sys
sys.path.append(".")

from common.load import load_data

def main(args):
    # load competition data for CVHunter
    competition_data = load_data(
        ['CVHunter'],
        args.audits_file, 
        args.run_file, 
        args.v3c_fps_file,
        args.v3c_segments_files)
    runreader = competition_data['runreader']

    events_df = pd.read_csv(args.input_file)

    events_df = events_df.rename(columns = {
        'name': 'task',
        'Rank': 'rank_shot_margin_0',
        'Rank GT+2x5s': 'rank_shot_margin_5',
        'VideoRank':'rank_video',
        'ts': 'timestamp',
        'query': 'value',
        'operator': 'user',
        'filter': 'additionals'
        })
    events_df['team'] = 'CVHunter'
    events_df.loc[events_df['user'] == 'LP','user'] = 0
    events_df.loc[events_df['user'] == 'JL','user'] = 1
    events_df['timestamp']=events_df['timestamp']*1000
    task_start = events_df['task'].apply(lambda x: runreader.tasks.get_task_from_taskname(x)['started'])
    events_df['elapsed_since_task_start_ms'] = events_df['timestamp'] - task_start

    csts = runreader.get_csts()
    correct_submission_time = events_df['task'].apply(lambda x: csts['CVHunter'][x])
    correct_submission_time = correct_submission_time.replace(-1, np.nan)
    events_df['correct_submission_time_ms'] = correct_submission_time - task_start
    events_df = events_df.filter(['task', 'team', 'user','timestamp', 'elapsed_since_task_start_ms', 'correct_submission_time_ms', 'rank_video', 'rank_shot_margin_0', 'rank_shot_margin_5', 'category', 'type', 'value','additionals' ])

    events_df[['rank_shot_margin_0','rank_shot_margin_5','rank_video']] = events_df[['rank_shot_margin_0','rank_shot_margin_5','rank_video']].replace(np.nan, np.inf)
    events_df[['type', 'value','additionals']] = events_df[['type', 'value','additionals']].replace(np.nan, "")

    
    out_path = Path(args.output_path)
    out_path.mkdir(parents=True, exist_ok=True)
    events_df.to_pickle(out_path / 'CVHunter_events.pkl')

    # write also an empty result dataframe (so that this output is consistent with the cache system used in the TeamLog class)
    results_df = pd.DataFrame()
    results_df.to_pickle(out_path / 'CVHunter_results.pkl')
    print("done")
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Read already processed logs and transform in the common pandas dataframe format')
    parser.add_argument('--input_file', default='data/2022/team_logs/CVHunter/CVHunter_filtered_data.csv')
    parser.add_argument('--output_path', default='cache/team_logs/2022')
    parser.add_argument('--audits_file', default='data/2022/audits.json')
    parser.add_argument('--run_file', default='data/2022/run.json')
    parser.add_argument('--v3c_segments_files', nargs='+', default=['data/v3c1_frame_segments.csv', 'data/v3c2_frame_segments.csv'])
    parser.add_argument('--v3c_fps_file', default='data/v3c1_2_fps.csv')

    args = parser.parse_args()
    main(args)

