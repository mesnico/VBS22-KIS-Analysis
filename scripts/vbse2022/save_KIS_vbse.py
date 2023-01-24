
import argparse
import os
import os.path
import json
import pandas as pd
from datetime import datetime
import pickle

from common.load import load_data

def main(args):
    # load competition data
    competition_data = load_data(
        [''],
        args.audits_file,
        args.run_file,
        args.v3c_fps_file,
        args.v3c_segments_files)

    ## getting info from the run file
    runreader = competition_data['runreader']  # RunReader2022
    run = runreader.run  # original run file
    vbsRunID = run['id']['string']  # RunId of the official VBS 2022
    tasks = runreader.tasks.tasks_df  # it is used to check if a timestamp is inside a KIS task


 #   rename_fun = lambda x: x.replace('vbs22-kis-t', 'T_').replace('vbs22-kis-v', 'V_')
  #  tasks['name'] = tasks['name'].apply(rename_fun)
    tname = tasks[['name', 'started']].sort_values(by=['started'])['name'].unique()
   # textual = [t for t in tname if t.startswith('T')]  # used later to order the column
    #tasks=tasks[tasks['name'].isin(textual)]
    tasks=tasks[['name','correct_video', 'hints']]
    dic=[]
    for index, row in tasks.iterrows():
        dic.append({'task': row['name'], 'hint': row['hints'][1]['text'], 'correct_video': row['correct_video'] })

    df=pd.DataFrame(dic)


    filename = f"{args.output_folder}/KIS.csv"
    with open(filename, 'w') as fp:
        df.to_csv(filename)



if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Read Logs (query events + results) only for KIS tasks from raw DRES files')
        parser.add_argument('--output_folder', default='../data/vbse2022/')
        parser.add_argument('--audits_file', default='../data/vbse2022/audits.jsonl')
        parser.add_argument('--run_file', default='../data/vbse2022/run.json')
        parser.add_argument('--v3c_fps_file', default='../data/v3c1_2_fps.csv')
        parser.add_argument('--v3c_segments_files', nargs='+',
                            default=['../data/v3c1_frame_segments.csv', '../data/v3c2_frame_segments.csv'])

        args = parser.parse_args()
        main(args)

