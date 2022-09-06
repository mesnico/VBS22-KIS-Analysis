import argparse
from pathlib import Path
import pandas as pd
import os
import tqdm

def main(args):
    v3cx_folder = Path(args.root) / args.dataset.upper() / 'msb'
    dfs = []
    for msb_file in tqdm.tqdm(os.listdir(v3cx_folder)):
        video_id = os.path.splitext(msb_file)[0] # the video_id is the filename without extension
        msb_file = v3cx_folder / msb_file 
        df = pd.read_csv(msb_file, sep='\t')

        df = df.reset_index().rename(columns={'index': 'segment'})
        df['video'] = video_id

        dfs.append(df)

    df = pd.concat(dfs, axis=0)

    # rename columns 
    df = df.rename(columns={'starttime': 'start', 'endtime': 'end'})

    df['segment'] = df['segment'] + 1   # segment id is one-based

    # from seconds to milliseconds
    df['start'] = (df['start'] * 1000).astype(int)
    df['end'] = (df['end'] * 1000).astype(int)

    out_file = Path(args.output_path) / '{}_frame_segments.csv'.format(args.dataset)
    df.to_csv(out_file, index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform evaluation on test set')
    parser.add_argument('--root', default='data/V3C_dataset', help='root of the folder containing msb files')
    parser.add_argument('--dataset', default='v3c2', choices=['v3c1', 'v3c2'], help='from which V3C split I want to create frame segments .csv file')
    parser.add_argument('--output_path', default='data', help='where to store the final .csv file')

    args = parser.parse_args()
    main(args)