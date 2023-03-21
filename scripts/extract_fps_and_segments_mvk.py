import argparse
from datetime import datetime
import math
import os
import json
import subprocess
import pandas as pd
import numpy as np
import cv2
import logging
import tqdm
import glob

logging.basicConfig(level=logging.DEBUG)


def main(args):
    # this version uses video files directly instead of metadata

    fps_data = []
    segments_data = []

    for root, dir, files in tqdm.tqdm(os.walk(args.marine_videos_path)):
        files = [f for f in files if os.path.splitext(f)[1] == '.mp4']
        if len(files) == 0:
            continue
        for f in tqdm.tqdm(files):
            # total duration using opencv
            video = cv2.VideoCapture(os.path.join(root, f))
            fps = video.get(cv2.CAP_PROP_FPS)
            num_frames = video.get(cv2.CAP_PROP_FRAME_COUNT)
            total_duration_cv2 = num_frames / fps

            # total duration using ffprobe
            result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", os.path.join(root, f)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
            total_duration = float(result.stdout)
            if abs(total_duration - total_duration_cv2) >= 0.5:
                logging.warning(f'Video {root}/{f} may have variable FPS!')

            # double check with metadata, if existing
            metadata_file = os.path.join(root.replace('/videos', '/metadata/metadata_new'), f.replace('.mp4', '.json'))
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as jsonf:
                    sdata = json.load(jsonf)
                fps_metadata = float(sdata['fps'])
                # frame_ids = sdata['selected_frames'][0]['id']
                # total_duration = float(sdata['duration'].replace(' s', ''))
                if sdata['duration'].endswith(' s') and ':' not in sdata['duration']:
                    total_duration_metadata = float(sdata['duration'].replace(' s', ''))
                else:
                    pt = datetime.strptime(sdata['duration'].replace(' s', ''),'%H:%M:%S')
                    total_duration_metadata = pt.second + pt.minute*60 + pt.hour*3600
                if abs(fps_metadata - fps) >= 0.1:
                    logging.error(f"{metadata_file} FPS does not match! (metadata is {fps_metadata}, original is {fps})")
                if abs(total_duration_metadata - total_duration) >= 0.5:
                    logging.error(f"{metadata_file} Total duration does not match! (metadata is {total_duration_metadata}, original is {total_duration})")          
                fps = fps_metadata
                total_duration = total_duration_metadata
            else:
                logging.warning(f'Video {f} does not have metadata!')
            
            video_id = os.path.splitext(f)[0]

            # accumulate fps data
            fps_data.append({'video_id': video_id, 'fps': fps})

            # accumulate segments data (NOTE: assumed each frame is a different segment)
            num_segments = math.ceil(total_duration)
            segments_data.append(pd.DataFrame({
                'video': [video_id] * num_segments,
                'segment': list(range(1, num_segments + 1)),
                'start': [t * 1000 for t in range(num_segments)],
                'startframe': [t * fps for t in range(num_segments)],
                'end': [min(t, total_duration) * 1000 for t in range(1, num_segments + 1)],
                'endframe': [min(t, total_duration) * fps for t in range(1, num_segments + 1)],
            }).astype({'start': int, 'end': int, 'startframe': int, 'endframe': int}))

    # output fps
    fps_data = pd.DataFrame(fps_data)
    fps_data.to_csv(args.out_fps_file, index=False, header=False)
        
    # output segments
    segments_data = pd.concat(segments_data)
    segments_data.to_csv(args.out_segments_file, index=False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get frames from video')
    parser.add_argument('--marine_videos_path', type=str, default='/media/visione/data/vbs/mvk/videos', help='input directory of marine videos')
    parser.add_argument('--out_fps_file', type=str, default='data/mvk_fps.csv', help='output .csv file for FPS')
    parser.add_argument('--out_segments_file', type=str, default='data/mvk_frame_segments.csv', help='output .csv file for segments')
    args = parser.parse_args()

    main(args)