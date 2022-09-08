import pandas as pd
import bisect
import logging

class Videos:
    """
    A helper class for managing V3C videos and their shots
    """
    def __init__(self, v3c_segments_files, fps_file):
        """
        shots: pandas dataframe containing shots information
        """
        dfs = []
        for v3cx in v3c_segments_files:
            dfs.append(pd.read_csv(v3cx))
            
        videos = pd.concat(dfs, axis=0)
        videos = videos.groupby("video")

        # create a dict of dataframes, where the key is the videoid
        self.videos = {n:g for n,g in videos}

        # create a dict for FPSs
        fps = pd.read_csv(fps_file, names=['videoId', 'FPS'], index_col='videoId')
        self.fps = fps.to_dict()

    def get_shot_from_video_and_frame(self, videoId, frame, unit="frames"):
        """
        get shotId from videoId and framenumber
        unit : 'frames' | 'milliseconds'
        """

        assert unit in ["frames", "milliseconds"]
        videoId = int(videoId) if isinstance(videoId, str) else videoId
        try:
            frame = int(frame) if isinstance(frame, str) else frame
        except ValueError:
            logging.warning('Found an invalid frame number in logs. Setting to -1')
            frame = -1

        df = self.videos[videoId]
        start = df['startframe'] if unit == 'frames' else df['start']
        start = start.to_list()

        # efficiently search the id of the shot using binary search
        idx_after_start = bisect.bisect_right(start, frame)
        return idx_after_start

    def get_shots_from_video_and_segment(self, videoId, startpoint, endpoint, unit="milliseconds"):
        """
        get shotIds from videoId and video segment
        unit : 'frames' | 'milliseconds'
        """

        assert unit in ["frames", "milliseconds"]
        videoId = int(videoId) if isinstance(videoId, str) else videoId
        try:
            startpoint = int(startpoint) if isinstance(startpoint, str) else startpoint
        except ValueError:
            logging.warning('Found an invalid frame number in logs. Setting to -1')
            frame = -1

        df = self.videos[videoId]
        start = df['startframe'] if unit == 'frames' else df['start']
        start = start.to_list()

        # efficiently search the id of the shot using binary search

        id_shot_left = max(bisect.bisect_right(start, startpoint) - 1, 0)  # the smallest shot ID that intersect the target segment
        id_shot_right = bisect.bisect_left(start, endpoint)   # thet fisrt shot ID greather than id_shot_left that do not intersect the target segment
        correct_shots_ids=list(range(id_shot_left,id_shot_right))
        return correct_shots_ids

    def get_shot_time_from_video_and_frame(self, videoId, frame):
        # use FPS to infer the time in milliseconds from the frame
        videoId = int(videoId)
        try:
            frame = int(frame)
        except ValueError:
            logging.warning('Found an invalid frame number in logs. Setting to -1')
            return -1
        fps = self.fps['FPS'][videoId]
        time = frame * 1000 / fps

        return time