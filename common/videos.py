import pandas as pd
import bisect
import logging
import numpy as np

class Videos:
    """
    A helper class for managing V3C videos and their shots
    """
    def __init__(self, segments_files, fps_files):
        """
        shots: pandas dataframe containing shots information
        """
        dfs = []
        for seg in segments_files:
            dfs.append(pd.read_csv(seg, dtype={'video': str}))
            
        videos = pd.concat(dfs, axis=0)
        videos = videos.groupby("video")

        # create a dict of dataframes, where the key is the videoid
        self.videos = {n:g for n,g in videos}

        # create a dict for FPSs
        fpss = []
        for ff in fps_files:
            fpss.append(pd.read_csv(ff, index_col='videoId', dtype={'videoId': str, 'FPS': float}))

        fpss = pd.concat(fpss)
        self.fps = fpss.to_dict()

    def get_shot_from_video_and_frame(self, videoId, frame, unit="frames"):
        """
        get shotId from videoId and framenumber
        unit : 'frames' | 'milliseconds'
        """

        assert unit in ["frames", "milliseconds"]
        # videoId = int(videoId) if isinstance(videoId, str) else videoId
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
        # videoId = int(videoId) if isinstance(videoId, str) else videoId
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
        # videoId = int(videoId)
        try:
            frame = int(frame)
        except ValueError:
            logging.warning(f'Found an invalid frame number in logs: {frame}. Setting to -1')
            return -1
        fps = self.fps['FPS'][videoId]
        time = frame * 1000 / fps

        # max_frames_in_msb = self.videos[videoId]['endframe'].max()
        # logging.debug('Submitted frame is {} (total number of frames of video {} is {})'.format(frame, videoId, max_frames_in_msb))
        # if time > max_time_in_msb:
        #     logging.warning('Frame {} (millisecond {}) is out of the video {} length'.format(frame, time, videoId))

        return time

    def get_shot_time_from_video_and_segment(self, videoId, segment, method="middle_frame"):
        """
        get middle frame of the given input segment
        """

        # videoId = int(videoId) if isinstance(videoId, str) else videoId
        df = self.videos[videoId]
        row = df[df['segment'] == segment]

        if method == 'middle_frame':
            # TODO: to be consistent with get_shot_time_from_video_and_frame, I use the frame numbers and then convert them to milliseconds using fixed FPS. Is this correct?
            shot_frame = (row['endframe'] + row['startframe']) / 2
        else:
            raise ValueError('Method {} not recognized!'.format(method))

        if shot_frame.empty:
            logging.warning('Video {}: segment {} not found (Max is {})'.format(videoId, segment, self.videos[videoId]['segment'].max()))
            shot_frame = np.nan
        else:
            shot_frame = shot_frame.iat[0]

        fps = self.fps['FPS'][videoId]
        shot_ms = shot_frame * 1000 / fps
        return shot_ms
