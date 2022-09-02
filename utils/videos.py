import pandas as pd
import bisect

class Videos:
    """
    A helper class for managing V3C videos and their shots
    """
    def __init__(self, v3c_segments_files):
        """
        shots: pandas dataframe containing shots information
        """
        videos = pd.read_csv(v3c_segments_files[0]) # TODO: extend with V3C2
        videos = videos.groupby("video")

        # create a dict of dataframes, where the key is the videoid
        self.videos = {n:g for n,g in videos}

    def get_shot_from_video_and_frame(self, videoId, frame, unit="frames"):
        """
        get shotId from videoId and framenumber
        unit : 'frames' | 'milliseconds'
        """
        assert unit in ["frames", "milliseconds"]
        videoId = int(videoId) if isinstance(videoId, str) else videoId
        frame = int(frame) if isinstance(frame, str) else frame

        df = self.videos[videoId]
        start = df['startframe'] if unit == 'frames' else df['start']
        start = start.to_list()

        # efficiently search the id of the shot using binary search
        idx_after_start = bisect.bisect_right(start, frame)
        return idx_after_start