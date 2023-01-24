import pandas as pd
import numpy as np
import bisect

class Tasks:
    """
    Store all the necessary information to a task
    """

    def __init__(self, v3c_videos):
        self.v3c_videos = v3c_videos
        self.tasks_df = pd.DataFrame()

    def add_task_vbs2022(self, name, started, ended, duration, position, uid, taskType, correct_video, target_start_ms, target_end_ms, submissions=[]):
        correct_shot = self.v3c_videos.get_shot_from_video_and_frame(correct_video, target_start_ms, unit='milliseconds')
        correct_shots = self.v3c_videos.get_shots_from_video_and_segment(correct_video, target_start_ms,target_end_ms,unit='milliseconds')
        #TODO substitute correct_shot with correct_shots (that is a list)
        self.tasks_df = self.tasks_df.append({
            'name': name,
            'started': started,
            'ended': ended,
            'duration': duration,
            'position': position,
            'uid': uid,
            'task_type': taskType,
            'correct_video': correct_video,
            'correct_shot': correct_shot,
            'target_start_ms': target_start_ms,
            'target_end_ms': target_end_ms ,
            'submissions':submissions}, ignore_index=True
        )
        self.tasks_df['correct_video'] = self.tasks_df['correct_video'].astype(int)

    def add_task_vbse2022(self, name, started, ended, duration, position, uid, taskType, correct_video, fps, target_start_ms,
                 target_end_ms, submissions=[], hints=[]):
        correct_shot = self.v3c_videos.get_shot_from_video_and_frame(correct_video, target_start_ms,
                                                                     unit='milliseconds')
        correct_shots = self.v3c_videos.get_shots_from_video_and_segment(correct_video, target_start_ms, target_end_ms,
                                                                         unit='milliseconds')
        # TODO substitute correct_shot with correct_shots (that is a list)
        self.tasks_df = self.tasks_df.append({
            'name': name,
            'started': started,
            'ended': ended,
            'duration': duration,
            'position': position,
            'uid': uid,
            'task_type': taskType,
            'correct_video': correct_video,
            'fps':fps,
            'correct_shot': correct_shot,
            'target_start_ms': target_start_ms,
            'target_end_ms': target_end_ms,
            'submissions': submissions,
            'hints':hints}, ignore_index=True
        )
        self.tasks_df['correct_video'] = self.tasks_df['correct_video'].astype(int)
    def add_task_vbs2023(self, name, started, ended, duration, position, uid, taskType, correct_video, target_start_ms, target_end_ms, submissions=[]):
        self.tasks_df = self.tasks_df.append({
            'name': name,
            'started': started,
            'ended': ended,
            'duration': duration,
            'position': position,
            'uid': uid,
            'task_type': taskType,
            'correct_video': correct_video,
            'target_start_ms': target_start_ms,
            'target_end_ms': target_end_ms ,
            'submissions':submissions}, ignore_index=True
        )
        self.tasks_df['correct_video'] = self.tasks_df['correct_video']

    def get_task_from_timestamp(self, timestamp):
        names, starts, ends = zip(*self.tasks_df[['name', 'started', 'ended']].values.tolist())
        sorting_idxs = np.array(starts).argsort()
        starts = np.array(starts)[sorting_idxs]
        ends = np.array(ends)[sorting_idxs]
        if timestamp < starts[0] or timestamp > ends[-1]:
            # outside limits, not a valid timestamp
            return None
        
        f_start_idx = bisect.bisect_right(starts.tolist(), timestamp) - 1
        f_end_idx = bisect.bisect_right(ends.tolist(), timestamp)

        if f_end_idx != f_start_idx:
            # in the middle between two tasks, so not a valid submission
            return None
        
        correct_name = names[sorting_idxs[f_start_idx]]
        correct_task = self.tasks_df[self.tasks_df['name'] == correct_name]
        return correct_task.iloc[0].to_dict()

    def get_task_from_taskname(self, name):
        return self.tasks_df[self.tasks_df['name'] == name].iloc[0].to_dict()

    # def get_logged_time(self, logged_time):
    #     return (logged_time - self.started) / 1000
        
    # def get_name(self):
    #     return self.name
    
    # def add_correct_shot_and_video(self, shotId, videoId):
    #     self.correct_video = int(videoId)
    #     self.correct_shot = int(shotId)

    # def add_correct_shot_start_and_end_milliseconds(self, target_start_ms, target_end_ms): ##
    #     self.target_start_ms = target_start_ms
    #     self.target_end_ms = target_end_ms