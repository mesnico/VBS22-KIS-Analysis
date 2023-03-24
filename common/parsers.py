from .videos import Videos
import numpy as np
import logging

class TeamLogParser():
    def __init__(self, data, team, v3c_videos) -> None:
        version = data['version']
        self.v3c_videos = v3c_videos
        if version == '2022' or version == '2023':
            if team.lower() == 'diveXplore'.lower():
                self.get_results = self.get_results_divexplore_2022
            elif team.lower() == 'VERGE'.lower():
                self.get_results = self.get_results_verge_2022
            elif team.lower() == 'vitrivr'.lower():
                self.get_results = self.get_results_vitrivr_2022
            elif team.lower() == 'VIREO'.lower():
                self.get_results = self.get_results_vireo_2022
            else:
                self.get_results = self.get_results_standard_2022 # if the team followed the standard, this function works just fine

            self.get_events = self.get_events_standard_2022 # if version == '2022' else None

            if version == '2022' and team.lower() == 'vitrivr'.lower():
                # patch the v3c_video to use their segments (cineast used other segments)
                self.v3c_videos = Videos(['data/v3c1_2_cineast_segments.csv'], data['config']['fps_files'])

        elif version == 'vbse2022':
            self.get_results = self.get_results_standard_2022
            self.get_events = self.get_events_standard_2022
        else:
            self.get_results = self.get_results_visione_2021
            self.get_events = self.get_events_standard_2022

    def get_results_standard_2022(self, result):
        result = result.rename(columns={'item': 'videoId'})
        result['videoId'] = result['videoId'].str.replace('GreenEggSep2021', 'GreenEgg_Sep2021')
        # result['shotId'] = result.apply(lambda x: self.v3c_videos.get_shot_from_video_and_frame(x['videoId'], x['frame'], unit='milliseconds'), axis=1)
        result['shotTimeMs'] = result.apply(lambda x: self.v3c_videos.get_shot_time_from_video_and_frame(x['videoId'], x['frame']), axis=1)
        result = result.filter(['shotTimeMs', 'shotId', 'videoId', 'rank'])
        result = result.astype({'shotTimeMs': int})
        return result

    # submitted the segment, not the frame
    def get_results_verge_2022(self, result):
        result = result.rename(columns={'item': 'videoId'})
        result['shotTimeMs'] = result.apply(lambda x: self.v3c_videos.get_shot_time_from_video_and_segment(x['videoId'], x['segment'], method='middle_frame'), axis=1)
        result = result.filter(['shotTimeMs', 'shotId', 'videoId', 'rank'])
        result = result.astype({'shotTimeMs': int})
        return result
    
    # submitted the segment, not the frame
    def get_results_vireo_2022(self, result):
        result = result.rename(columns={'video': 'videoId', 'shot': 'segment'})

        # pad ids of v3c
        result['videoId'] = result['videoId'].str.pad(width=5, fillchar='0')

        def decode_segment(x):
            segment = x['segment']
            if isinstance(segment, str) and ';' in segment:
                # format HH;MM,SS;FF. I use the frames (FF)
                frame = int(x['segment'].rsplit(';', 1)[1])
                time = self.v3c_videos.get_shot_time_from_video_and_frame(x['videoId'], frame)
            else:
                time = self.v3c_videos.get_shot_time_from_video_and_segment(x['videoId'], segment, method='middle_frame')
            return time

        result['shotTimeMs'] = result.apply(decode_segment, axis=1)
        result = result.filter(['shotTimeMs', 'shotId', 'videoId', 'rank'])
        result = result.astype({'shotTimeMs': int})
        return result

    # submitted the segment, not the frame
    def get_results_vitrivr_2022(self, result):
        result = result.rename(columns={'item': 'videoId'})

        def decode_segment(x):
            if isinstance(x['videoId'], str) and '_' in x['videoId']:
                # format HH;MM,SS;FF. Use the frames (FF)
                frame = x['segment']    # in the segment there is the frame
                time = self.v3c_videos.get_shot_time_from_video_and_frame(x['videoId'], frame)
            else:
                time = self.v3c_videos.get_shot_time_from_video_and_segment(x['videoId'], x['segment'], method='middle_frame')
            return time

        result['videoId'] = result['videoId'].apply(lambda x: x[2:].replace('GreenEggSep2021', 'GreenEgg_Sep2021')) # remove leading "v_" and possibly replace GreenEggSep2021        
        result['shotTimeMs'] = result.apply(decode_segment, axis=1)
        result = result.filter(['shotTimeMs', 'shotId', 'videoId', 'rank'])
        result = result.astype({'shotTimeMs': int})
        return result

    def get_results_divexplore_2022(self, result):
        # TODO: check if there are results in which frame is not present but segment is present instead.

        result = result.rename(columns={'item': 'videoId'})
        result['videoId'] = result['videoId'].apply(lambda x: x.replace('v_', '')) # remove leading "v_"
        if 'frame' not in result.columns:
            logging.warning('Found no "frame" information inside the results data. Setting to nan')
            result['shotTimeMs'] = np.nan
        else:
            result['shotTimeMs'] = result.apply(lambda x: self.v3c_videos.get_shot_time_from_video_and_frame(x['videoId'], x['frame']), axis=1)
        result['videoId'] = result['videoId'].astype(int)
        result = result.filter(['shotTimeMs', 'shotId', 'videoId', 'rank'])
        return result

    def get_results_visione_2021(self, result):
        result = result.rename(columns={'frame': 'shotId', 'item': 'videoId'})
        result = result.filter(['shotId', 'videoId', 'rank'])
        return result

    def get_events_standard_2022(self, events):
        events = events.filter(['timestamp', 'category', 'type', 'value'])
        return events