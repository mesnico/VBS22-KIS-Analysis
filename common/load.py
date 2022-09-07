import json
import csv
from common.runreaders import build_runreader

from common.videos import Videos

class Shot:
    def __init__(self, shotStart, shotEnd, segmentId):
        self.shotStart = int(shotStart)
        self.shotEnd = int(shotEnd)
        self.segmentId = int(segmentId)
        
    def isWithin(self, shotId):
        return shotId >= self.shotStart and shotId <= self.shotEnd
    
    def get_segmentId(self):
        return self.segmentId


def load_data(
        teams,
        audits_file, 
        run_file,
        fps_file,
        v3c_segments_files=None):

    # load run file
    with open(run_file) as f:
        run = json.load(f)
    
    # load audits file
    audit = []
    for line in open(audits_file, 'r'):
        audit.append(json.loads(line))

    # load v3c segments
    v3c_videos = Videos(v3c_segments_files, fps_file)

    # load the run file
    if '2021' in run_file:
        version = '2021'
    elif '2022' in run_file:
        version = '2022'
    else:
        raise ValueError("Cannot infer the version to use to read the run file!")
    runreader = build_runreader(run, v3c_videos, teams, version=version)

    return {
        'audit': audit,
        'runreader': runreader,
        'v3c_videos': v3c_videos,
        'version': version
    }