import json
import csv
from utils.runreaders import build_runreader

from utils.videos import Videos

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
        teams_metadata_file, 
        v3c_segments_files=None):

    # load run file
    with open(run_file) as f:
        run = json.load(f)
    
    # load audits file
    audit = []
    for line in open(audits_file, 'r'):
        audit.append(json.loads(line))

    # load v3c segments
    v3c_videos = Videos(v3c_segments_files)

    # load team metadata
    with open(teams_metadata_file) as f:
        teams_metadata = json.load(f)

    runreader = build_runreader(run, v3c_videos, teams_metadata, teams)

    return {
        'audit': audit,
        'runreader': runreader,
        'v3c_videos': v3c_videos,
        'teams_metadata': teams_metadata
    }