import json
import csv

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
    # for seg_file in v3c_segments_files:    
    #     with open(seg_file, mode='r') as infile:
    #         reader = csv.reader(infile)
    #         is_header = True
    #         for row in reader:
    #             if is_header:
    #                 is_header = False
    #                 continue
    #             if row[0] not in v3c_ids_shots:
    #                 v3c_ids_shots[row[0]] = []
    #             v3c_ids_shots[row[0]].append(Shot(row[3], row[5], row[1]))

    #             if row[0] not in v3c_ids:
    #                 v3c_ids[row[0]] = dict()
    #             v3c_ids[row[0]][row[2]] = row[1]

    # load teams metadata
    with open(teams_metadata_file) as f:
        teams_metadata = json.load(f)

    return {
        'audit': audit, 
        'run': run,
        'v3c_videos': v3c_videos,
        'teams_metadata': teams_metadata
    }