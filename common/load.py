import json
import csv
import tqdm

import yaml
from common.logs import TeamLogs
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

def load_competition_data(config, teams_override=None):
    # load config file for this plot
    with open(config, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    # load competition data
    teams = cfg["teams"]
    competition_data = load_data(
        teams,
        cfg['audits_file'],
        cfg['run_file'],
        cfg['fps_files'],
        cfg['segment_files'])
    competition_data['config'] = cfg

    return competition_data

def process_team_logs(config, competition_data, force=False, teams_override=None):
    # create or load logs, for each team
    # load config file for this plot
    with open(config, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    logs = {}

    if teams_override and 'all' not in teams_override:
        teams = teams_override
    else:
        teams = cfg["teams"]

    for team in tqdm.tqdm(teams, desc='Loading (or generating) intermediate DataFrames'):
        team_log = TeamLogs(
            competition_data, 
            team,
            max_records=10000, 
            use_cache=True, # FIXME: refactor cache in actual output! (cache should be the wanted output)
            cache_path='processed/team_logs',
            force=force)
        logs[team] = team_log

    return logs

def load_data(
        teams,
        audits_file, 
        run_file,
        fps_files,
        v3c_segments_files=None):

    # load run file
    with open(run_file) as f:
        run = json.load(f)

    # load segments
    v3c_videos = Videos(v3c_segments_files, fps_files)

    # load run and audits file
    audit = []
    if '2021' in run_file:
        version = '2021'
        for line in open(audits_file, 'r'):
            audit.append(json.loads(line))
    elif 'vbse2022' in run_file:
        version = 'vbse2022'
        for line in open(audits_file, 'r'):
            audit.append(json.loads(line))
    elif '2022' in run_file:
        version = '2022'
        for line in open(audits_file, 'r'):
            audit.append(json.loads(line))
    elif '2023' in run_file:
        version = '2023'
        for line in open(audits_file, 'r'):
            audit_event=json.loads(line)
            if 1672831958820<audit_event['timestamp']<1673308800000:
                audit.append(audit_event)

    else:
        raise ValueError("Cannot infer the version to use to read the run file!")
    runreader = build_runreader(run, v3c_videos, teams, version=version)


    return {
        'audit': audit,
        'runreader': runreader,
        'v3c_videos': v3c_videos,
        'version': version
    }