
import bisect
from common.tasks import Tasks
import numpy as np

from common.teams import Teams


def build_runreader(run, v3c_videos, teams, version='2022'):
    if version == '2021':    # TODO (condition based on run that discriminates between 2021 and 2022)
        runreader = RunReader2021(run, v3c_videos, teams)
    elif version == '2022':
        runreader = RunReader2022(run, v3c_videos, teams)
    else:
        raise ValueError("Runreader version {} not recognized!".format(version))
    return runreader


class RunReader:
    def __init__(self, run, v3c_videos, teams) -> None:
        self.run = run
        self.v3c_videos = v3c_videos
        self.teams = teams

        # collects informations about tasks and teams
        self.tasks = self.build_tasks()
        self.teams = self.build_teams()

        # collects correct_submission_time from every team
        self.csts = self.build_correct_submission_times()

    def build_correct_submission_times(self):
        """
        for every task, remember which was the correct submission time from each team
        """
        return NotImplementedError

    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video and shot ids
        """
        return NotImplementedError

    def build_teams(self):
        """
        Mine teams from the run file
        """
        return NotImplementedError

    def get_csts(self):
        return self.csts

    def get_tasks(self):
        return self.tasks
    
    def get_teams(self):
        return self.teams


class RunReader2021(RunReader):
    def __init__(self, run, v3c_videos, teams):
        super().__init__(run, v3c_videos, teams)

    def get_teamid_from_teamname(self, team_name):
        for t in self.run['description']['teams']:
            if t['name'] == team_name:
                teamId = t['uid']
                break

        return teamId

    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video and shot ids
        """
        tasks = {}

        for t in self.run['tasks']:
            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                task = Task(t['started'], t['ended'], t['duration'], t['position'], t['uid'], t['description']['taskType']['name'])
                task.add_name(t['description']['name'])

                videoId = t['description']['target']['item']['name']
                timeshot = int(t['description']['target']['temporalRange']['start']['value'] * 1000)
                shotId = self.v3c_videos.get_shot_from_video_and_frame(videoId, timeshot, unit='milliseconds')

                task.add_correct_shot_and_video(shotId, videoId)
                tasks[task.name] = task

        return tasks

    def build_correct_submission_times(self):
        """
        for every task, remember which was the correct submission time from each team
        """
        team_ids = set()
        id_to_tname = {}
        csts = {}

        # collect team informations
        for tname in self.teams:
            team_log_name = self.teams_metadata[tname]["name_in_logs"]
            teamId = self.get_teamid_from_teamname(team_log_name)
            id_to_tname[teamId] = tname
            team_ids.add(teamId)
            csts[tname] = {}

        for t in self.run['tasks']:
            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                taskname = t['description']['name']
                remaining_team_ids = set(team_ids)
                for s in t['submissions']:
                    if s['status'] == 'CORRECT' and s['teamId'] in remaining_team_ids:
                        csts[id_to_tname[s['teamId']]][taskname] = s['timestamp']
                        remaining_team_ids.remove(s['teamId'])
                        if len(remaining_team_ids) == 0:
                            break

                # the remaining teams have not found the correct result, put -1
                for r in remaining_team_ids:
                    csts[id_to_tname[r]][taskname] = -1

        return csts


class RunReader2022(RunReader):
    def __init__(self, run, v3c_videos, teams):
        super().__init__(run, v3c_videos, teams)

    # def get_teamid_from_teamname(self, team_name):
    #     for t in self.run['description']['teams']:
    #         if t['name'] == team_name:
    #             teamId = t['uid']['string']
    #             break

    #     return teamId

    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video, the predefined shot ids, the target shot start and end in milliseconds #
        """
        tasks = Tasks(self.v3c_videos)

        for t in self.run['tasks']:
            if(t['ended'] - t['started'] <= 1000): 
                # an invalid task, discard it
                continue

            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                # assert task.name not in tasks

                videoId = t['description']['target']['item']['name']
                target_start_ms = int(t['description']['target']['temporalRange']['start']['millisecond'])  # start time of TARGET video segment
                target_end_ms = int(t['description']['target']['temporalRange']['end']['millisecond']) # end time of TARGET video segment
                # shotId = self.v3c_videos.get_shot_from_video_and_frame(videoId, target_start_ms, unit='milliseconds')
                #TODO: what happens if the tagert video segment overlaps between two predefined shots, for example, if target_end_ms falls into the shot (shotID+1)? Perhaps the shotID should be a list of the predefined shots that intersect the target video segment. We should check it
                # task.add_correct_shot_and_video(shotId, videoId)
                # task.add_correct_shot_start_and_end_milliseconds(target_start_ms,target_end_ms)
                # tasks[task.name] = task
                tasks.add_task(
                    t['description']['name'], 
                    t['started'], 
                    t['ended'], 
                    t['duration'], 
                    t['position'], 
                    t['uid']['string'], 
                    t['description']['taskType']['name'],
                    videoId,
                    target_start_ms,
                    target_end_ms
                )
        return tasks

    def build_teams(self):
        teams = Teams()
        for team in self.run['description']['teams']:
            name = team['name']
            uid = team['uid']['string']
            teams.add_team(name, uid)

        return teams
        
    def build_correct_submission_times(self):
        """
        for every task, remember which was the correct submission time from each team
        """
        team_ids = self.teams.get_team_ids()
        csts = {k: {} for k in self.teams.get_team_names()}

        for t in self.run['tasks']:
            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                taskname = t['description']['name']
                remaining_team_ids = set(team_ids)
                for s in t['submissions']:
                    if s['status'] == 'CORRECT' and s['teamId']['string'] in remaining_team_ids:
                        csts[self.teams.get_teamname_from_id(s['teamId']['string'])][taskname] = s['timestamp']
                        remaining_team_ids.remove(s['teamId']['string'])
                        if len(remaining_team_ids) == 0:
                            break

                # the remaining teams have not found the correct result, put -1
                for r in remaining_team_ids:
                    csts[self.teams.get_teamname_from_id(r)][taskname] = -1
        return csts