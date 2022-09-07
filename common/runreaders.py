
import bisect
from common.task import Task
import numpy as np


def build_runreader(run, v3c_videos, teams_metadata, teams, version='2022'):
    if version == '2021':    # TODO (condition based on run that discriminates between 2021 and 2022)
        runreader = RunReader2021(run, v3c_videos, teams_metadata, teams)
    elif version == '2022':
        runreader = RunReader2022(run, v3c_videos, teams_metadata, teams)
    else:
        raise ValueError("Runreader version {} not recognized!".format(version))
    return runreader


class RunReader:
    def __init__(self, run, v3c_videos, teams_metadata, teams) -> None:
        self.run = run
        self.v3c_videos = v3c_videos
        self.teams_metadata = teams_metadata
        self.teams = teams

        # collects informations about single tasks
        self.tasks = self.build_tasks()

        # collects correct_submission_time from every team
        self.csts = self.build_correct_submission_times()


    def get_teamid_from_teamname(self, team_name):
        return NotImplementedError

    def get_task_from_taskname(self, task_name):
        return self.tasks[task_name]

    def get_tasks(self):
        return list(self.tasks.values())

    def get_correct_submission_times(self):
        return self.csts

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

    def get_task_from_timestamp(self, timestamp):
        names, starts, ends = zip(*[(n, t.started, t.ended) for n, t in self.tasks.items()])
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
        correct_task = self.tasks[correct_name]
        return correct_task


class RunReader2021(RunReader):
    def __init__(self, run, v3c_videos, teams_metadata, teams):
        super().__init__(run, v3c_videos, teams_metadata, teams)

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
    def __init__(self, run, v3c_videos, teams_metadata, teams):
        super().__init__(run, v3c_videos, teams_metadata, teams)

    def get_teamid_from_teamname(self, team_name):
        for t in self.run['description']['teams']:
            if t['name'] == team_name:
                teamId = t['uid']['string']
                break

        return teamId

    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video, the predefined shot ids, the target shot start and end in milliseconds #
        """
        tasks = {}

        for t in self.run['tasks']:
            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                task = Task(t['started'], t['ended'], t['duration'], t['position'], t['uid']['string'], t['description']['taskType']['name'])
                task.add_name(t['description']['name'])

                videoId = t['description']['target']['item']['name']
                target_start_ms = int(t['description']['target']['temporalRange']['start']['millisecond'])  # start time of TARGET video segment
                target_end_ms = int(t['description']['target']['temporalRange']['end']['millisecond']) # end time of TARGET video segment
                shotId = self.v3c_videos.get_shot_from_video_and_frame(videoId, target_start_ms, unit='milliseconds')
                #TODO: what happens if the tagert video segment overlaps between two predefined shots, for example, if target_end_ms falls into the shot (shotID+1)? Perhaps the shotID should be a list of the predefined shots that intersect the target video segment. We should check it
                task.add_correct_shot_and_video(shotId, videoId)
                task.add_correct_shot_start_and_end_milliseconds(target_start_ms,target_end_ms)
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
                    if s['status'] == 'CORRECT' and s['teamId']['string'] in remaining_team_ids:
                        csts[id_to_tname[s['teamId']['string']]][taskname] = s['timestamp']
                        remaining_team_ids.remove(s['teamId']['string'])
                        if len(remaining_team_ids) == 0:
                            break

                # the remaining teams have not found the correct result, put -1
                for r in remaining_team_ids:
                    csts[id_to_tname[r]][taskname] = -1

        return csts
