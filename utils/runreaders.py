
import bisect
from utils.task import Task
import numpy as np


def build_runreader(run, v3c_videos, teams_metadata, teams):
    if True:    # TODO (condition based on run that discriminates between 2021 and 2022)
        runreader = RunReader2021(run, v3c_videos, teams_metadata, teams)
    else:
        runreader = RunReader2022(run, v3c_videos, teams_metadata, teams)
    return runreader


class RunReader2021:
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
        for t in self.run['description']['teams']:
            if t['name'] == team_name:
                teamId = t['uid']
                break

        return teamId

    def get_tasks(self):
        return self.tasks

    def get_correct_submission_times(self):
        return self.csts

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


    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video and shot ids
        """
        tasks = []

        for t in self.run['tasks']:
            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                task = Task(t['started'], t['ended'], t['duration'], t['position'], t['uid'], t['description']['taskType']['name'])
                task.add_name(t['description']['name'])
                # cst = -1
                # for s in t['submissions']:
                #     if s['status'] == 'CORRECT' and s['teamId'] == teamId:
                #         cst = s['timestamp']
                #         break
                # task.add_correct_submission_time(cst)
                videoId = t['description']['target']['item']['name']
                timeshot = int(t['description']['target']['temporalRange']['start']['value'] * 1000)
                shotId = self.v3c_videos.get_shot_from_video_and_frame(videoId, timeshot, unit='milliseconds')

                task.add_correct_shot_and_video(shotId, videoId)
                tasks.append(task)

        return tasks

    def get_taskname_from_timestamp(self, timestamp):
        task_names, starts, ends = zip(*[(t.get_name(), t.started, t.ended) for t in self.tasks])
        sorting_idxs = np.array(starts).argsort()
        starts = np.array(starts)[sorting_idxs]
        ends = np.array(ends)[sorting_idxs]
        if timestamp < starts[0] or timestamp > ends[-1]:
            # outside limits, not a valid timestamp
            return None
        
        f_start_idx = bisect.bisect_right(starts.tolist(), timestamp) - 1
        f_end_idx = bisect.bisect_right(ends.tolist(), timestamp)

        assert f_end_idx == f_start_idx
        
        correct_task_idx = task_names[sorting_idxs[f_start_idx]]
        return correct_task_idx
        

        # intervals = [{'name': t.get_name(), 'start': t.start, 'end': t.end} for t in self.tasks()]
        # intervals = pd.DataFrame(intervals)
