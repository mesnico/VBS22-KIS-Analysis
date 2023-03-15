
import bisect
from common.tasks import Tasks
import numpy as np
from copy import deepcopy

from common.teams import Teams


def build_runreader(run, v3c_videos, teams, version='2022'):
    if version == '2021':    # TODO (condition based on run that discriminates between 2021 and 2022)
        runreader = RunReader2021(run, v3c_videos, teams)
    elif version == '2022':
        runreader = RunReader2022(run, v3c_videos, teams)
    elif version == 'vbse2022':
        runreader = RunReaderVbse2022(run, v3c_videos, teams)
    elif version == '2023':
        runreader = RunReader2023(run, v3c_videos, teams)
    else:
        raise ValueError("Runreader version {} not recognized!".format(version))
    return runreader


class RunReader:
    def __init__(self, run, v3c_videos, teams) -> None:
        self.run = run
        self.v3c_videos = v3c_videos
        self.teams = teams

        # collects information about tasks and teams
        self.teams = self.build_teams()
        # collect task info and correct_submission_time from every team
        self.tasks, self.csts  = self.build_tasks()



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

    def build_teams(self):
        teams = Teams()
        for team in self.run['description']['teams']:
            name = team['name']
            uid = team['uid']['string']
            teams.add_team(name, uid)

        return teams

    # def get_teamid_from_teamname(self, team_name):
    #     for t in self.run['description']['teams']:
    #         if t['name'] == team_name:
    #             teamId = t['uid']
    #             break
    #
    #     return teamId

    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video, the predefined shot ids, the target shot start and end in milliseconds #
       """
        team_ids = self.teams.get_team_ids()
        tasks = {}
        csts = {k: {} for k in self.teams.get_team_names()} #csts = {}
        tasks = Tasks(self.v3c_videos)

        for t in self.run['tasks']:
            if (t['ended'] - t['started'] <= 1000):
                # an invalid task, discard it
                continue

            task_type=t['description']['taskType']['name']
            if  task_type == 'Visual KIS' or task_type == 'Textual KIS':
                # assert task.name not in tasks
                task_name = t['description']['name']
                videoId = t['description']['target']['item']['name']
                timeshot = int(t['description']['target']['temporalRange']['start']['value'] * 1000)
                shotId = self.v3c_videos.get_shot_from_video_and_frame(videoId, timeshot, unit='milliseconds')
               #TODO check target_start_ms
                target_start_ms = int(t['description']['target']['temporalRange']['start']['millisecond'])  # start time of TARGET video segment
                target_end_ms = int(t['description']['target']['temporalRange']['end'][ 'millisecond'])  # end time of TARGET video segment

                # task = Task(t['started'], t['ended'], t['duration'], t['position'], t['uid'], t['description']['taskType']['name'])
                # task.add_correct_shot_and_video(shotId, videoId)
                # tasks[task.name] = task
                # for every task, remember which was the correct submission time from each team in csts
                remaining_team_ids = set(team_ids)
                submissions = []
                for s in t['submissions']:
                    team_name = self.teams.get_teamname_from_id(s['teamId']['string'])
                    ss = deepcopy(s)
                    ss['teamName'] = team_name
                    submissions.append(ss)
                    if s['status'] == 'CORRECT' and s['teamId']['string'] in remaining_team_ids and len(
                            remaining_team_ids) > 0:
                        csts[team_name][task_name] = s['timestamp']
                        remaining_team_ids.remove(s['teamId']['string'])

                # the remaining teams have not found the correct result, put -1
                for r in remaining_team_ids:
                    csts[self.teams.get_teamname_from_id(r)][task_name] = -1

                tasks.add_task_vbs2022(
                    task_name,
                    t['started'],
                    t['ended'],
                    t['duration'],
                    t['position'],
                    t['uid']['string'],
                    task_type,
                    videoId,
                    target_start_ms,
                    target_end_ms,
                    submissions
                )

        return tasks,csts




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
        team_ids = self.teams.get_team_ids()
        csts = {k: {} for k in self.teams.get_team_names()}

        tasks = Tasks(self.v3c_videos)

        for t in self.run['tasks']:
            if(t['ended'] - t['started'] <= 1000): 
                # an invalid task, discard it
                continue

            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                # assert task.name not in tasks
                taskname = t['description']['name']
                videoId = t['description']['target']['item']['name']
                target_start_ms = int(t['description']['target']['temporalRange']['start']['millisecond'])  # start time of TARGET video segment
                target_end_ms = int(t['description']['target']['temporalRange']['end']['millisecond']) # end time of TARGET video segment

                # for every task, remember which was the correct submission time from each team in csts
                remaining_team_ids = set(team_ids)
                submissions=[]
                for s in t['submissions']:
                    team_name = self.teams.get_teamname_from_id(s['teamId']['string'])
                    ss = deepcopy(s)
                    ss['teamName'] = team_name
                    submissions.append(ss)
                    if s['status'] == 'CORRECT' and s['teamId']['string'] in remaining_team_ids and len(
                            remaining_team_ids) > 0:
                        csts[team_name][taskname] = s['timestamp']
                        remaining_team_ids.remove(s['teamId']['string'])

                # the remaining teams have not found the correct result, put -1
                for r in remaining_team_ids:
                    csts[self.teams.get_teamname_from_id(r)][taskname] = -1

                task_started=t['started']+5000 # the time when the countdown reached 0 and the first hint was displayed
                #note t['started']=the time when everybody was confirmed to be ready and the countdown for a task had started
                tasks.add_task_vbs2022(
                    taskname,
                    task_started,
                    t['ended'],
                    t['duration'],
                    t['position'],
                    t['uid']['string'],
                    t['description']['taskType']['name'],
                    videoId,
                    target_start_ms,
                    target_end_ms,
                    submissions
                )

        return tasks, csts

    def build_teams(self):
        teams = Teams()
        for team in self.run['description']['teams']:
            name = team['name']
            uid = team['uid']['string']
            teams.add_team(name, uid)

        return teams


class RunReaderVbse2022(RunReader):
    def __init__(self, run, v3c_videos, teams):
        super().__init__(run, v3c_videos, teams)

    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video, the predefined shot ids, the target shot start and end in milliseconds #
        """
        team_ids = self.teams.get_team_ids()
        csts = {k: {} for k in self.teams.get_team_names()}

        tasks = Tasks(self.v3c_videos)

        for t in self.run['tasks']:
            if (t['ended'] - t['started'] <= 1000 or t['description']['name'] in ['t1', 't2', 'vbse011', 'vbse027', 'vbse036']):
                # an invalid task, discard it
                continue

            taskname = t['description']['name']
            videoId = t['description']['target']['item']['name']
            target_start_ms = int(t['description']['target']['temporalRange']['start'][
                                      'millisecond'])  # start time of TARGET video segment
            target_end_ms = int(t['description']['target']['temporalRange']['end'][
                                    'millisecond'])  # end time of TARGET video segment

            # for every task, remember which was the correct submission time from each team in csts
            remaining_team_ids = set(team_ids)
            submissions = []
            for s in t['submissions']:
                team_name = self.teams.get_teamname_from_id(s['teamId']['string'])
                ss = deepcopy(s)
                ss['teamName'] = team_name
                submissions.append(ss)
                if s['status'] == 'CORRECT' and s['teamId']['string'] in remaining_team_ids and len(
                        remaining_team_ids) > 0:
                    csts[team_name][taskname] = s['timestamp']
                    remaining_team_ids.remove(s['teamId']['string'])

            # the remaining teams have not found the correct result, put -1
            for r in remaining_team_ids:
                csts[self.teams.get_teamname_from_id(r)][taskname] = -1

            task_started = t['started'] + 5000# the time when the countdown reached 0 and the first hint was displayed
                #note t['started']=the time when everybody was confirmed to be ready and the countdown for a task had started

            tasks.add_task_vbse2022(
                taskname,
                task_started,
                t['ended'],
                t['duration'],
                t['position'],
                t['uid']['string'],
                t['description']['taskType']['name'],
                videoId,
                t['description']['target']['item']['fps'],
                target_start_ms,
                target_end_ms,
                submissions,
                t['description']['hints']
            )

        return tasks, csts

    def build_teams(self):
        teams = Teams()
        for team in self.run['description']['teams']:
            name = team['name'].split('vbse_')[-1]
            uid = team['uid']['string']
            teams.add_team(name, uid)

        return teams

class RunReader2023(RunReader):
    def __init__(self, run, v3c_videos, teams):
        super().__init__(run, v3c_videos, teams)


    def build_tasks(self):
        """
        Mine tasks from the run file, including the correct video, the predefined shot ids, the target shot start and end in milliseconds #
        """
        team_ids = self.teams.get_team_ids()
        csts = {k: {} for k in self.teams.get_team_names()}

        tasks = Tasks(self.v3c_videos)

        for t in self.run['tasks']:
            if(t['ended'] - t['started'] <= 1000):
                # an invalid task, discard it
                continue

            if t['description']['taskType']['name'] == 'Visual KIS' or t['description']['taskType']['name'] == 'Textual KIS':
                # assert task.name not in tasks
                taskname = t['description']['name']
                videoId = t['description']['target']['item']['name']
                target_start_ms = int(t['description']['target']['temporalRange']['start']['millisecond'])  # start time of TARGET video segment
                target_end_ms = int(t['description']['target']['temporalRange']['end']['millisecond']) # end time of TARGET video segment

                # for every task, remember which was the correct submission time from each team in csts
                remaining_team_ids = set(team_ids)
                submissions=[]
                for s in t['submissions']:
                    team_name = self.teams.get_teamname_from_id(s['teamId']['string'])
                    ss = deepcopy(s)
                    ss['teamName'] = team_name
                    submissions.append(ss)
                    if s['status'] == 'CORRECT' and s['teamId']['string'] in remaining_team_ids and len(
                            remaining_team_ids) > 0:
                        csts[team_name][taskname] = s['timestamp']
                        remaining_team_ids.remove(s['teamId']['string'])

                # the remaining teams have not found the correct result, put -1
                for r in remaining_team_ids:
                    csts[self.teams.get_teamname_from_id(r)][taskname] = -1

                task_started=t['started']+5000 # the time when the countdown reached 0 and the first hint was displayed
                #note t['started']=the time when everybody was confirmed to be ready and the countdown for a task had started
                tasks.add_task_vbs2023(
                    taskname,
                    task_started,
                    t['ended'],
                    t['duration'],
                    t['position'],
                    t['uid']['string'],
                    t['description']['taskType']['name'],
                    videoId,
                    target_start_ms,
                    target_end_ms,
                    submissions
                )

        return tasks, csts

    def build_teams(self):
        teams = Teams()
        for team in self.run['description']['teams']:
            name = team['name']
            uid = team['uid']['string']
            teams.add_team(name, uid)

        return teams
