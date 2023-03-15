from generate.result import Result

class TasksTableVbse2022(Result):
    def __init__(self, data, teams, logs, **kwargs):
        super().__init__(**kwargs)
        self.competition_data = data


    def _generate(self, **kwargs):
        """
        Returns table with tasks info
        """
        ## getting info from the run file
        runreader = self.competition_data['runreader']
        run = runreader.run  # original run file
        vbsRunID = run['id']['string']  # RunId of the official vbse
        tasks = runreader.tasks.tasks_df  # it is used to check if a timestamp is inside a KIS task

        tasks = tasks.drop(['duration', 'position', 'task_type', 'uid', 'submissions'], axis=1)
        tasks['started'] = tasks.started.astype('int64')
        tasks['ended'] = tasks.ended.astype('int64')
        tasks['correct_shot'] = tasks.correct_shot.astype('int64')
        tasks['target_start_ms'] = tasks.target_start_ms.astype('int64')
        tasks['target_end_ms'] = tasks.target_end_ms.astype('int64')

        return tasks

    def _render(self, df):
        """
        Render the dataframe into a table or into a nice graph
        """
        df.to_csv('output/vbse2022/KIS_tasks.csv')




