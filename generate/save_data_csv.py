import pandas as pd
from generate.result import Result
import re

class saveDATAasCSV(Result):
    def __init__(self, data, teams, logs, **kwargs):
        super().__init__(**kwargs)
        #self.data =data
        self.logs=logs
        self.tasks=df_tasks=data['runreader'].tasks.tasks_df


    def _generate(self, **kwargs):
        """
        Returns Pandas dataframe with all the submissions
        """
        r = re.compile("([a-zA-Z]+)([0-9]+)")
        df=[]
        for index, row in self.tasks.iterrows():
            submissions=row['submissions']
            for s in submissions:
                teamFamily, user=r.match(s['teamName']).groups()
                d={
                    'taskName':row['name'],
                    'team':s['teamName'],
                    'teamFamily': teamFamily,
                    'user':user,
                    'task_start': row['started'] ,
                    'task_end': row['ended'],
                    'timestamp':s['timestamp'],
                    'sessionID': s['teamId']['string'],
                    'status':s['status']
                }
                df.append(d)

        df = pd.DataFrame(df)

        return df

    def _render(self, df):
        """
        save data
        """

        #save tasks
        self.tasks.rename(columns={"name": "taskName"})
        self.tasks.to_csv('output/vbse2022/tasks.csv',index=False)

        #save df (submissions)
        df.to_csv('output/vbse2022/submissions.csv',index=False)

        #save teams results
        r = re.compile("([a-zA-Z]+)([0-9]+)")
        for team, log in  self.logs.items():
            teamFamily, user = r.match(team).groups()
            log.df_results['user']=user
            log.df_results['teamFamily'] = teamFamily
            log.df_events['user']=user
            log.df_events['teamFamily'] = teamFamily
            log.df_events.to_csv(f"output/vbse2022/team_logs_csv/{team}_events.csv", index=False)
            log.df_results.to_csv(f"output/vbse2022/team_logs_csv/{team}_results.csv", index=False)





