import pandas as pd
import numpy as np
from generate.result import Result
from generate.utils import get_team_values_df

class TimeRecallTable(Result):
    def __init__(self, data, teams, logs, **kwargs):
        super().__init__(**kwargs)
        self.data = data
        self.teams = teams
        self.logs = logs

    def _generate(self,**kwargs):
        """
        Returns the view of the data interesting for the current analysis, in the form of a Pandas dataframe
        """
        split_user=kwargs.get('split_user', False)
        max_records=kwargs.get('max_records', 10000)
        dfs = []
        for team in self.teams:
            df = get_team_values_df(self.data, self.logs[team],split_user, max_records)
            dfs.append(df)

        total_df = pd.concat(dfs, axis=0)
        return total_df

    def _render(self, df):
        """
        Render the dataframe into a table or into a nice graph
        """
        #renaming task
        rename_fun = lambda x: x.replace('vbs22-kis-t', 'T_').replace('vbs22-kis-v', 'V_')
        df['task'] = df['task'].apply(rename_fun)
        tasks = df[['task','task_start']].sort_values(by=['task_start'])['task'].unique()
        textual=[t for t in tasks if t.startswith('T')] #used later to order the column
        visual=[t for t in tasks if t.startswith('V')] #used later to order the column
        df.drop(columns='task_start', inplace=True)

        df = df.fillna(-1)
        col=[c for c in df.columns.values.tolist() if c!='team' and c!='task' and c!='user']
        df[col]= df[col].astype('int32')
        df[col] = df[col].applymap(lambda x: -1 if x< 0 else x)
        df = df.astype('str')
        df.replace(['-1'], '-', inplace=True)

        #aggregate
        agg_dic={c: (lambda x: ' / '.join(x)) for c in col}
        agg_dic['time_correct_submission']="min"
        df=df.groupby(['team','task'])[col].agg(agg_dic ).reset_index()
        df.replace('- / -', '-', regex=True, inplace=True)
        add_second= lambda x: x if x=='-' else x + 's'
        df['time_correct_submission']=df['time_correct_submission'].apply(add_second)
        df['time_best_shot']=df['time_best_shot'].apply(add_second)
        df['rank_shot_margin_5'] = df['rank_shot_margin_5'].apply(add_second)
        df['time_best_video'] = df['time_best_video'].apply(add_second)

        df = df.melt(var_name="metric", id_vars=["team", "task"], value_name="value")
        df['unit'] = df['metric'].apply(lambda x: 'rank' if x.startswith('rank_') else 'time')
        replace_dic = {
            'rank_shot_margin_0': 'correct frame',
            'time_best_shot': 'correct frame',
            'rank_shot_margin_5': 'frame in GT+2x5s',
            'time_best_shot_margin5': 'frame in GT+2x5s',
            'rank_video': 'correct video',
            'time_best_video': 'correct video',
            'time_correct_submission': 'correct submission'
        }
        df['metric']=df['metric'].map(replace_dic)
        df = df.pivot(index=['team', 'metric', 'unit'], columns="task", values="value")
        df=df.fillna('-')


        #sorting index desired order
        level_0=self.teams #order in the conf file
        level_1=['correct frame', 'frame in GT+2x5s', 'correct video','correct submission']
        level_2=['rank','time']
        df = df.reindex(pd.MultiIndex.from_product([level_0,level_1,level_2]))
        df.dropna(axis=0, inplace=True)#'correct submission'/rank shluld not be in the index
        print(df)
        df.to_csv('output/time_recall_table_withMargin5.csv')
        # sorting index desired order
        level_0 = self.teams  # order in teh conf file
        level_1 = ['correct frame', 'correct video', 'correct submission']
        level_2 = ['rank', 'time']
        df = df.reindex(pd.MultiIndex.from_product([level_0, level_1, level_2]))
        df.dropna(axis=0, inplace=True)  # 'correct submission'/rank shluld not be in the index

        #ordering columns
        df=df[textual+visual]
        df.to_csv('output/time_recall_table.csv')
        #formatting













