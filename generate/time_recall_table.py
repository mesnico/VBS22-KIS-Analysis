import pandas as pd
import numpy as np
from generate.result import Result
from generate.utils import get_team_values_df

class TimeRecallTable(Result):
    def __init__(self, data, teams):
        super().__init__()
        self.data = data
        self.teams = teams

    def _generate(self):
        """
        Returns the view of the data interesting for the current analysis, in the form of a Pandas dataframe
        """

        dfs = []
        for team in self.teams:
            df = get_team_values_df(self.data, team)
            dfs.append(df)

        total_df = pd.concat(dfs, axis=0)
        return total_df

    def _render(self, df):
        """
        Render the dataframe into a table or into a nice graph
        """
        df = df.melt(var_name="metric", id_vars=["team", "task"], value_name="value")
        df = df.pivot(index=['team', 'metric'], columns="task", values="value")
        df = df.astype('int32')
        df[df < 0] = -1
        df = df.astype('str')
        df.replace(['-1'], '-', inplace=True)
        print(df)

