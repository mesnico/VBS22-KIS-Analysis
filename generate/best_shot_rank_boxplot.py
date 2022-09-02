import pandas as pd
from generate.result import Result
from generate.utils import get_team_values_df

import matplotlib.pyplot as plt

class BestShotRankBoxplot(Result):
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
        # select only r_s
        df = df[["r_s", "team", "task"]]
        # discard NaN values
        df = df[df['r_s'] != -1]

        # df = df.pivot(columns="team", values="r_s")
        print(df)

        # draw boxplot
        bplot = df.boxplot(column="r_s", by="team")
        bplot.set_yscale('log')
        plt.savefig('output/best_shot_rank_boxplot.pdf', format='pdf', bbox_inches="tight")