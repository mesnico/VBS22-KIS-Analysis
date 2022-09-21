import pandas as pd
import seaborn as sns
from generate.result import Result
from generate.utils import get_team_values_df

import matplotlib.pyplot as plt
import logging
logging.basicConfig(level=logging.INFO)

class BrowsingEfficiencyBoxplot(Result):
    def __init__(self, data, teams, logs, **kwargs):
        super().__init__(**kwargs, cache_filename='TimeRecallTable.pkl')
        self.data = data
        self.teams = teams
        self.logs = logs

    def _generate(self,**kwargs):
        """
        Returns the view of the data interesting for the current analysis, in the form of a Pandas dataframe
        """
        split_user=kwargs.get('split_user', False)
        max_records=kwargs.get('max_records', 10000)
        self.max_records = max_records
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
        
        # discard NaN values
        df = df[df["time_correct_submission"] != -1]

        df["elapsed"] = df["time_correct_submission"] - df["time_first_appearance"]
        df = df[["elapsed", "team", "task"]]
        

        # df = df.pivot(columns="team", values="r_s")
        # print(df)

        # Initialize the figure with a logarithmic x axis
        f, ax = plt.subplots(figsize=(7, 6))
        ax.set_yscale("log")

        # Plot the orbital period with horizontal boxes
        sns.boxplot(x="team", y="elapsed", data=df,
                    whis=[0, 100], width=.6, palette="vlag")

        # Add in points to show each observation
        sns.stripplot(x="team", y="elapsed", data=df,
                    size=4, color=".3", linewidth=0)

        # Tweak the visual presentation
        ax.xaxis.grid(True)
        ax.set(ylabel="time delta (seconds)")
        # sns.despine(trim=True, left=True)

        # # draw boxplot
        # bplot = df.boxplot(column="rank_shot_margin_0", by="team")
        # bplot.set_yscale('log')
        plt.savefig(f'output/browsing_efficiency_boxplot_shotrank{self.max_records}.pdf', format='pdf', bbox_inches="tight")