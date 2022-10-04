import pandas as pd
import seaborn as sns
from generate.result import Result
from generate.utils import get_team_values_df

import matplotlib.pyplot as plt
import logging
logging.basicConfig(level=logging.INFO)

class BrowsingEfficiencyScatterplot(Result):
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

    def _render(self, df, time_of='first_appearance', marker_size=5, figsize=[7, 6]):
        """
        Render the dataframe into a table or into a nice graph
        """
        
        # discard NaN values
        df = df[(df["time_correct_submission"] != -1) & (df["rank_shot_last_appearance"] != -1) & (df["rank_shot_first_appearance"] != -1)]

        df["elapsed_first_appearance"] = df["time_correct_submission"] - df["time_first_appearance"]
        df["elapsed_last_appearance"] = df["time_correct_submission"] - df["time_last_appearance"]
        first_appearance_df = df[["elapsed_first_appearance", "rank_shot_first_appearance", "team", "task"]].rename(columns={"elapsed_first_appearance": "elapsed", "rank_shot_first_appearance": "rank_shot"})
        last_appearance_df = df[["elapsed_last_appearance", "rank_shot_last_appearance", "team", "task"]].rename(columns={"elapsed_last_appearance": "elapsed", "rank_shot_last_appearance": "rank_shot"})
        df = pd.concat([first_appearance_df.assign(dataset='first_appearance'), last_appearance_df.assign(dataset='last_appearance')])

        # Initialize the figure with a logarithmic x axis
        f, ax = plt.subplots(figsize=figsize)
        # ax.set_yscale("log")

        # Plot elapsed (time delta) vs rank of first occurrence
        df = df[df['dataset'] == time_of]
        sns.scatterplot(data=df, x="rank_shot", y="elapsed", style='team', hue='team', s=marker_size)
        # sns.scatterplot(data=df, x="rank_shot_last_appearance", y="elapsed_last_appearance")

        # Tweak the visual presentation
        ax.grid(True)
        ax.set(ylabel="time delta (seconds)", xlabel="shot rank")
        # sns.despine(trim=True, left=True)

        ax.set_xscale('log')
        plt.savefig(f'output/browsing_efficiency_scatterplot_timeof_{time_of}_shotrank{self.max_records}.pdf', format='pdf', bbox_inches="tight")