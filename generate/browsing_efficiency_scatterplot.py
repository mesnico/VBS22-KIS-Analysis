import math
import pandas as pd
import numpy as np
import seaborn as sns
from generate.result import Result
from generate.utils import compute_user_penalty, get_team_values_df

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
            team_df = self.logs[team].get_events_dataframe().reset_index()
            df = get_team_values_df(self.data, team_df, split_user, max_records)
            dfs.append(df)

        total_df = pd.concat(dfs, axis=0).reset_index()

        user_penalty = compute_user_penalty(total_df, max_records)       
        total_df['user_penalty'] = user_penalty
        total_df['best_user'] = 1
        total_df.loc[total_df.groupby(['team', 'task'])['user_penalty'].idxmin(), 'best_user'] = 0
        total_df = total_df.drop(['user_penalty'], axis=1)
        total_df = total_df[total_df["best_user"] == 0] # take only the best user directly

        return total_df

    def _render(self, df, time_of='first_appearance', marker_size=5, figsize=[7, 6], include_incorrect_submissions=False, split_tasks=False, regression_line=False):
        """
        Render the dataframe into a table or into a nice graph
        """
        
        if not include_incorrect_submissions:
            df = df[df["time_correct_submission"] != -1]
        
        # remap time correct_submission
        df["correct_submission"] = (df["time_correct_submission"] >= 0)

        # discard NaN values
        df = df[(df["rank_shot_last_appearance"] != -1) & (df["rank_shot_first_appearance"] != -1)]

        df["elapsed_first_appearance"] = df["time_correct_submission"] - df["time_first_appearance"]
        df["elapsed_last_appearance"] = df["time_correct_submission"] - df["time_last_appearance"]
        first_appearance_df = df[["elapsed_first_appearance", "rank_shot_first_appearance", "team", "task", "correct_submission"]].rename(columns={"elapsed_first_appearance": "elapsed", "rank_shot_first_appearance": "rank_shot"})
        last_appearance_df = df[["elapsed_last_appearance", "rank_shot_last_appearance", "team", "task", "correct_submission"]].rename(columns={"elapsed_last_appearance": "elapsed", "rank_shot_last_appearance": "rank_shot"})
        df = pd.concat([first_appearance_df.assign(dataset='first_appearance'), last_appearance_df.assign(dataset='last_appearance')])

        df["task"] = df["task"].apply(lambda x: 'Textual-KIS' if 'kis-t' in x else 'Visual-KIS')

        # Initialize the figure with a logarithmic x axis
        f, ax = plt.subplots(figsize=figsize)
        # ax.set_yscale("log")

        if include_incorrect_submissions:
            df.loc[~df["correct_submission"], "elapsed"] = 450

        df = df[df['dataset'] == time_of]

        if split_tasks == "visual":
            df = df[df['task'] == 'Visual-KIS']
        elif split_tasks == "textual":
            df = df[df['task'] == 'Textual-KIS']

        print(f"total rows: {len(df)}")

        ax.grid(True)

        # Plot elapsed (time delta) vs rank of first occurrence
        sns.scatterplot(data=df, x="rank_shot", y="elapsed", style="team", hue="task" if split_tasks=="same_graph" else "team", s=marker_size)
        # sns.scatterplot(data=df, x="rank_shot_last_appearance", y="elapsed_last_appearance")

        if regression_line:
            df_regression = df.copy()
            df_regression[~df_regression["correct_submission"]] = np.nan
            sns.regplot(x ='rank_shot', y ='elapsed', logx=True, scatter_kws={'s':0}, data=df_regression, label="regression line")
            # df_rolling = df_rolling.sort_values(by=["rank_shot"])
            # df_rolling['rolling_elapsed'] = df_rolling['elapsed'].rolling(25, min_periods=2).mean()
            # sns.lineplot(data=df_rolling, x="rank_shot", y="rolling_elapsed")

        if include_incorrect_submissions:
            rang = list(range(0, 500, 100)) + [450]
            ax.set_yticks(rang)
            ax.set_yticklabels(list(map(str,rang[:-1])) + ["NCS"])
            ax.axhline(450, ls='--', alpha=0.3)
            ax.set_ylim(0, 470)

        # Tweak the visual presentation
        ax.set(ylabel="time delta (seconds)", xlabel="shot rank")
        # sns.despine(trim=True, left=True)

        ax.set_xscale('log')
        r = range(0, int(math.log10(self.max_records)))
        ax.set_xticks([10**x for x in r])
        ax.set_xticklabels(['1' if x==0 else '10' if x==1 else f'10$^{x}$' for x in r])
        plt.savefig(f'output/kis_browsing_efficiency_scatterplot_timeof_{time_of}_splittask_{split_tasks}_shotrank{self.max_records}.pdf', format='pdf', bbox_inches="tight")