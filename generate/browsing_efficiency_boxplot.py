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
        fill_missing=kwargs.get('fill_missing', False)
        max_records=kwargs.get('max_records', 10000)
        self.max_records = max_records
        dfs = []
        for team in self.teams:
            df = get_team_values_df(self.data, self.logs[team],split_user, max_records)
            dfs.append(df)

        total_df = pd.concat(dfs, axis=0)
        
        view = total_df[total_df["time_correct_submission"] != -1].groupby(['team', 'user']).agg('count')["time_correct_submission"]
        print(view)

        # if fill_missing, missing datapoints are set to max_records + 1
        if fill_missing:
            total_df["rank_shot_margin_0"] = total_df["rank_shot_margin_0"].replace({-1: max_records + 1})

        return total_df

    def _render(self, df, figsize=[7, 6], show_boxplot=True):
        """
        Render the dataframe into a table or into a nice graph
        """
        
        # discard NaN values
        df = df[df["time_correct_submission"] != -1]

        df["elapsed"] = df["time_correct_submission"] - df["time_first_appearance"]
        df = df[["elapsed", "team", "user", "task"]]
        
        # rename users column for better visualization
        df['user'] = df['user'].replace({0: '1st', 1: '2nd'})

        # df = df.pivot(columns="team", values="r_s")
        # print(df)

        # Initialize the figure with a logarithmic x axis
        f, ax = plt.subplots(figsize=figsize)
        ax.set_yscale("log")

        # Plot the orbital period with horizontal boxes
        if show_boxplot:
            sns.boxplot(x="team", hue="user", y="elapsed", data=df,
                        whis=[0, 100], width=.6, palette="vlag")

        # Add in points to show each observation
        sns.stripplot(x="team", hue="user", y="elapsed", data=df,
                    size=5, linewidth=1, dodge=show_boxplot, alpha=0.4 if show_boxplot else 1.0)

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[:2], labels[:2], title="User", ncol=2, loc="lower right")

        # Tweak the visual presentation
        ax.xaxis.grid(True)
        ax.set(ylabel="time delta (seconds)")
        # sns.despine(trim=True, left=True)

        # # draw boxplot
        # bplot = df.boxplot(column="rank_shot_margin_0", by="team")
        # bplot.set_yscale('log')
        plt.savefig(f'output/browsing_efficiency_boxplot_shotrank{self.max_records}.pdf', format='pdf', bbox_inches="tight")