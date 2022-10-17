import pandas as pd
import seaborn as sns
from generate.result import Result
from generate.utils import compute_user_penalty, get_team_values_df

import matplotlib.pyplot as plt
import logging
logging.basicConfig(level=logging.INFO)

class BrowsingEfficiencyBoxplot(Result):
    def __init__(self, data, teams, logs, **kwargs):
        super().__init__(**kwargs, cache_filename='TimeRecallTable.pkl')
        self.data = data
        self.teams = teams
        self.logs = logs

    def _generate(self,
        split_user=False,  
        fill_missing=False, 
        max_records=10000):
        """
        Returns the view of the data interesting for the current analysis, in the form of a Pandas dataframe
        """

        self.max_records = max_records
        self.split_user = split_user
        dfs = []
        for team in self.teams:
            team_df = self.logs[team].get_events_dataframe().reset_index()
            team_df = get_team_values_df(self.data, team_df, split_user, max_records)
            dfs.append(team_df)

        total_df = pd.concat(dfs, axis=0).reset_index()
        
        view = total_df[total_df["time_correct_submission"] != -1].groupby(['team', 'user']).agg('count')["time_correct_submission"]
        print(view)

        user_penalty = compute_user_penalty(total_df, max_records)
                    
        total_df['user_penalty'] = user_penalty
        total_df['best_user'] = 1
        total_df.loc[total_df.groupby(['team', 'task'])['user_penalty'].idxmin(), 'best_user'] = 0
        total_df = total_df.drop(['user_penalty'], axis=1)

        # if fill_missing, missing datapoints are set to max_records + 1
        if fill_missing:
            total_df["rank_shot_margin_0"] = total_df["rank_shot_margin_0"].replace({-1: max_records + 1})

        return total_df

    def _render(self, df, figsize=[7, 6], show_boxplot=True, time_of=['time_first_appearance'], show_only_best=True):
        """
        Render the dataframe into a table or into a nice graph
        """

        assert not (not show_only_best and len(time_of) > 1)
        
        # discard NaN values
        if show_only_best:
            df = df[df['best_user'] == 0]

        df = df[(df["time_correct_submission"] != -1) & (df["rank_shot_last_appearance"] != -1) & (df["rank_shot_first_appearance"] != -1)]
        # time_column = "time_first_appearance" if time_of == 'first_appearance' else "time_last_appearance"
        user_column = "best_user"

        # df = df[df[time_column] != -1]
        df = df[["team", user_column, "task", "time_correct_submission", "time_first_appearance", "time_first_appearance_video", "time_last_appearance"]]
        df = df.melt(id_vars=["team", user_column, "task", "time_correct_submission"], value_vars=["time_first_appearance", "time_first_appearance_video", "time_last_appearance"], var_name="type")
        
        df["elapsed"] = df["time_correct_submission"] - df["value"]

        df = df[df["type"].isin(time_of)]

        # rename columns for better visualization
        df[user_column] = df[user_column].map({0: '1st' if user_column == "user" else "Best", 1: '2nd' if user_column == "user" else "Other"})
        df["type"] = df["type"].map({
            "time_first_appearance": "shot",
            "time_first_appearance_video": "video",
            "time_last_appearance": "last_shot"
        })

        # df = df.pivot(columns="team", values="r_s")
        # print(df)

        # Initialize the figure with a logarithmic x axis
        f, ax = plt.subplots(figsize=figsize)
        # ax.set_yscale("log")

        if not show_only_best:
            hue = user_column
        elif len(time_of) > 1:
            hue = "type"
        else:
            hue = None

        ax.yaxis.grid(True)

        if show_boxplot:
            sns.boxplot(x="team", hue=hue, y="elapsed", data=df,
                        whis=[0, 100], width=.6, palette="vlag")

        # Add in points to show each observation
        sns.stripplot(x="team", hue=hue, y="elapsed", data=df,
                    size=5, linewidth=1, dodge=show_boxplot, alpha=0.4 if show_boxplot else 1.0)

        if hue is not None:
            handles, labels = ax.get_legend_handles_labels()
            ax.legend(handles[:2], labels[:2], title="User" if not show_only_best else "")# , ncol=2, loc="lower right")
            
        # Tweak the visual presentation
        ax.set(ylabel="time delta (seconds)")
        # sns.despine(trim=True, left=True)

        # # draw boxplot
        # bplot = df.boxplot(column="rank_shot_margin_0", by="team")
        # bplot.set_yscale('log')
        plt.savefig(f'output/browsing_efficiency_boxplot_timeof_{time_of}_shotrank{self.max_records}.pdf', format='pdf', bbox_inches="tight")