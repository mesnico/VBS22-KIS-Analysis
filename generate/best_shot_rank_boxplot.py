import pandas as pd
import seaborn as sns
from generate.result import Result
from generate.utils import compute_user_penalty, get_team_values_df

import matplotlib.pyplot as plt
import logging
logging.basicConfig(level=logging.INFO)

class BestShotRankBoxplot(Result):
    def __init__(self, data, teams, logs, **kwargs):
        super().__init__(**kwargs, cache_filename='TimeRecallTable.pkl')
        self.data = data
        self.teams = teams
        self.logs = logs

    def _generate(self, 
        split_user=False, 
        best_user_policy=False, 
        fill_missing=False, 
        max_records=10000,
        aggregate_users_for=[]):
        """
        Returns the view of the data interesting for the current analysis, in the form of a Pandas dataframe
        """
        self.max_records = max_records
        self.best_user_policy = best_user_policy

        dfs = []
        for team in self.teams:
            team_df = self.logs[team].get_events_dataframe().reset_index()
            if team in aggregate_users_for:
            # these teams have undistinguishable physical users, given that their logs are taken from DRES
                team_df['user'] = 0
            team_df = get_team_values_df(self.data, team_df, split_user, max_records)
            dfs.append(team_df)

        total_df = pd.concat(dfs, axis=0).reset_index()

        view = total_df[total_df["rank_shot_margin_0"] != -1].groupby(['team', 'user']).agg('count')['rank_shot_margin_0']
        print(view)

        user_penalty = compute_user_penalty(total_df, max_records)
                    
        total_df['user_penalty'] = user_penalty
        total_df['best_user'] = 1
        total_df.loc[total_df.groupby(['team', 'task'])['user_penalty'].idxmin(), 'best_user'] = 0
        total_df = total_df.drop(['user_penalty'], axis=1)

        # if fill_missing, missing datapoints are set to max_records + 1
        if fill_missing:
            total_df["rank_shot_margin_0"] = total_df["rank_shot_margin_0"].replace({-1: max_records + 5000})

        return total_df

    def _render(self, df, figsize=[7, 6], show_boxplot=True, swarmplot=True, exclude_teams=[]):
        """
        Render the dataframe into a table or into a nice graph
        """
        # select only r_s
        df = df[["rank_shot_margin_0", "team", "user", "best_user", "task"]]

        # filter out unwanted teams
        df = df[~df["team"].isin(exclude_teams)]

        # discard NaN values
        df = df[df["rank_shot_margin_0"] != -1]

        # rename users column for better visualization
        df['user'] = df['user'].replace({0: '1st', 1: '2nd'})

        # df = df.pivot(columns="team", values="r_s")
        # print(df)

        # Initialize the figure with a logarithmic x axis
        f, ax = plt.subplots(figsize=figsize)
        ax.set_yscale("log")
        ax.set_yticks([1, 10, 100, 1000, 10000, 15000])
        ax.set_yticklabels([1,10,'10$^2$','10$^3$','10$^4$', 'NA'])#'>10$^4$'])

        # Plot the orbital period with horizontal boxes
        if show_boxplot:
            sns.boxplot(x="team", hue="best_user" if self.best_user_policy else "user", y="rank_shot_margin_0", data=df,
                        whis=[0, 100], width=.6, palette="vlag")

        # Add in points to show each observation

        if swarmplot:
            sns.swarmplot(x="team", hue="best_user" if self.best_user_policy else "user", y="rank_shot_margin_0", data=df,
                     size=4, linewidth=.3, dodge=True, alpha=0.4 if show_boxplot else 1.0)
        else:
            sns.stripplot(x="team", hue="best_user" if self.best_user_policy else "user", y="rank_shot_margin_0", data=df,
                     size=5, linewidth=1, dodge=True, alpha=0.4 if show_boxplot else 1.0)
        
        handles, labels = ax.get_legend_handles_labels()
        if self.best_user_policy:
            labels[0] = "best"
            labels[1] = "other"
        ax.legend(handles[:2], labels[:2], title="user", ncol=2, loc="lower right")

        # Tweak the visual presentation
        ax.yaxis.grid(True)
        ax.set(ylabel="best shot rank")
        # sns.despine(trim=True, left=True)

        # # draw boxplot
        # bplot = df.boxplot(column="rank_shot_margin_0", by="team")
        # bplot.set_yscale('log')
        plt.savefig(f'output/best_shot_rank_boxplot_bestuser-policy{self.best_user_policy}_shotrank{self.max_records}.pdf', format='pdf', bbox_inches="tight")