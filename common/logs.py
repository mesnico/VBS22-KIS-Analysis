from pathlib import Path
import pandas as pd
import os
import tqdm
import json

class TeamLogParser():
    def __init__(self, version, team, v3c_videos) -> None:
        self.version = version
        self.v3c_videos = v3c_videos
        if version == '2022':
            self.get_results = self.get_results_standard_2022 # if version == '2022' else self.get_results_visione_2021
            self.get_events = self.get_events_standard_2022 # if version == '2022' else None
        else:
            self.get_results = self.get_results_visione_2021
            self.get_events = self.get_events_standard_2022

    def get_results_standard_2022(self, result):
        result = result.rename(columns={'item': 'videoId'})
        # result['shotId'] = result.apply(lambda x: self.v3c_videos.get_shot_from_video_and_frame(x['videoId'], x['frame'], unit='milliseconds'), axis=1)
        result['shotTimeMs'] = result.apply(lambda x: self.v3c_videos.get_shot_time_from_video_and_frame(x['videoId'], x['frame']), axis=1)
        result = result.filter(['shotTimeMs', 'shotId', 'videoId', 'rank'])
        result = result.astype(int)
        return result

    def get_results_visione_2021(self, result):
        result = result.rename(columns={'frame': 'shotId', 'item': 'videoId'})
        result = result.filter(['shotId', 'videoId', 'rank'])
        result = result.astype(int)
        return result

    def get_events_standard_2022(self, events):
        events = events.filter(['timestamp', 'category', 'type', 'value'])
        return events
    

class TeamLogs:
    def __init__(self, data, team, max_records=10000, use_cache=False, cache_path='cache/team_logs'):
        self.v3c_videos = data['v3c_videos']
        self.runreader = data['runreader']

        # some caching logic for results
        cache_path = Path(cache_path) / data['version'] # append the version to the cache_path
        if not cache_path.exists():
            cache_path.mkdir(parents=True, exist_ok=True)
        results_cache_file = cache_path / '{}_results.pkl'.format(team)
        events_cache_file = cache_path / '{}_events.pkl'.format(team)
        if use_cache and (results_cache_file.exists() and events_cache_file.exists()):
            self.df_results = pd.read_pickle(results_cache_file)
            self.df_events = pd.read_pickle(events_cache_file)
        else:
            self.df_results, self.df_events = self.get_data(data, team, max_records)
            self.df_results.to_pickle(results_cache_file)
            self.df_events.to_pickle(events_cache_file)

    def _retrieve_timestamp(self, filename, js_list):
        try:
            # assume that every team has the timestamp in the filename
            timestamp = int(os.path.splitext(filename)[0])
        except ValueError:
            # try to search for a "timestamp" field in the json and return that one (e.g., vibro)
            timestamp = int(js_list['timestamp'])
        return timestamp

    def get_data(self, data, team, max_records):
        """
        retrieve all the data
        """
        results_dfs = []
        events_dfs = []
        team_log = data['config']['logs'][team]

        user_idx = 0
        log_parser = TeamLogParser(data['version'], team, self.v3c_videos)
        for root, _, files in os.walk(team_log):
            for file in tqdm.tqdm(files, desc="Processing {} logs".format(team)):
                path = os.path.join(root, file)
                if file == '.DS_Store':
                    continue
                with open(path) as f:
                    ranked_list = json.load(f)

                    timestamp = self._retrieve_timestamp(file, ranked_list)                    

                    # retrieve the task we are in at the moment
                    task = self.runreader.tasks.get_task_from_timestamp(timestamp)
                    if task is None:
                        # the logs outside task ranges are not important for us
                        continue
                    task_name = task['name']

                    # if a team already submitted, all the subsequent logs are just noise, delete them
                    csts = self.runreader.get_csts()
                    cst = csts[team][task_name]
                    if cst > 0 and timestamp > cst:
                        continue

                    # grab relevant infos from different team log files
                    if len(ranked_list['results']) > 0:
                        results_df = self.get_teams_results(ranked_list['results'], log_parser.get_results, max_records)

                        results_df['timestamp'] = timestamp # note that in events the timestamp should be already present
                        results_df['user'] = user_idx
                        results_df['task'] = task_name
                        results_df['team'] = team
                        results_dfs.append(results_df)

                    if len(ranked_list['events']) > 0:
                        events_df = self.get_teams_events(ranked_list['events'], log_parser.get_events)

                        events_df['user'] = user_idx
                        events_df['task'] = task_name
                        events_df['team'] = team
                        events_dfs.append(events_df)

            if Path(root) != Path(team_log):
                user_idx += 1   # number of user is the number of folders

        assert user_idx <= 2

        # prepare the final dataframe
        results_df = pd.concat(results_dfs, axis=0).reset_index(drop=True)
        events_df = pd.concat(events_dfs, axis=0).reset_index(drop=True)

        # for each timestamp, find the ranks of correct results
        ranks_df = results_df.groupby('timestamp').apply(lambda x: self.get_rank_of_correct_results(x))
        ranks_df = ranks_df.reset_index()
        # merge this table with the events, the key is the timestamp column
        events_and_ranks_df = events_df.merge(ranks_df, on='timestamp')

        return results_df, events_and_ranks_df

    def get_teams_events(self, events, events_fn):
        if isinstance(events, dict):
            events = [events]
        events = pd.DataFrame(events)
        return events_fn(events)

    def get_teams_results(self, results, results_fn, max_records):
        results = results[:max_records]
        results = pd.DataFrame(results)
        results = results_fn(results)

        # correct if rank is zero-based
        min_rank = results['rank'].min()
        assert min_rank in [0, 1]
        if min_rank == 0:
            results['rank'] = results['rank'] + 1
            
        return results

    def get_rank_of_correct_results(self, result, method='timeinterval'): # 'shotid' or 'timeinterval'
        best_logged_rank_video = float('inf')
        best_logged_rank_shot = float('inf')
        assert not result.empty
        task_name = result['task'].iloc[0]
        task = self.runreader.tasks.get_task_from_taskname(task_name)
        res = result

        # find correct videos
        res = res[res['videoId'] == task['correct_video']]

        if not res.empty:           
            best_video_rank_idx = res[['rank']].idxmin().iat[0]
            best_logged_rank_video = res[['rank']].at[best_video_rank_idx, 'rank']
            # best_logged_time_video = res[['adj_logged_time']].at[best_video_rank_idx, 'adj_logged_time']

            if method == 'shotid':
                # use shot id to discriminate the correct results
                res = res[res['shotId'] == task['correct_shot']]
            elif method == 'timeinterval':
                # use the time interval of the shot target to discriminate the correct results
                res = res[res['shotTimeMs'].between(task['target_start_ms'], task['target_end_ms'])]

            # check also for best shot rank
            if not res.empty:
                best_shot_rank_idx = res[['rank']].idxmin().iat[0]
                best_logged_rank_shot = res[['rank']].at[best_shot_rank_idx, 'rank']
                # best_logged_time_shot = res[['adj_logged_time']].at[best_video_rank_idx, 'adj_logged_time']

        return pd.Series({
            'rank_video': best_logged_rank_video,
            'rank_shot': best_logged_rank_shot
            })

    def filter_by_timestep(self, start_timestep, end_timestep):
        # easy but expensive solution
        t1 = self.df_results[self.df_results['timestamp'].between(start_timestep, end_timestep)].copy()
        
        # efficient implementation using bisect
        # timestamps = self.df['timestamp'].to_list()
        # start_idx = bisect.bisect_right(timestamps, start_timestep)
        # end_idx = bisect.bisect_right(timestamps, end_timestep)
        # t2 = self.df.iloc[start_idx:end_idx].copy()
        return t1

    def filter_by_task_name(self, task_name):
        t1 = self.df_results[self.df_results['task'] == task_name].copy()
        return t1

    def get_events_dataframe(self):
        return self.df_events

    def get_raw_results_dataframe(self):
        return self.df_results
