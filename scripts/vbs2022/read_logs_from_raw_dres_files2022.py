import argparse
import os
import os.path
import json
import pandas as pd
from datetime import datetime
import pickle
import time
import numpy as np

from common.load import load_data


def get_session_to_user_dic(team, df):
    '''
    A function that tries approximately to associate each session with a user so that two sessions that
    are active at the same time are associated with different users

    Note:The 'session' uniquely identifies the user by session token:
    if we have multiple systems signed in independently, even using the same account,
     their session tokens will be different. How many of sessions you have per team
     depends on how many unique systems there were and how often people logged in and out
     Since in VBS2022 there was only one user account per team, there isn't necessarily a proper
     mapping to individual participants.
    This function heuristically try to assign a userID to each session so that
     any two sessions that are active at the same time cannot be of the same user.
    '''
    session_to_user = {}

    df_grouped_session = df.groupby(['session', 'login', 'logout'])['timestamp'].agg(
        ["min", "max"])  # min=first log submission, max=last log submission
    # sorting rows according to login times
    df_grouped_session.reset_index(level=2, inplace=True)
    df_grouped_session.sort_index(level=1, inplace=True, ascending=True)

    # user assigment
    if (team == 'vitrivr-vr'):
        # using session-user assignement saved locally  by the vitrivr-vr
        session_to_user = {
            ('node013vscuk483at6nh0qxm0rwjiy75', 1654506387566): 'Florian',
            ('node019gxtv6g8rga81xk6egdupyamq90', 1654583452345): 'Florian',
            ('node01g16na7y4h90z1eijqcevdo42k8', 1654510583484): 'Florian',
            ('node01kmm0jqr03ptmvutrvr2hwiz1159', 1654593468889): 'Ralph',
            ('node01mfnps94cu5fh10vqfirc6fijm134', 1654590087134): 'Ralph',
            ('node01rqxyv0fmnmpkb30mstl8e95499', 1654584835435): 'Ralph',
            ('node01xjxcbso6lgkv127uw8ld8u4cp579', 1654596958503): 'Ralph',
            ('node01xott5c5bqimghwd6r0v7m91f1358', 1654599569873): 'Ralph',
            ('node0428ehl3ekhfpa2gnueudm1s4280', 1654507174671): 'Ralph',
            ('node08szn1ed7mnqj1dfyh30ig0px588', 1654583156033): 'Florian',
            ('node0c26ao34zq8dx1cdqxcaw9uhg210', 1654510659050): 'Ralph',
            ('node0epocc4bw67rh182xw0gauu7kj38', 1654506230321): 'Ralph',
            ('node0hp3fdgm8naokur8mfia6hg7g130', 1654590029509): 'Florian',
            ('node0iyepdak46yqr1sfu9450a9eml18', 1654512833492): 'Ralph',
            ('node0ma6d0ylhuf7x1tjrispdexri1309', 1654507873231): 'Ralph',
            ('node0mcl96c49mcn05xopd5fxpwp9114', 1654588457666): 'Ralph'
        }

        df_grouped_session['user'] = df_grouped_session.index.to_series().map(
            session_to_user)  # to see the resulting assigment
        df_grouped_session.reset_index(inplace=True)
    else:
        n_user = -1
        last_login_logout_users = {}
        for idx, row in df_grouped_session.iterrows():
            session, login = idx
            if n_user < 0:  # first log is assigned to first user with ID 0
                n_user = 0
                session_to_user[idx] = n_user
                last_login_logout_users[n_user] = {'login': login, 'logout': min(row['max'], row['logout'])}
                continue
            # case in which we have already at least a user
            new_login = login
            new_logout = min(row['max'], row['logout'])
            control = False
            for user, login_logout in last_login_logout_users.items():
                if (new_login > login_logout[
                    'logout']):  # the current session time does not intersect a previous user session time -> we assume that it is the same user
                    control = True
                    session_to_user[idx] = user
                    last_login_logout_users[user] = {'login': new_login, 'logout': new_logout}
                    break
            if (not control):  # the current section time intersect all the previous user session times-> new user
                n_user += 1  # creating a new user
                session_to_user[idx] = n_user
                last_login_logout_users[n_user] = {'login': new_login, 'logout': new_logout}

        df_grouped_session['user'] = df_grouped_session.index.to_series().map(
                session_to_user)  # to see the resulting assigment
        df_grouped_session.reset_index(inplace=True)
        # adding some columns with transformed timestamps that are useful during debugging
        df_grouped_session["login_time"] = df_grouped_session["login"].apply(
            lambda row: datetime.fromtimestamp(row / 1000))
        df_grouped_session["logout_time"] = df_grouped_session["logout"].apply(
            lambda row: datetime.fromtimestamp(row / 1000))
        df_grouped_session["first_log_time"] = df_grouped_session["min"].apply(
            lambda row: datetime.fromtimestamp(row / 1000))
        df_grouped_session["last_log_time"] = df_grouped_session["max"].apply(
            lambda row: datetime.fromtimestamp(row / 1000))

    return session_to_user


def get_data_from_raw_files(args):
    '''
    Reading raw DRES data and selecting QueryResultLogEvent, QueryEventLogEvent, and  SubmissionEvent relevant to VBS KIS tasks
    '''
    query_result_and_events_logs = []
    query_events_logs = []
    correct_submissions = []
    all_submissions = []
    resfile = f"{args.output_folder}/query_result_and_events_logs.p"
    os.makedirs(args.output_folder, exist_ok=True)

    if os.path.exists(resfile):
        query_result_and_events_logs, query_events_logs, correct_submissions,all_submissions = pickle.load(open(resfile, "rb"))
    else:
        # The raw raw_event files may contains several kinds of events, e.g.,
        # {'dev.dres.run.eventstream.RunEndEvent', 'dev.dres.run.eventstream.QueryResultLogEvent',
        # 'dev.dres.run.eventstream.SubmissionEvent', 'dev.dres.run.eventstream.RunStartEvent',
        # 'dev.dres.run.eventstream.TaskStartEvent', 'dev.dres.run.eventstream.QueryEventLogEvent',
        # 'dev.dres.run.eventstream.TaskEndEvent'}
        # For the logs, we are interested only on  'dev.dres.run.eventstream.QueryResultLogEvent'

        # Please note that there are events not related to the Official VBS KIS tasks
        # (e.g, AVS tasks or other tasks issued before or after the competition)
        # We'll use the audit file to read the LOGIN events and creating a mapping between team names and sessions ID
        # We'll use the run file to get info useful to discriminate if a raw_event in the raw file is related to the Official VBS KIS tasks

        raw_event_log_folder = args.raw_event_log_folder
        # load competition data
        competition_data = load_data(
            ['visione'],
            args.audits_file,
            args.run_file,
            args.v3c_fps_file,
            args.v3c_segments_files)

        ## getting LOGIN info from the audit file
        audit = competition_data['audit']
        session_team_list = []
        for audit_event in audit:
            if audit_event['type'] == "LOGIN":
                session = audit_event['session']
                team = audit_event['user']
                login_time = audit_event['timestamp']
                session_team_list.append({'session': session, 'team': team, 'login_timestamp': login_time})

        all_sessions_df = pd.DataFrame(session_team_list)
        all_sessions_df.to_csv(f"{args.output_folder}/sessionIdLogin.csv")  # saving session IDlogin
        sessions = all_sessions_df['session'].unique()

        session_df = pd.DataFrame(columns=['session', 'team', 'login', 'logout'])
        for s in sessions:
            selected = all_sessions_df.loc[all_sessions_df['session'] == s]
            # if len(selected.index)>1:
            #     #finding consecutive rows with the same session and same team to merge the login
            #     # selected['pattern']=(selected.groupby(['session', selected['team'].ne(selected['team'].shift()).cumsum()])['session'].transform('size').ge(2).astype(int))
            #     g = (selected['team'] != selected.shift().fillna(method='bfill')['team']).cumsum().rename('group')
            #     selected=selected.groupby(['session', 'team', g])['login_timestamp'].agg(['min']).reset_index().rename(
            #         columns={'min': 'login'}).drop(['group'], axis=1)
            # else:
            #
            selected = selected.rename(columns={'login_timestamp': 'login'})
            selected.sort_values(by='login', inplace=True, ascending=True)
            logins = selected['login'].values
            logout = [logins[l + 1] for l in range(len(logins) - 1)]
            logout.append(
                1654732799000)  # no info on the logout, setting 8 June 2022, 11PM #(datetime.timestamp(datetime.now()) * 1000)
            selected['logout'] = logout
            # selected['login_time'] = selected['login'].apply(lambda x: datetime.fromtimestamp(x/1000))
            # selected['logout_time'] = selected['logout'].apply(lambda x: datetime.fromtimestamp(x/1000))
            session_df = pd.concat([session_df, selected])

        ## getting info from the run file
        runreader = competition_data['runreader']  # RunReader2022
        run = runreader.run  # original run file
        vbsRunID = run['id']['string']  # RunId of the official VBS 2022
        tasks = runreader.tasks  # it is used to check if a timestamp is inside a KIS task

        ## reading the raw files
        for filename in os.listdir(raw_event_log_folder):
            f = os.path.join(raw_event_log_folder, filename)
            with open(f, 'r') as ff:
                print(f"Reading file {f}, about {round(os.stat(f).st_size / (1024 * 1024))} MB ...", end='   ')
                for line in ff:
                    raw_event = json.loads(line)  #
                    dres_timestamp = raw_event['timeStamp']

                    isNotVBS = raw_event['runId']['string'] != vbsRunID
                    # excluding events not related to Official KIS VBS run # using 5 seconds of tollerance before the start and end of a task
                    isNotKIStask = (tasks.get_task_from_timestamp(dres_timestamp) is None) and (
                                tasks.get_task_from_timestamp(dres_timestamp + 5000) is None) and (
                                           tasks.get_task_from_timestamp(
                                               dres_timestamp - 5000) is None)

                    isResultLogEvent = raw_event['class'] == 'dev.dres.run.eventstream.QueryResultLogEvent'
                    isEventLogEvent = raw_event['class'] == 'dev.dres.run.eventstream.QueryEventLogEvent'
                    isSubmissionEvent = raw_event['class'] == 'dev.dres.run.eventstream.SubmissionEvent'

                    isNotRelevantEvent = not (isResultLogEvent or isEventLogEvent or isSubmissionEvent)
                    if (isNotVBS or isNotKIStask or isNotRelevantEvent):
                        continue

                    df = session_df[session_df['session'] == raw_event['session']]
                    assert not df.empty, 'Unrecognized session - team'

                    intervals = df.apply(lambda x: pd.Interval(x['login'], x['logout'], closed='left'), axis=1)
                    s_df = df[pd.arrays.IntervalArray(intervals).contains(dres_timestamp)]

                    assert len(s_df.index) == 1, 'Not unique assignment of session to team is possible'

                    team_name = s_df['team'].iloc[0]
                    session_login = s_df['login'].iloc[0]
                    session_logout = s_df['logout'].iloc[0]

                    if (isResultLogEvent):
                        client_timestamp = raw_event['queryResultLog']['timestamp']
                        log = {'name': team_name,
                               'session': raw_event['session'],
                               'login': session_login,
                               'logout': session_logout,
                               'timestamp': client_timestamp,
                               'client_timestamp': client_timestamp,
                               'dres_timestamp': dres_timestamp,
                               'logs': raw_event['queryResultLog']}
                        if abs(client_timestamp-dres_timestamp)>1000:
                            print(f"{team_name}, {dres_timestamp}, client_timestamp-dres_timestamp={(client_timestamp-dres_timestamp)}")
                        query_result_and_events_logs.append(log)
                    if (isEventLogEvent):
                        client_timestamp = raw_event['queryEventLog']['timestamp']
                        log = {'name': team_name,
                               'session': raw_event['session'],
                               'login': session_login,
                               'logout': session_logout,
                               'timestamp': client_timestamp,
                               'client_timestamp': client_timestamp,
                               'dres_timestamp': dres_timestamp,
                               'logs': raw_event['queryEventLog']}
                        query_events_logs.append(log)
                    if (isSubmissionEvent):
                        client_timestamp = raw_event['submission']['timestamp']
                        sub = {'name': team_name,
                               'session': raw_event['session'],
                               'login': session_login,
                               'logout': session_logout,
                               'timestamp': client_timestamp,
                               'client_timestamp': client_timestamp,
                               'dres_timestamp': dres_timestamp,
                               'runId': raw_event['runId'],
                               'taskId': raw_event['taskId'],
                               'submission': raw_event['submission']}
                        if (raw_event['submission']['status'] == 'CORRECT'):
                            correct_submissions.append(sub)
                        all_submissions.append(sub)

                print("[DONE]")
        # saving intermediate result
        pickle.dump((query_result_and_events_logs, query_events_logs, correct_submissions,all_submissions), open(resfile, "wb"))  # save it into a file named save.p

    return query_result_and_events_logs, query_events_logs, correct_submissions,all_submissions


def main(args):
    '''
       Read raw DRES files and save teams logs submitted during KIS tasks
    '''

    query_result_and_events_logs, query_events_logs, correct_submissions,all_submissions = get_data_from_raw_files(
        args)  # array of events
    #teams_with_logs = set([q['name'] for q in query_result_and_events_logs])  #
    # {'ivist', 'visione', 'verge', 'vitrivr-vr', 'vitrivr'}
    # ivist has empty logs,
    # visione logs are incomplete, they provided logs saved locally
    teams_with_logs = [  'vitrivr-vr', 'vitrivr' ]  #'verge',

    def contain_res (x):
        if len(x.get('results',[])) > 0:
            return len(x.get('results',[]))
        else:
            return np.nan

    for team in teams_with_logs:
        print(team)
        log_list = [log for log in query_result_and_events_logs if log['name'] == team]
        df = pd.DataFrame(log_list)  # data frame with the logs of the selected team
        subdf = pd.DataFrame([sub for sub in all_submissions if sub['name'] == team])

        session_to_user_dic = get_session_to_user_dic(team, df)
        # = df[['session']].map(session_to_user_dic) #add a column with estimated user ID
        df['user'] = df.apply(lambda x: session_to_user_dic[(x['session'], x['login'])],
                              axis=1)
        users = df.user.unique()
        for user in users:
            print(f"user: {user}")
            out_folder = f"{args.output_folder}/{team}/user{user}"
            os.makedirs(out_folder, exist_ok=True)
            print(f"Saving logs of {team} - user{user}", end=" ...")
            selected_df=df.loc[df['user'] == user]
            logs = selected_df['logs']
            ts_df=selected_df[['client_timestamp','dres_timestamp', 'logs']]
            ts_df['hasRes']=ts_df['logs'].apply(contain_res)
            ts_df = ts_df[ts_df['hasRes'].notna()]
            diff=(ts_df['dres_timestamp']-ts_df['client_timestamp'])/1000

            for log in logs:
                filename = f"{out_folder}/{log['timestamp']}.json"
                with open(filename, 'w') as fp:
                    json.dump(log, fp)
            print(f"{diff.describe()}")
            print("[DONE]")
    print("End")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Read Logs (query events + results) only for KIS tasks from raw DRES files')
    parser.add_argument('--raw_event_log_folder', default='../../data/2022/raw event logs')
    parser.add_argument('--output_folder', default='../../data/2022/logs_from_dres')
    parser.add_argument('--audits_file', default='../../data/2022/audits.json')
    parser.add_argument('--run_file', default='../../data/2022/run.json')
    parser.add_argument('--v3c_fps_file', default='../../data/v3c1_2_fps.csv')
    parser.add_argument('--v3c_segments_files', nargs='+',
                        default=['../../data/v3c1_frame_segments.csv', '../../data/v3c2_frame_segments.csv'])

    args = parser.parse_args()
    main(args)
