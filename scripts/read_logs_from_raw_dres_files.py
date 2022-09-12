
import argparse
import os
import os.path
import json
import pandas as pd
from datetime import datetime
import pickle

from common.load import load_data


def get_session_to_user_dic(df):
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
     any two sections that are active at the same time cannot be of the same user.
    '''
    session_to_user = {}

    #group df by session and compute login and logout timestamps
    #we assume
    # login=time of first log sumbission of a session,
    # logout=login=time of the last log sumbission of a session
    df_grouped_session = df.groupby('session')['timestamp'].agg(["min", "max"]) #min=Login time, max=Logout time

    #adding some colums with transformed timestamps that are useful during debugging
    df_grouped_session["login"] = df_grouped_session["min"].apply(lambda row: datetime.fromtimestamp(row / 1000))
    df_grouped_session["logout"] = df_grouped_session["max"].apply(lambda row: datetime.fromtimestamp(row / 1000))

    #sorting rows accpording to login times
    df_grouped_session.sort_values(by='min', inplace=True, ascending=True)

    #user assigment
    n_user=-1
    last_login_logout_users={}
    for session, row in df_grouped_session.iterrows():
        if n_user<0 : #first log is assigned to first user with ID 0
            n_user=0
            session_to_user[session]=n_user
            last_login_logout_users[n_user]={'login':row['min'],'logout': row['max']}
            continue
        #case in which we have already at least a user
        new_login=row['min']
        new_logout=row['max']
        control=False
        for user, login_logout in last_login_logout_users.items():
            if( new_login> login_logout['logout'] ):  #the current session time does not intersect a previous user session time -> we assume that it is the same user
                control=True
                session_to_user[session] = user
                last_login_logout_users[user] = {'login': new_login, 'logout': new_logout}
                break
        if(not control): #the current section time intersect all the previous user session times-> new user
            n_user +=1 #creating a new user
            session_to_user[session] = n_user
            last_login_logout_users[n_user] = {'login': new_login, 'logout': new_logout}

    df_grouped_session['user']= df_grouped_session.index.to_series().map(session_to_user) # to see the resulting assigment

    return session_to_user


def get_query_result_and_events_logs(args):
    '''
    Reading raw DRES data and selecting QueryResultLogEvent relevant to VBS KIS task
    '''
    query_result_and_events_logs = []
    resfile=f"{args.output_folder}/query_result_and_events_logs.p"
    os.makedirs(args.output_folder, exist_ok=True)

    if os.path.exists(resfile):
        query_result_and_events_logs=pickle.load(open(resfile, "rb"))
    else:
        # The raw raw_event files may contains several kinds of events, e.g.,
        # {'dev.dres.run.eventstream.RunEndEvent', 'dev.dres.run.eventstream.QueryResultLogEvent',
        # 'dev.dres.run.eventstream.SubmissionEvent', 'dev.dres.run.eventstream.RunStartEvent',
        # 'dev.dres.run.eventstream.TaskStartEvent', 'dev.dres.run.eventstream.QueryEventLogEvent',
        # 'dev.dres.run.eventstream.TaskEndEvent'}
        # For the logs, we are interested only on  'dev.dres.run.eventstream.QueryEventLogEvent' and 'dev.dres.run.eventstream.QueryResultLogEvent'

        # Please note that there are events not related to the Official VBS KIS tasks (e.g, AVS tasks or other tasks issued before or after the competition)
        # We'll use the audit file to read the LOGIN events and creating a mapping between team names and sessions ID
        # We'll use the run file to get info useful to discriminate if an raw_event in the raw file is related to the Official VBS KIS tasks


        raw_event_log_folder=args.raw_event_log_folder
        #load competition data
        competition_data = load_data(
            ['visione'],
            args.audits_file,
            args.run_file,
            args.v3c_fps_file,
            args.v3c_segments_files)

        ## getting LOGIN info from the audit file
        audit = competition_data['audit']
        session_team_dic = {}
        for audit_event in audit:
            if audit_event['type'] == "LOGIN":
                session = audit_event['session']
                session_team_dic[session] = audit_event['user']


        ## getting info from the run file
        runreader = competition_data['runreader']  # RunReader2022
        run = runreader.run  # original run file
        vbsRunID = run['id']['string']  # RunId of the official VBS 2022
        tasks = runreader.tasks  # it is used to check if a timestamp is inside a KIS task

        ## reading the raw files
        query_events_logs = []
        for filename in os.listdir(raw_event_log_folder):
            f = os.path.join(raw_event_log_folder, filename)
            with open(f, 'r') as ff:
                print(f"Reading file {f}, about {round(os.stat(f).st_size / (1024 * 1024))} MB ...", end='   ')
                for line in ff:
                    raw_event = json.loads(line)  #
                    event_timestamp = raw_event['timeStamp']
                    if (raw_event['runId']['string'] != vbsRunID or (tasks.get_task_from_timestamp(event_timestamp+10000) is None  and tasks.get_task_from_timestamp(event_timestamp-10000) is None)): #  excluding events not related to Official KIS VBS run #10 seconds of tollerance
                        continue
                    team_name = session_team_dic.get(raw_event['session']) #will put None for unrecognized teams
                    if raw_event['class'] == 'dev.dres.run.eventstream.QueryEventLogEvent':
                        log = {'name': team_name, 'session': raw_event['session'], 'timestamp': event_timestamp, 'events': raw_event['queryEventLog']}
                        query_events_logs.append(log)

                    if raw_event['class'] == 'dev.dres.run.eventstream.QueryResultLogEvent':
                        log = {'name': team_name,'session': raw_event['session'], 'timestamp': event_timestamp,
                               'logs': raw_event['queryResultLog']}
                        query_result_and_events_logs.append(log)
                print("[DONE]")
        # saving intermediate result
        pickle.dump(query_result_and_events_logs, open(resfile, "wb"))  # save it into a file named save.p

    return query_result_and_events_logs


def main(args):
    '''
       Read raw Dres files and save teams logs submitted during a KIS task
    '''

    query_result_and_events_logs=get_query_result_and_events_logs(args) #arry of events

    teams_with_logs = set([q['name'] for q in query_result_and_events_logs])

    for team in teams_with_logs:
        log_list = [log for log in query_result_and_events_logs if log['name'] == team]
        df = pd.DataFrame(log_list) #data frame with the logs of teh selected team

        session_to_user_dic = get_session_to_user_dic(df)
        df['user'] = df['session'].map(session_to_user_dic) #add a column with estimated user ID
        users=df.user.unique()
        for user in users:
            out_folder=f"{args.output_folder}/{team}/user{user}"
            os.makedirs(out_folder, exist_ok=True)
            print(f"Saving logs of {team} - user{user}", end=" ...")
            logs=df.loc[df['user'] == user]['logs']
            for log in logs:
                filename=f"{out_folder}/{log['timestamp']}.json"
                with open(filename, 'w') as fp:
                    json.dump(log, fp)
            print("[DONE]")
    print("End")


if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Read Logs (query events + results) only for KIS tasks from raw DRES files')
        parser.add_argument('--raw_event_log_folder', default='../data/2022/raw event logs')
        parser.add_argument('--output_folder', default='../data/2022/logs_from_dres')
        parser.add_argument('--audits_file', default='../data/2022/audits.json')
        parser.add_argument('--run_file', default='../data/2022/run.json')
        parser.add_argument('--v3c_fps_file', default='../data/v3c1_2_fps.csv')
        parser.add_argument('--v3c_segments_files', nargs='+',
                            default=['../data/v3c1_frame_segments.csv', '../data/v3c2_frame_segments.csv'])

        args = parser.parse_args()
        main(args)

