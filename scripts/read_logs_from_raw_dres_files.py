
import argparse
import os
import json
import pandas as pd

from common.load import load_data


def main(args):

    raw_event_log_folder=args.raw_event_log_folder
    # The raw raw_event files may contains several kinds of events, e.g.,
    # {'dev.dres.run.eventstream.RunEndEvent', 'dev.dres.run.eventstream.QueryResultLogEvent',
    # 'dev.dres.run.eventstream.SubmissionEvent', 'dev.dres.run.eventstream.RunStartEvent',
    # 'dev.dres.run.eventstream.TaskStartEvent', 'dev.dres.run.eventstream.QueryEventLogEvent',
    # 'dev.dres.run.eventstream.TaskEndEvent'}
    # For the logs, we are interested only on  'dev.dres.run.eventstream.QueryEventLogEvent' and 'dev.dres.run.eventstream.QueryResultLogEvent'

    # Please note that there are events not related to the Official VBS KIS tasks (e.g, AVS tasks or other tasks issued before or after the competition)
    # We'll use the audit file to read the LOGIN events and creating a mapping between team names and sessions ID
    # We'll use the run file to get info useful to discriminate if an raw_event in the raw file is related to the Official VBS KIS tasks

    competition_data = load_data(
        ['visione'],
        args.audits_file,
        args.run_file,
        args.v3c_fps_file,
        args.v3c_segments_files)

    ## getting LOGIN info from the run file
    audit = competition_data['audit']
    session_team_dic = { }
    for audit_event in audit:
        if audit_event['type'] == "LOGIN":
            session = audit_event['session']
            session_team_dic[session] = audit_event['user']

    ## getting info from the run file
    runreader = competition_data['runreader']  # RunReader2022
    run = runreader.run #original run file
    vbsRunID = run['id']['string'] #RunId of the official VBS 2022
    kis_tasks = runreader.tasks.tasks_df #list of the KIS tasks
    # now we search for two timestamps delimiting the interval on which the KIS tasks where issued
    kis_start = int(min(kis_tasks['started']))
    kis_end =  int(max(kis_tasks['ended'])) + 300000  # adding 5 minutes to the end of the last KIS task for late submitted logs


    ## reading the raw files
    query_events_logs = []
    query_result_and_events_logs = []
    for filename in os.listdir(raw_event_log_folder):
        f = os.path.join(raw_event_log_folder, filename)
        for line in open(f, 'r'):
            raw_event = json.loads(line)  #
            event_timestamp = raw_event['timeStamp']
            if (raw_event['runId']['string'] != vbsRunID or event_timestamp < kis_start or event_timestamp > kis_end ):  # excluding events not related to Official KIS VBS runs
                continue
            team_name = session_team_dic.get(raw_event['session']) #will put None for unrecognized teams
            if raw_event['class'] == 'dev.dres.run.eventstream.QueryEventLogEvent':
                log = {'name': team_name, 'timestamp': event_timestamp, 'events': raw_event['queryEventLog']}
                query_events_logs.append(log)

            if raw_event['class'] == 'dev.dres.run.eventstream.QueryResultLogEvent':
                log = {'name': team_name, 'timestamp': event_timestamp,
                                    'logs': raw_event['queryResultLog']}
                query_result_and_events_logs.append(log)

    teams_with_logs = set([q['name'] for q in query_result_and_events_logs])
    print(f"systems that submitted res and events logs (QueryResultLogEvent): {teams_with_logs}")
    print(f"systems that submitted  QueryEventLogEvent: {set([q['name'] for q in query_events_logs])}")
    print(" TODO: save the query_result_and_events_logs in the desired format, e.g. a dataframe for each team? ") #TODO

    teams_df={}#dic of dataframes
    for team in teams_with_logs:
        log_list=[log for log in query_result_and_events_logs if log['name']==team]
        df=pd.DataFrame(log_list)
        teams_df[team]= df

   # print(len(teams_df))




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Read Logs (query events + results) only for KIS tasks from raw DRES files')
    parser.add_argument('--raw_event_log_folder', default='../data/2022/raw event logs')
    parser.add_argument('--audits_file', default='../data/2022/audits.json')
    parser.add_argument('--run_file', default='../data/2022/run.json')
    parser.add_argument('--v3c_fps_file', default='../data/v3c1_2_fps.csv')
    parser.add_argument('--v3c_segments_files', nargs='+',
                        default=['../data/v3c1_frame_segments.csv', '../data/v3c2_frame_segments.csv'])

    args = parser.parse_args()
    main(args)





    # Not useful anymore--but not sure!#
    # task_uid = [t.uid for task_name, t in kis_tasks.items()]  # {t.uid: task_name for task_name,t in kis_tasks.items()}
    # teams_list=run['description']['teams']
    # teams_uid_from_run_dic={}
    # teams_users_from_run_dic={}
    # for t in teams_list:
    #     team_name=t['name']
    #     users=t['users']
    #     teams_uid_from_run_dic[t['uid']['string']]=team_name
    #     for user in users:
    #         teams_users_from_run_dic[user['string']]=team_name

    # for task_name, t in kis_tasks.items():
    #     start_tasks.append(int(t.started))
    #     end_tasks.append(int(t.ended))
    #     competition_start = min(start_tasks)
    #     competition_end = max(end_tasks) + 300000  # adding 5 minutes to the end of the last KIS task for submitting logs