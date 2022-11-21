import tqdm
from common.load import load_data
import generate
import argparse
import yaml
import argparse

from common.logs import TeamLogs

import logging
logging.basicConfig(level=logging.DEBUG)

def main(args):
    # load config file for this plot
    with open(args.config, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    # load competition data
    teams = cfg["teams"]
    competition_data = load_data(
        teams,
        cfg['audits_file'],
        cfg['run_file'],
        args.v3c_fps_file,
        args.v3c_segments_files)
    competition_data['config'] = cfg
    competition_data['args'] = args
    
    plot_cfgs = [c for c in cfg["generate"] if c["name"] in args.graphs]

    # create or load logs, for each team
    logs = {}
    for team in tqdm.tqdm(teams, desc='Loading (or generating) intermediate DataFrames'):
        team_log = TeamLogs(
            competition_data, 
            team,
            max_records=10000, 
            use_cache=args.log_cache, 
            cache_path='cache/team_logs')
        logs[team] = team_log

    # generate results
    for plot_cfg in tqdm.tqdm(plot_cfgs, desc='Generating plots'):
        result = eval(plot_cfg["function"])(competition_data, teams, logs, use_cache=args.result_cache)
        result.generate_and_render(plot_cfg["generate_args"], plot_cfg["render_args"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform evaluation on test set')
    parser.add_argument('graphs', nargs='+', default=['time_recall_table'], help="Name of the plots to render (see in config.yaml for the names that can be used)")
    parser.add_argument('--config', default='config2022.yaml', help='config file to generate the graph')
    parser.add_argument('--v3c_segments_files', nargs='+', default=['data/v3c1_frame_segments.csv', 'data/v3c2_frame_segments.csv'])
    parser.add_argument('--v3c_fps_file', default='data/v3c1_2_fps.csv')
    parser.add_argument('--no_log_cache', action='store_true', help='Wether to use the log cache from each team')
    parser.add_argument('--no_result_cache', action='store_true', help='Wether to use the result cache for rendering results')

    args = parser.parse_args()
    args.result_cache = not args.no_result_cache
    args.log_cache = not args.no_log_cache
    main(args)
