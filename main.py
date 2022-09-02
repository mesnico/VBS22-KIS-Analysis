import tqdm
from utils.load import load_data
import generate
import argparse
import yaml
import argparse

from utils.team import TeamLogs

def main(args):
    # load config file for this plot
    with open(args.config, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    # load competition data
    competition_data = load_data(
        args.audits_file, 
        args.run_file, 
        args.teams_metadata_file, 
        args.v3c_segments_files)

    teams = cfg["teams"]
    plot_cfgs = [c for c in cfg["generate"] if c["name"] in args.graphs]
    plot_cfg = plot_cfgs[0]

    # create or load logs, for each team
    logs = {}
    for team in tqdm.tqdm(teams):
        team_log = TeamLogs(
            competition_data, 
            team,
            max_records=10000, 
            use_cache=args.log_cache, 
            cache_path='cache/logs')
        logs[team] = team_log

    # generate results
    result = eval(plot_cfg["function"])(competition_data, teams, logs, use_cache=args.result_cache)
    result.generate_and_render(plot_cfg["generate_args"], plot_cfg["render_args"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform evaluation on test set')
    parser.add_argument('graphs', default='time_recall_table', help="Name of the plots to render (see in config.yaml for the names that can be used)")
    parser.add_argument('--config', default='config.yaml', help='config file to generate the graph')
    parser.add_argument('--audits_file', default='data/2021/audits.json')
    parser.add_argument('--run_file', default='data/2021/run.json')
    parser.add_argument('--teams_metadata_file', default='data/teams_metadata.json')
    parser.add_argument('--v3c_segments_files', nargs='+', default=['data/v3c1_frame_segments.csv'])
    parser.add_argument('--no_log_cache', action='store_true', help='Wether to use the log cache from each team')
    parser.add_argument('--no_result_cache', action='store_true', help='Wether to use the result cache for rendering results')

    args = parser.parse_args()
    args.result_cache = not args.no_result_cache
    args.log_cache = not args.no_log_cache
    main(args)
