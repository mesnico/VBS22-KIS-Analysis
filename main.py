from utils.load import load_data
import generate
import argparse
import yaml
import argparse

def main(args):
    # load config file for this plot
    with open(args.config, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    # load data
    data = load_data(
        args.audits_file, 
        args.run_file, 
        args.teams_metadata_file, 
        args.v3c_segments_files)

    # generate results
    teams = cfg["teams"]
    result = eval(cfg["generate"]["function"])(data, teams)
    result.generate_and_render(cfg["generate"]["args"])    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform evaluation on test set')
    parser.add_argument('config', help='config file to generate the graph')
    parser.add_argument('--audits_file', default='data/audits.json')
    parser.add_argument('--run_file', default='data/run.json')
    parser.add_argument('--teams_metadata_file', default='data/teams_metadata.json')
    parser.add_argument('--v3c_segments_files', nargs='+', default=['data/v3c1_frame_segments.csv'])

    args = parser.parse_args()
    main(args)
