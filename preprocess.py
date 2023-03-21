from common.load import load_competition_data, process_team_logs
import argparse

from common.logs import TeamLogs

import logging
logging.basicConfig(level=logging.DEBUG)

def main(args):
    # load competition data from dres files and auxiliary data (FPSs, sequences)
    comp_data = load_competition_data(args.config)

    # compute team logs and put them in the form of dataframes
    process_team_logs(args.config, comp_data, force=args.force)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform evaluation on test set')
    parser.add_argument('--config', default='config_vbs2023.yaml', help='config file to generate the graph')
    parser.add_argument('--force', action='store_true', help='wether to force re-creation of all the dataframes')

    args = parser.parse_args()
    main(args)
