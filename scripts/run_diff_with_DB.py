import LIMS2DB.diff as df
import datetime
import argparse
import os
import random
from statusdb.db.utils import load_couch_server


def write_results_to_file(diffs, args):
    with open(args.resultfile, 'w') as f:
        for p, diff in diffs.items():
            if diffs[p]:
                f.write("Project {} :\n".format(p))
            for d in diff[0]:
                f.write(" {} : was {}, is {}\n".format(d, diff[0], diff[0]))


def main(args):
    couch = load_couch_server(args.conf)
    diffs = {}
    if args.pj_id:
        diffs[args.pj_id] = df.diff_project_objects(args.pj_id, couch, args.log)

    elif args.random:
        random.seed()
        closed_ids = []
        proj_db = couch['projects']
        view = proj_db.view('project/summary')
        for row in view[['closed', '']:['closed', 'ZZZZZZZZ']]:
            if row.value.get('open_date', '0') > '2014-06-01':
                closed_ids.append(row.key[1])
        nb = int(len(closed_ids)/10)
        picked_ids = random.sample(closed_ids, nb)
        for one_id in picked_ids:
            diffs[one_id] = df.diff_project_objects(one_id, couch, args.log)
    else:
        proj_db = couch['projects']
        view = proj_db.view('project/project_id')
        for row in view:
            diffs[row.key] = df.diff_project_objects(row.key, couch, args.log)

    write_results_to_file(diffs, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare the results of the installed PSUL with the contents of the DB')
    parser.add_argument('--conf', dest="conf", default=os.path.expanduser("~/opt/config/post_process.yaml"),
                        help='configuration file path. default is ~/opt/config/post_process.yaml')
    parser.add_argument('--log', '-l', dest="log", default=os.path.expanduser("~/psul_validation.log"),
                        help='log file path. default is ~/psul_validation.log')
    parser.add_argument('--result', '-r', dest="resultfile", default=os.path.expanduser("~/psul_validations/{}_psul_validation.out".format(datetime.datetime.now().isoformat())),
                        help='validation output path. default is ~/psul_validations/{date}_psul_validation.out')
    parser.add_argument('--project', '-p', dest='pj_id', help='project id to perform the check')
    parser.add_argument('--randomsample', '-s', action="store_true", dest='random', help='pick a random subset of projects to check')

    args = parser.parse_args()

    main(args)
