import LIMS2DB.diff as df
import datetime
import argparse
import os
from statusdb.db.utils import load_couch_server



def main(args):
    couch = load_couch_server(args.conf)

    if args.pj_id:
        diffs=df.diff_project_objects(args.pj_id, couch, args.log)

    else:
        proj_db = couch['projects']
        view = proj_db.view('project/project_id')
        for row in view:
            diffs=df.diff_project_objects(row.key, couch, args.log)

    with open(args.resultfile, 'w') as f:
        for p in diffs:
            if diffs[p]:
                f.write("Project {} :\n".format(p))
            for d in diffs[p]:
                f.write(" {} : was {}, is {}\n".format(d, diffs[p][d][0], diffs[p][d][1]))





if __name__ == "__main__":


    
    parser = argparse.ArgumentParser(description='Compare the results of the installed PSUL with the contents of the DB')
    parser.add_argument('--conf', dest="conf", default=os.path.expanduser("~/opt/config/post_process.yaml"),
                               help='configuration file path. default is ~/opt/config/post_process.yaml')
    parser.add_argument('--log', '-l', dest="log", default=os.path.expanduser("~/psul_validation.log"),
                               help='log file path. default is ~/psul_validation.log')
    parser.add_argument('--result', '-r', dest="resultfile", default=os.path.expanduser("~/psul_validations/{}_psul_validation.out".format(datetime.datetime.now().isoformat())),
                               help='validation output path. default is ~/psul_validations/{date}_psul_validation.out')
    parser.add_argument('--project', '-p', dest='pj_id',help='project id to perform the check')

    args = parser.parse_args()


    main(args)
