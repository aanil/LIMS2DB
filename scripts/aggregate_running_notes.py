import genologics_sql.tables as tbls
from genologics_sql.utils import get_session
from sqlalchemy import text
import datetime
import argparse


def main(args):
    session = get_session()
    project_nr = args.project.replace('P', '')
    int(project_nr)
    # get aggregate QC comments for running notes
    query = "select distinct esc.* from escalationevent esc \
            inner join processiotracker piot on piot.processid=esc.processid \
            inner join artifact_sample_map asm on piot.inputartifactid=asm.artifactid \
            inner join sample sa on sa.processid=asm.processid \
            where sa.projectid = {pjid};".format(pjid=project_nr)
    escalations = session.query(tbls.EscalationEvent).from_statement(text(query)).all()

    def make_esc_running_note(researcher, comment, date):
        timestamp = datetime.datetime.strftime(date, '%Y-%m-%d %H:%M:%S')
        newNote = {'user': "{0} {1}".format(researcher.firstname, researcher.lastname),
                   'email': researcher.email,
                   'note': comment,
                   'category': 'Lab',
                   'timestamp': timestamp}
        return newNote

    for esc in escalations:
        query = "select rs.* from principals pr \
                 inner join researcher rs on rs.researcherid=pr.researcherid \
                 where principalid=:pid;"
        owner = session.query(tbls.Researcher).from_statement(text(query)).params(pid=esc.ownerid).first()
        escnote = make_esc_running_note(owner, esc.escalationcomment, esc.escalationdate)
        print(escnote)

        if esc.reviewcomment:
            reviewer = session.query(tbls.Researcher).from_statement(text(query)).params(pid=esc.reviewerid).first()
            revnote = make_esc_running_note(reviewer, esc.reviewcomment, esc.reviewdate)
            print(revnote)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync the comments made in aggregate QC to running notes UDFs')
    parser.add_argument('--project', '-p', help='Only run for the given specific project id in PNNNN form.')
    args = parser.parse_args()

    main(args)
