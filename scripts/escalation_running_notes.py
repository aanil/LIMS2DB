#!/usr/bin/env python

"""Script to copy comments from the AggregateQC step in LIMS as
running notes to statusdb. Also notifies project coordinators

Should be run atleast daily as a cronjob
"""


import genologics_sql.tables as tbls
from genologics_sql.utils import get_session
from sqlalchemy import text
from sqlalchemy.orm import aliased
import datetime
import argparse
import os
from statusdb.db.utils import load_couch_server
from LIMS2DB.utils import send_mail
import markdown


def main(args):

    session = get_session()
    couch = load_couch_server(args.conf)
    db = couch['running_notes']

    def get_researcher(userid):
        query = "select rs.* from principals pr \
                    inner join researcher rs on rs.researcherid=pr.researcherid \
                    where principalid=:pid;"
        return session.query(tbls.Researcher).from_statement(text(query)).params(pid=userid).first()

    def make_esc_running_note(researcher, reviewer, comment, date, processid, project, step_name, review_ask):
        created_time = date.astimezone(datetime.timezone.utc)
        lims_link = "[LIMS](https://ngi-lims-prod.scilifelab.se/clarity/work-complete/{0})".format(processid)
        researcher_name = f"{researcher.firstname} {researcher.lastname}"

        if reviewer:
            reviewer_name = f"{reviewer.firstname} {reviewer.lastname}"
        if review_ask:
            comment_detail = f'**{researcher_name} asked for review from {reviewer_name}**'
            categories = ['Lab']
        else:
            comment_detail = f'**Reviewer {researcher_name} replied**'
            categories = ['Administration', 'Decision']
        newNote = {
                    '_id': f'P{project}:{datetime.datetime.timestamp(created_time)}',
                    'user': researcher_name,
                    'email': researcher.email,
                    'note': f"Comment from {step_name} ({lims_link}) ({comment_detail}): \n{comment}",
                    'categories': categories,
                    'note_type': 'project',
                    'parent': f'P{project}',
                    'created_at_utc': created_time.isoformat(),
                    'updated_at_utc': created_time.isoformat(),
                    'projects': [f'P{project}']
                    }
        return newNote
    
    def update_note_db(note):
        updated = False
        note_existing = db.get(note['_id'])

        if note_existing:
            dict_note = dict(note_existing)
            del dict_note['_rev']
            if not dict_note==note:
                db.save(note)
                updated = True
        else:
            db.save(note)
            updated = True

        return updated

    def email_proj_coord(project, note, date):
        res = session.query(tbls.Project.name, tbls.Project.ownerid).filter(tbls.Project.projectid==project).first()
        proj_coord = get_researcher(res.ownerid)

        time_in_format = datetime.datetime.strftime(date, "%a %b %d %Y, %I:%M:%S %p")

        html = ('<html>'
            '<body>'
            '<p>'
            f'A note has been created from LIMS in the project P{project}, {res.name}! The note is as follows</p>'
            '<blockquote>'
            '<div class="panel panel-default" style="border: 1px solid #e4e0e0; border-radius: 4px;">'
            '<div class="panel-heading" style="background-color: #f5f5f5; padding: 10px 15px;">'
                f'<a href="#">{note["user"]}</a> - <span>{time_in_format}</span> <span>{", ".join(note.get("categories"))}</span>'
            '</div>'
            '<div class="panel-body" style="padding: 15px;">'
                f'<p>{markdown.markdown(note.get("note"))}</p>'
            '</div></div></blockquote></body></html>')

        send_mail(f'[LIMS] Running Note:P{project}, {res.name}', html, proj_coord.email)
 
    esc = aliased(tbls.EscalationEvent)
    sa = aliased(tbls.Sample)
    piot = aliased(tbls.ProcessIOTracker)
    asm = aliased(tbls.artifact_sample_map)

    #Assumed that it runs atleast once daily
    yesterday = datetime.date.today() - datetime.timedelta(days = 1)

    # get aggregate QC comments for running notes
    escalations = session.query(
        esc, sa
        ).join(
            piot, piot.processid == esc.processid
        ).join(
            asm, piot.inputartifactid == asm.columns['artifactid']
        ).join(
            sa, sa.processid == asm.columns['processid']
        ).distinct(
            esc.eventid, sa.projectid
        ).filter(
            esc.lastmodifieddate>f'{yesterday}'
        ).all()

    for (escalation, sample) in escalations:
        step_name = session.execute('select ps.name '
                                    'from escalationevent esc, process pr, protocolstep ps '
                                    'where esc.processid=pr.processid and pr.protocolstepid=ps.stepid '
                                    f'and esc.processid={escalation.processid};'
                                    ).first()[0]
        owner = get_researcher(escalation.ownerid)
        reviewer = get_researcher(escalation.reviewerid)
        escnote = make_esc_running_note(owner, reviewer, escalation.escalationcomment, escalation.escalationdate, 
                                        escalation.processid, sample.projectid, step_name, True)

        if update_note_db(escnote):
            email_proj_coord(sample.projectid, escnote, escalation.escalationdate)

        if escalation.reviewdate:
            if not escalation.reviewcomment:
                comment= '[No comments]'
            else:
                comment = escalation.reviewcomment
            revnote = make_esc_running_note(reviewer, None, comment, escalation.reviewdate, escalation.processid, 
                                            sample.projectid, step_name,False)
            if update_note_db(revnote):
                email_proj_coord(sample.projectid, revnote, escalation.reviewdate)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync the comments made in aggregate QC to project running notes')
    parser.add_argument("-c", "--conf", default=os.path.join(os.environ['HOME'], 'conf/LIMS2DB/post_process.yaml'),
                        help="Config file.  Default: ~/conf/LIMS2DB/post_process.yaml")
    args = parser.parse_args()

    main(args)
