import io
import glob
import ftplib
from datetime import datetime
import json
import os
import tempfile
import tarfile
from raven import Client

import requests
from requests.adapters import HTTPAdapter
import slumber

import config

sentry_client = Client(config.SENTRY_DSN)
TEMPDIR = tempfile.mkdtemp()

_api_cache = None


def _api():
    global _api_cache
    if not _api_cache:
        _session = requests.Session()
        _session.auth = (config.CLOSEIO_API_KEY, "")
        _session.verify = True
        _session.mount('http://', HTTPAdapter(max_retries=5))
        _session.mount('https://', HTTPAdapter(max_retries=5))

        _api_cache = slumber.API(
            'https://app.close.io/api/v1/',
            session=_session
        )

    return _api_cache


# handles pagination in the closeio-api:
def _data_iter(func, *args, **kwargs):
    skip = 0
    limit = 100

    while True:
        kwargs['_skip'] = skip
        kwargs['_limit'] = limit

        response = func(*args, **kwargs)

        for item in response['data']:
            yield item

        if not response['has_more']:
            break

        else:
            skip += limit


def backup(filename, fn):
    try:
        filename = os.path.join(TEMPDIR, filename)
        print("backup {} to {}".format(
            fn.__self__._store['base_url'],
            filename,
        ))

        if os.path.exists(filename):
            os.remove(filename)

        with io.open(filename, 'w', encoding='utf-8') as output_file:
            output_file.write(u"[\n")

            first = True

            count = 0
            for item in _data_iter(fn):
                if first:
                    first = False
                else:
                    output_file.write(u',\n')

                output_file.write(unicode(json.dumps(
                    item,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=4
                )))

                count += 1

            output_file.write(u"\n]\n")

        print('\twrote {} records').format(count)

    except:
        sentry_client.captureException()


def doit():
    backup('lead.json', _api().lead.get)
    backup('contact.json', _api().contact.get)
    backup('activity.json', _api().activity.get)
    backup('activity_note.json', _api().activity.note.get)
    backup('activity_email.json', _api().activity.email.get)
    backup('activity_emailthread.json', _api().activity.emailthread.get)
    backup(
        'activity_statuschange_lead.json',
        _api().activity.status_change.lead.get,
    )
    backup(
        'activity_statuschange_opportunity.json',
        _api().activity.status_change.opportunity.get,
    )
    backup('activity_call.json', _api().activity.call.get)
    backup('opportunity.json', _api().opportunity.get)
    backup('task.json', _api().task.get)
    backup('status_lead.json', _api().status.lead.get)
    backup('status_opportunity.json', _api().status.opportunity.get)
    backup('email_template.json', _api().email_template.get)

    print("creating compressed file")
    zipfilename = os.path.join(TEMPDIR, datetime.now().isoformat() + ".tar.gz")
    with tarfile.open(zipfilename, "w:gz") as tar:
        for file in glob.glob(os.path.join(TEMPDIR, "*.json")):
            tar.add(
                file,
                arcname=os.path.basename(file),
            )
            os.remove(file)

    print("uploading to FTP-server")
    session = ftplib.FTP_TLS(
        config.FTP_SERVER,
        config.FTP_USER,
        config.FTP_PASSWORD,
    )

    try:
        with open(zipfilename) as zf:
            session.storbinary('STOR ' + os.path.basename(zipfilename), zf)

    finally:
        session.quit()

    os.remove(zipfilename)


if __name__ == '__main__':
    try:
        doit()
    except BaseException:
        sentry_client.captureException()
        raise
