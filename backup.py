import io
import glob
import ftplib
from datetime import datetime
import json
import os
import zipfile
from raven import Client

import requests
import slumber

import config

sentry_client = Client(config.SENTRY_DSN)
TEMPDIR = '/Users/syphar/tmp/_backup/' #tempfile.mkdtemp()

_api_cache = None


def _api():
    global _api_cache
    if not _api_cache:
        _session = requests.Session()
        _session.auth = (config.CLOSEIO_API_KEY, "")
        _session.verify = True

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
        print "backup {} to {}".format(
            fn.__self__._store['base_url'],
            filename,
        )

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

        print '\twrote {} records'.format(count)

    except:
        sentry_client.captureException()
        raise


def doit():
    backup('lead.json', _api().lead.get)
    backup('contact.json', _api().contact.get)
    backup('opportunity.json', _api().opportunity.get)
    backup('task.json', _api().task.get)
    backup('status_lead.json', _api().status.lead.get)
    backup('status_opportunity.json', _api().status.opportunity.get)
    backup('email_template.json', _api().email_template.get)

    print "creating zipfile"
    zipfilename = os.path.join(TEMPDIR, datetime.now().isoformat() + ".zip")
    with zipfile.ZipFile(zipfilename, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for file in glob.glob(os.path.join(TEMPDIR, "*.json")):
            zf.write(
                file,
                os.path.basename(file)
            )

            os.remove(file)

    print "uploading to FTP-server"
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
    doit()

