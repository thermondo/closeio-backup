#!/usr/bin/env python
"""Create backup of a Close.io organization and store it on AWS S3."""
import io
import json
import os
import tarfile
import tempfile
import time

import boto3
import requests
import slumber
from raven import Client
from requests.adapters import HTTPAdapter
from slumber.exceptions import HttpServerError

AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET')
CLOSEIO_API_KEY = os.getenv('CLOSEIO_API_KEY')
SENTRY_DSN = os.getenv('SENTRY_DSN')


def get_closeio_api():
    """Return slumber API client for Close.io API."""
    _session = requests.Session()
    _session.auth = (CLOSEIO_API_KEY, "")
    _session.verify = True
    _session.mount('http://', HTTPAdapter(max_retries=5))
    _session.mount('https://', HTTPAdapter(max_retries=5))

    _api_cache = slumber.API(
        'https://app.close.io/api/v1/',
        session=_session
    )

    return _api_cache


def _data_iter(func, *args, **kwargs):
    """Handle Close.io API pagination."""
    skip = 0
    limit = 100
    retries = 0

    while True:
        kwargs['_skip'] = skip
        kwargs['_limit'] = limit

        try:
            response = func(*args, **kwargs)
        except HttpServerError:
            if retries < 10:
                time.sleep(min(600, 2 ** retries))
                retries += 1
                continue
            raise
        else:
            retries = 0

        for item in response['data']:
            yield item

        if not response['has_more']:
            break

        else:
            skip += limit


def backup(tarball: tarfile.TarFile, filename, fn):
    """Create JSON dump and add to tarball."""
    with io.BytesIO() as output_file:
        tar_info = tarfile.TarInfo(filename)

        print(f"Adding {filename} to archive...", end=' ')

        output_file.write(b"[\n")

        for item, count in enumerate(_data_iter(fn)):
            if count != 0:
                output_file.write(b",")

            output_file.write(
                json.dumps(
                    item,
                    ensure_ascii=False,
                ).encode()
            )

        output_file.write(b"\n]\n")

        output_file.seek(0)
        tar_info.size = output_file.tell()
        tarball.addfile(tar_info, output_file)

        print("Done!")


def main():
    """Create backup and upload to AWS S3."""
    tmp_dir = tempfile.mkdtemp()
    filename = time.strftime('%Y-%m-%d.tar.gz')
    archive_path = os.path.join(tmp_dir, filename)
    api = get_closeio_api()
    with tarfile.open(archive_path, "w:gz") as tarball:
        backup(tarball, 'lead.json', api.lead.get)
        backup(tarball, 'contact.json', api.contact.get)
        backup(tarball, 'activity.json', api.activity.get)
        backup(tarball, 'activity_note.json', api.activity.note.get)
        backup(tarball, 'activity_email.json', api.activity.email.get)
        backup(
            tarball,
            'activity_emailthread.json',
            api.activity.emailthread.get
        )
        backup(
            tarball,
            'activity_statuschange_lead.json',
            api.activity.status_change.lead.get,
        )
        backup(
            tarball,
            'activity_statuschange_opportunity.json',
            api.activity.status_change.opportunity.get,
        )
        backup(
            tarball,
            'activity_call.json', api.activity.call.get)
        backup(
            tarball,
            'opportunity.json', api.opportunity.get)
        backup(tarball, 'task.json', api.task.get)
        backup(tarball, 'status_lead.json', api.status.lead.get)
        backup(tarball, 'status_opportunity.json', api.status.opportunity.get)
        backup(tarball, 'email_template.json', api.email_template.get)

    print("Uploading to AWS S3 bucket...", end=' ')
    s3 = boto3.resource('s3')

    with open(archive_path) as zf:
        s3.Bucket(AWS_S3_BUCKET).put_object(Key=filename, Body=zf)

    print("Done!")

    print("Cleaning up filesystem...", end=' ')
    os.remove(archive_path)
    print("Done!")


if __name__ == '__main__':
    sentry_client = Client(SENTRY_DSN)
    try:
        main()
    except BaseException:
        sentry_client.captureException()
        raise
