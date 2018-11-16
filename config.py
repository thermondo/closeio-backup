"""Settings for backup script."""
import os

SENTRY_DSN = os.getenv('SENTRY_DSN')
CLOSEIO_API_KEY = os.getenv("CLOSEIO_API_KEY")

FTP_SERVER = os.getenv('FTP_SERVER')
FTP_USER = os.getenv('FTP_USER')
FTP_PASSWORD = os.getenv('FTP_PASSWORD')
