from flask import Flask
from raven.contrib.flask import Sentry

import config

app = Flask(__name__)
app.config['SENTRY_DSN'] = config.SENTRY_DSN
sentry = Sentry(app)


@app.route('/')
def hello_world():
    return 'Hello World!'
