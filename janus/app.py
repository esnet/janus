import os
import signal
import platform
import argparse
import logging.config
from configparser import ConfigParser

# XXX Temporary fix for https://github.com/jarus/flask-testing/issues/143
#import werkzeug
#werkzeug.cached_property = werkzeug.utils.cached_property

# XXX More monkeypatching
#import flask.scaffold
#flask.helpers._endpoint_from_view_func = flask.scaffold._endpoint_from_view_func

from flask_restx import Api
from flask import Flask, Blueprint
from flask_sock import Sock

from janus.api.controller import ns as controller_ns
from janus.api.agent import ns as agent_ns
from janus import settings
from janus.settings import cfg
from janus.api.db import DBLayer
from janus.api.profile import ProfileManager
from janus.api.manager import ServiceManager
from janus.api.sockets import handle_websocket


app = Flask(__name__)
sock = Sock(app)

try:
    logging.config.fileConfig(settings.LOG_CFG_PATH)
except:
    logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), 'config/logging.conf'))
    logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__name__)

def parse_config(fpath):
    parser = ConfigParser(allow_no_value=True)

    parser.read(fpath)
    if 'JANUS' not in parser.sections():
        log.warning("No configuration sections found in {}".format(fpath))
        return

    config = parser['JANUS']
    try:
        cfg.PORTAINER_URI = str(config.get('PORTAINER_URI', None))
        cfg.PORTAINER_WS = str(config.get('PORTAINER_WS', None))
        cfg.PORTAINER_USER = str(config.get('PORTAINER_USER', None))
        cfg.PORTAINER_PASSWORD = str(config.get('PORTAINER_PASSWORD', None))
        vssl = str(config.get('PORTAINER_VERIFY_SSL', True))
        if vssl == 'False':
            cfg.PORTAINER_VERIFY_SSL = False
            import urllib3
            urllib3.disable_warnings()
        else:
            cfg.PORTAINER_VERIFY_SSL = True
    except Exception as e:
        raise AttributeError('Config file parser error: {}'.format(e))


def register_api(name, title, version, desc, prefix, nslist):
    blueprint = Blueprint(name, __name__, url_prefix=prefix)
    api = Api(blueprint,
              title=title,
              version=version,
              description=desc
              )
    app.register_blueprint(blueprint)
    for n in nslist:
        api.add_namespace(n)
    return api

def init(app):
    api = register_api("Janus", "The ESnet Janus container API", "0.1",
                       "REST endpoints for container provisioning and tuning",
                       settings.API_PREFIX, [])
    if (cfg.is_agent):
        api.add_namespace(agent_ns)
    if (cfg.is_controller):
        api.add_namespace(controller_ns)

    @sock.route("/ws")
    def WebSocket(sock):
        handle_websocket(sock)

def main():
    parser = argparse.ArgumentParser(description='Janus Controller/Agent')
    parser.add_argument('-b', '--bind', type=str, default='127.0.0.1',
                        help='Bind to IP address (default: 127.0.0.1)')
    parser.add_argument('-p', '--port', type=int, default=5000,
                        help='Listen on port (default: 5000)')
    parser.add_argument('--ssl', action='store_true', default=False,
                        help='Use SSL')
    parser.add_argument('-H', '--host', type=str, help='Remote Provisioning URI ')
    parser.add_argument('-C', '--controller', action='store_true', default=False,
                        help='Run as Controller')
    parser.add_argument('-A', '--agent', action='store_true', default=False,
                        help='Run as Tuning Agent')
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help='Perform config provisioning but do not create containers')
    parser.add_argument('-f', '--config', type=str, default=settings.DEFAULT_CFG_PATH,
                        help='Path to configuration file')
    parser.add_argument('-P', '--profiles', type=str, default=settings.DEFAULT_PROFILE_PATH,
                        help='Path to profile directory')
    parser.add_argument('-db', '--database', type=str, default=settings.DEFAULT_DB_PATH)
    args = parser.parse_args()

    parse_config(args.config)

    if args.controller:
        try:
            # Setup the Database Layer
            db = DBLayer(path=args.database)
            # Setup the Profile Manager
            pm = ProfileManager(db, args.profiles)
            # Setup the Service Manager
            sm = ServiceManager(db)
            # Save handles to these in our global config class
            cfg.setdb(db, pm, sm)
            # Read all profiles at startup
            cfg.pm.read_profiles()
        except Exception as e:
            import traceback
            traceback.print_exc()
            log.error(e)
            exit(1)
        cfg._controller = True
    if args.agent:
        cfg._agent = True
    if args.dryrun:
        cfg._dry_run = True

    # signal closure for re-reading profiles
    def sighup_handler(signum, frame):
        if args.controller:
            log.info(f"Caught HUP signal, reading profiles at {args.profiles}")
            cfg.db.read_profiles(refresh=True)
    signal.signal(signal.SIGHUP, sighup_handler)

    log.info('Starting development Janus Server at http://{}{}'.format(platform.node(),
                                                                       settings.API_PREFIX))
    log.info("Using database file {}".format(cfg.get_dbpath()))

    init(app)
    if settings.FLASK_DEBUG:
        app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

    ssl = 'adhoc' if args.ssl else None
    app.run(host=args.bind, port=args.port, ssl_context=ssl,
            debug=settings.FLASK_DEBUG, threaded=True)

if __name__ == '__main__':
    main()
