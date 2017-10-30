#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ec_request_handler import *

from cs_main import *

import random
import sys
import locale
from socketserver import ThreadingMixIn


# Multi-threaded HTTP server according to https://pymotw.com/2/BaseHTTPServer/index.html#module-BaseHTTPServer
class ThreadedHTTPServer(ThreadingMixIn, http.server.HTTPServer):
    """
    Handles requests in a separate thread each.
    """


class StartApp:
    """
    This is a simple wrapper around the function starting the application. Everything is static.
    """

    @classmethod
    def start_conspi_suck(cls):
        """
        The actual entry point, called from ``if __name__ == "__main__":``. Does the following:

        #. Initialises the mailer (:any:`EcMailer.init_mailer`)
        #. Test the availability of the database connection.
        #. Initialises the logging system (:any:`EcLogger.log_init`)
        #. Instantiates the application class (:any:`CsApp`). This launches the background stories-downloading
           threads.
        #. Initialises the request handler class (:any:`EcRequestHandler.init_class`), passing it a handle 
           on the application class.
        #. Instantiates the standard Python HTTP server class (:any:`ThreadedHTTPServer`). The request handler
           class will be instantiated by this class and a handle to it is therefore passed to it.
        #. Set up the appropriate locale (for proper date format handling)
        #. Launches the server (the method `serve_forever()` of subclass
           :any:`http.server.HTTPServer` of :py:class:`socketserver.BaseServer`)

        The dependencies between the main application classes is as follows:

        HTTP server: one instance "running forever"
            ↳ request handler: one instance for each HTTP request
                ↳ application: one instance created at startup. Response building methods called by request handler
                    ↳ Background threads:
                        * Health check: launched by application base class (:any:`EcAppCore`)
                        * Stories downloading: launched by app specific subclass (:any:`CsApp`)
        """
        print('EC server starting ...')

        # random generator init
        random.seed()

        # mailer init
        EcMailer.init_mailer()

        # test connection to PostgresQL and wait if unavailable
        while True:
            try:
                l_connect = psycopg2.connect(
                    host=EcAppParam.gcm_dbServer,
                    database=EcAppParam.gcm_dbDatabase,
                    user=EcAppParam.gcm_dbUser,
                    password=EcAppParam.gcm_dbPassword
                )

                l_connect.close()
                break
            except psycopg2.Error as e:
                EcLogger.cm_logger.debug('WAITING: No PostgreSQL yet ... : ' + repr(e))
                EcMailer.send_mail('WAITING: No PostgreSQL yet ...', repr(e))
                time.sleep(1)
                continue

        # logging system init
        try:
            EcLogger.log_init()
        except Exception as e:
            EcMailer.send_mail('Failed to initialize EcLogger', repr(e))
            sys.exit(0)

        try:
            # instantiate the app (and the connection pool within it)
            l_app = CsApp()
        except Exception as e:
            EcLogger.cm_logger.critical('App class failed to instantiate. Error: {0}'.format(repr(e)))
            sys.exit(0)

        # initializes request handler class
        EcRequestHandler.init_class(l_app)

        try:
            # python http server init
            l_httpd = ThreadedHTTPServer(("", EcAppParam.gcm_httpPort), EcRequestHandler)
        except Exception as e:
            EcLogger.cm_logger.critical('Cannot start server at [{0}:{1}]. Error: {2}-{3}'.format(
                EcAppParam.gcm_appDomain,
                EcAppParam.gcm_httpPort,
                type(e).__name__, repr(e)
            ))
            sys.exit(0)

        EcLogger.root_logger().info('locale (LC_CTYPE) : {0}'.format(locale.getlocale(locale.LC_CTYPE)))
        EcLogger.root_logger().info('locale (LC_TIME)  : {0}'.format(locale.getlocale(locale.LC_TIME)))

        l_locale, l_encoding = locale.getlocale(locale.LC_TIME)
        if l_locale is None:
            locale.setlocale(locale.LC_TIME, locale.getlocale(locale.LC_CTYPE))
        EcLogger.root_logger().info('locale (LC_TIME)  : {0}'.format(locale.getlocale(locale.LC_TIME)))

        EcLogger.root_logger().info('gcm_appName       : ' + EcAppParam.gcm_appName)
        EcLogger.root_logger().info('gcm_appVersion    : ' + EcAppParam.gcm_appVersion)
        EcLogger.root_logger().info('gcm_appTitle      : ' + EcAppParam.gcm_appTitle)

        # final success message (sends an e-mail message because it is a warning)
        EcLogger.cm_logger.warning('Server up and running at [{0}:{1}]'
                                   .format(EcAppParam.gcm_appDomain, str(EcAppParam.gcm_httpPort)))

        # start all threads (the app's own maintenance thread and the background tasks thread)
        l_app.start_threads()

        try:
            # start server main loop
            l_httpd.serve_forever()
        except Exception as e:
            EcLogger.cm_logger.critical('App crashed. Error: {0}-{1}'.format(type(e).__name__, repr(e)))

# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    StartApp.start_conspi_suck()
