#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ec_request_handler import *

from cs_main import *

import random
import sys
import locale
import argparse
import multiprocessing
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

    @staticmethod
    def start_conspi_suck():
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

        # list of arguments
        l_parser = argparse.ArgumentParser(description='Launch ConspiSuck server and threads/processes')
        l_parser.add_argument('--likes-proc', type=int, help='Process count for likes download', default=1)
        l_parser.add_argument('--ocr-proc', type=int, help='Process count for image ocr', default=1)
        l_parser.add_argument('--get-pages', help='Execute get_pages() (default: false)', action='store_true')

        # dummy class to receive the parsed args
        class C:
            def __init__(self):
                self.likes_proc = 1
                self.ocr_proc = 1
                self.get_pages = False

        # do the argument parse
        c = C()
        l_parser.parse_args()
        l_parser.parse_args(namespace=c)

        try:
            # instantiate the app (and the connection pool within it)
            l_app = CsApp(c.likes_proc, c.ocr_proc, c.get_pages)
        except Exception as e:
            EcMailer.send_mail('App class failed to instantiate.', repr(e))
            sys.exit(0)

        # select the correct process launch method to avoid SSL issues in Psycopg2
        multiprocessing.set_start_method('spawn')

        # give one-letter name to current process
        multiprocessing.current_process().name = 'M'

        l_app.start_processes()

        # Set-up mailer & EcLogger
        GlobalStart.basic_env_start()
        l_app.full_init()

        # test the logging system by displaying the parameters
        EcLogger.cm_logger.info('c.likes_proc : {0}'.format(c.likes_proc))
        EcLogger.cm_logger.info('c.ocr_proc   : {0}'.format(c.ocr_proc))

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

        # start all threads (the app's own maintenance thread and the background tasks threads)
        l_app.start_threads()

        try:
            # start server main loop
            l_httpd.serve_forever()
        except Exception as e:
            EcLogger.cm_logger.critical('App crashed. Error: {0}-{1}'.format(type(e).__name__, repr(e)))

# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    StartApp.start_conspi_suck()
