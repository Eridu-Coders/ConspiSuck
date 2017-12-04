#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ec_utilities import *

import threading
import psutil

__author__ = 'Pavan Mahalingam'


class EcAppCore(threading.Thread):
    """
    Root class of the application core instance. Actual applications must subclass this.
    """

    def __init__(self):
        """
        Perform the following housekeeping tasks:

        * Tests the DB connection by storing a startup message in `TB_EC_MSG`.
        * Starts the health check thread.

        **NB** The health check thread is actually no longer started here but placed under the
        responsibility of the implementation app. The reason fo this is that the app may need to be instantiated
        before it is sensible to start the thread (30/10/2017).
        """
        super().__init__(daemon=True)

        # bogus variables introduced to avoid a PEP-8 pedantic complaint in get_response
        self.m_rq = None
        self.m_pd = None

        #: logger
        self.m_logger = logging.getLogger('AppCore')

        # Add a record to TB_EC_MSG, thus testing the db connection
        l_conn = EcConnectionPool.get_global_pool().getconn('DB Connection test in EcAppCore.__init__()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                insert into "TB_EC_MSG"(
                    "ST_NAME",
                    "ST_LEVEL",
                    "ST_MODULE",
                    "ST_FILENAME",
                    "ST_FUNCTION",
                    "N_LINE",
                    "TX_MSG",
                    "ST_ENV",
                    "DT_MSG"
                )
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                'xxx',
                'XXX',
                'ec_app_core',
                './ec_app_core.py',
                '__init__',
                0,
                '{0} v. {1} starting'.format(
                    EcAppParam.gcm_appName,
                    EcAppParam.gcm_appVersion
                ),
                'PRD' if LocalParam.gcm_prodEnv else 'DEV',
                datetime.datetime.now(tz=pytz.timezone(EcAppParam.gcm_timeZone))
            ))
            l_conn.commit()
        except psycopg2.IntegrityError as e:
            self.m_logger.warning('TB_EC_MSG insert failure - Integrity error: {0}-{1}'.format(
                type(e).__name__,
                repr(e)) +
                '/[{0}] {1}'.format(e.pgcode, e.pgerror)
            )
            raise
        except Exception as e:
            self.m_logger.warning('TB_EC_MSG insert failure: {0}-{1}'.format(
                type(e).__name__,
                repr(e)
            ))
            raise

        l_cursor.close()
        EcConnectionPool.get_global_pool().putconn(l_conn)
        self.m_logger.info('Successful TB_EC_MSG insert - The DB appears to be working')

        #: health check counter. Number of calls to :any:`EcConnectionPool.getconn`
        self.m_hcCounter = 0

        #: One letter thread name (member inherited from thread class :any:`threading.Thread`)
        self.name = 'H'

        # Thread start placed under the responsibility of the implementation app (30/06/2017)
        # self.start()

    def get_response_post(self, p_request_handler, p_post_data):
        """
        Main application entry point - App response to an HTTP POST request (must be reimplemented by actual app)

        :param p_request_handler: The request handler (see :any:`EcRequestHandler`) containing the request info.
        :param p_post_data: The post data received from the caller.
        :return: A warning JSON string indicating that this method should not be called directly but be subclassed.
        """
        # completely useless 2 lines. Only there to avoid PEP-8 pedantic complaint
        self.m_rq = p_request_handler
        self.m_pd = p_post_data

        return '{"status":"FAIL", "message":"You should never see this. If you do then things are really wrong"}'

    def get_response_get(self, p_request_handler):
        """
        Main application entry point - App response to an HTTP GET request (must be reimplemented by actual app)

        :param p_request_handler: The request handler (see :any:`EcRequestHandler`) containing the request info.
        :return: A warning HTML string indicating that this method should not be called directly but be subclassed.
        """
        # completely useless line. Only there to avoid PEP-8 pedantic complaint
        self.m_rq = p_request_handler

        return """
            <html>
                <head></head>
                <body>
                    <p style="color: red;">You should never see this! There is a serious problem here ....</p>
                </body>
            </html>
        """

    # ------------------------- System health test ---------------------------------------------------------------------
    def check_system_health(self):
        """
        Every 30 sec., checks memory usage and issues a warning if over 75% and produces a full connection pool
        status report.

        Every tenth time (once in 5 min.) a full recording of system parameters is made through
        `psutil <http://psutil.readthedocs.io/en/latest/>`_ and stored in `TB_MSG`.
        """

        # builds a thread list representation string of the form: XXXXX-aaaaa/bbbbb/cccc where the Xs are
        # the one-letter names of the application's threads and aaaaa, bbbbb, ... are the names of any other
        # threads, if any (there should not be). The main thread of the application is represented as 'µ'
        l_thread_list_letter = []
        l_thread_list_other = []
        for t in threading.enumerate():
            if t.name == 'MainThread':
                l_thread_list_letter.append('µ')
            elif len(t.name) == 1:
                l_thread_list_letter.append(t.name)
            else:
                l_thread_list_other.append(t.name)
        l_thread_list_letter.sort()
        l_thread_list_other.sort()
        l_thread_list = '[{0}]-[{1}]'.format(''.join(l_thread_list_letter), '/'.join(l_thread_list_other))

        # get list of memory metrics from psutil (http://psutil.readthedocs.io/en/latest/#memory)
        l_mem = psutil.virtual_memory()

        # display available ram + thread list
        self.m_logger.info(('System Health Check - Available RAM: {0:.2f} Mb ({1:.2f} % usage) ' +
                            'Threads: {2}').format(
            l_mem.available / (1024 * 1024), l_mem.percent, l_thread_list))

        # if used RAM over 75% --> issue warning
        if l_mem.percent >= 75.0:
            self.m_logger.warning('System Health Check ALERT - Available RAM: {0:.2f} Mb ({1:.2f} % usage)'.format(
                l_mem.available / (1024 * 1024), l_mem.percent))

        # full system resource log into TB_EC_MSG every 5 minutes (once out of 10 calls)
        if self.m_hcCounter % 10 == 0:
            l_cpu = psutil.cpu_times()
            l_swap = psutil.swap_memory()
            l_disk_root = psutil.disk_usage('/')
            l_net = psutil.net_io_counters()
            l_process_count = len(psutil.pids())

            # log message in TB_EC_MSG
            l_conn = psycopg2.connect(
                host=EcAppParam.gcm_dbServer,
                database=EcAppParam.gcm_dbDatabase,
                user=EcAppParam.gcm_dbUser,
                password=EcAppParam.gcm_dbPassword
            )
            l_cursor = l_conn.cursor()
            try:
                l_cursor.execute("""
                    insert into "TB_EC_MSG"(
                        "ST_TYPE",
                        "ST_NAME",
                        "ST_LEVEL",
                        "ST_MODULE",
                        "ST_FILENAME",
                        "ST_FUNCTION",
                        "N_LINE",
                        "TX_MSG",
                        "ST_ENV",
                        "DT_MSG"
                    )
                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    'HLTH',
                    'xxx',
                    'XXX',
                    'ec_app_core',
                    './ec_app_core.py',
                    'check_system_health',
                    0,
                    'MEM: {0}/CPU: {1}/SWAP: {2}/DISK(root): {3}/NET: {4}/PROCESSES: {5}'.format(
                        l_mem, l_cpu, l_swap, l_disk_root, l_net, l_process_count
                    ),
                    'PRD' if LocalParam.gcm_prodEnv else 'DEV',
                    datetime.datetime.now(tz=pytz.timezone(EcAppParam.gcm_timeZone))
                ))
                l_conn.commit()
            except Exception as e:
                EcMailer.send_mail('TB_EC_MSG insert failure: {0}-{1}'.format(
                    type(e).__name__,
                    repr(e)
                ), 'Sent from EcAppCore::check_system_health')
                raise

            l_cursor.close()
            l_conn.close()

        self.m_hcCounter += 1

    #: System health check and app monitoring thread (to be launched by the actual application)
    def run(self):
        self.m_logger.info('System health check thread started ...')
        while True:
            # sleeps for 30 seconds
            time.sleep(30)

            # system health check
            self.check_system_health()

            # output a full connection pool usage report
            l_f_log_name = re.sub('\.csv', '.all_connections', EcAppParam.gcm_logFile)
            l_f_log = open(l_f_log_name, 'w')
            l_f_log.write(EcConnectionPool.get_global_pool().connection_report())
            l_f_log.close()
