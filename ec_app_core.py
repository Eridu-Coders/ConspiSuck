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

        * Start the connection pool.
        * Test the DB connection by storing a startup message in `TB_EC_MSG`.
        * Start the health check thread.
        """
        super().__init__(daemon=True)

        # bogus variable introduced to avoid a PEP-8 pedantic complaint in get_response
        self.m_rq = None

        # logger
        self.m_logger = logging.getLogger('AppCore')

        # connection pool init
        try:
            self.m_connectionPool = EcConnectionPool.get_new()
        except Exception as e:
            self.m_logger.warning('Unable to start Connection pool: {0}-{1}'.format(
                type(e).__name__, repr(e)
            ))
            raise

        # Add a record to TB_EC_MSG, thus testing the db connection
        l_conn = self.m_connectionPool.getconn('DB Connection test in EcAppCore.__init__()')
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
                    "TX_MSG"
                )
                values(%s, %s, %s, %s, %s, %s, %s);
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
                )
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
        self.m_connectionPool.putconn(l_conn)
        self.m_logger.info('Successful TB_EC_MSG insert - The DB appears to be working')

        # health check counter
        self.m_hcCounter = 0

        # starts the refresh thread
        self.name = 'S'
        # placed under the responsibility of the implementation app
        # self.start()

    #: Connection pool access
    def get_connection_pool(self):
        return self.m_connectionPool

    #: Main application entry point - App response to an HTTP POST request
    def get_response_post(self, p_requestHandler, p_postData):
        # completely useless line. Only there to avoid PEP-8 pedantic complaint
        self.m_rq = p_requestHandler

        return '{"status":"FAIL", "message":"You should never see this. If you do then things are really wrong"}'

    #: Main application entry point - App response to an HTTP GET request
    def get_response_get(self, p_request_handler):
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
        `psutil <https://pythonhosted.org/psutil/>`_ and stored in `TB_MSG`.
        """
        l_thread_list_letter = []
        l_thread_list_other = []
        for t in threading.enumerate():
            if t.name == 'MainThread':
                l_thread_list_letter.append('M')
            elif len(t.name) == 1:
                l_thread_list_letter.append(t.name)
            else:
                l_thread_list_other.append(t.name)
        l_thread_list_letter.sort()
        l_thread_list_other.sort()
        l_thread_list = '[{0}]-[{1}]'.format(''.join(l_thread_list_letter), '/'.join(l_thread_list_other))

        l_mem = psutil.virtual_memory()

        self.m_logger.info(('System Health Check - Available RAM: {0:.2f} Mb ({1:.2f} % usage) ' +
                            'Threads: {2}').format(
            l_mem.available / (1024 * 1024), l_mem.percent, l_thread_list))

        if l_mem.percent >= 75.0:
            self.m_logger.warning('System Health Check ALERT - Available RAM: {0:.2f} Mb ({1:.2f} % usage)'.format(
                l_mem.available / (1024 * 1024), l_mem.percent))

        # full system resource log every 5 minutes
        if self.m_hcCounter % 10 == 0:
            l_cpu = psutil.cpu_times()
            l_swap = psutil.swap_memory()
            l_diskRoot = psutil.disk_usage('/')
            l_net = psutil.net_io_counters()
            l_processCount = len(psutil.pids())

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
                        "TX_MSG"
                    )
                    values(%s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    'HLTH',
                    'xxx',
                    'XXX',
                    'ec_app_core',
                    './ec_app_core.py',
                    'check_system_health',
                    0,
                    'MEM: {0}/CPU: {1}/SWAP: {2}/DISK(root): {3}/NET: {4}/PROCESSES: {5}'.format(
                        l_mem, l_cpu, l_swap, l_diskRoot, l_net, l_processCount
                    )
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

    #: System health check and app monitoring thread
    def run(self):
        self.m_logger.info('System health check thread started ...')
        while True:
            # sleeps for 30 seconds
            time.sleep(30)

            # system health check
            self.check_system_health()

            # output a full connection pool usage report
            l_fLogName = re.sub('\.csv', '.all_connections', EcAppParam.gcm_logFile)
            l_fLog = open(l_fLogName, 'w')
            l_fLog.write(self.m_connectionPool.connection_report())
            l_fLog.close()
