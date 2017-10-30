#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ec_app_param import *

import logging
import re
import email
import datetime
import time
import pytz
import smtplib
import psycopg2.pool
import psycopg2
import psycopg2.extensions
import threading

__author__ = 'Pavan Mahalingam'


# -------------------------------------- Logging Set-up ----------------------------------------------------------------
class EcLogger(logging.Logger):
    """
    Custom logging class (see `the python doc re. logging <https://docs.python.org/3.5/library/logging.html>`_)
    """

    #: static variable containing the root logger.
    cm_logger = None

    # connection pool for logging purposes
    # the logging system uses a separate connection pool and it is an ordinary psycopg pool because there is
    # no concern that connections might get lost
    cm_pool = None

    @classmethod
    def root_logger(cls):
        """
        Access to the root logger (used only in the startup sequence)
        :return: the root logger
        """
        return cls.cm_logger

    def __init__(self, p_name=None, p_level=logging.NOTSET):
        """
        Custom logger init. Handles the logger name (app name + '.logger name') and sets
        the logging level according to :any:`gcm_verboseModeOn` and :any:`gcm_debugModeOn`.

        :param str p_name: Name to append to the logger name (+ '.p_name'). If ``None`` then
            the logger name is just the app name (``gcm_appName``)
        :param p_level: optional level setting (never used)
        :type p_level: `logging level <https://docs.python.org/3.5/library/logging.html#logging-levels>`_

        """
        if p_name is None:
            super().__init__(EcAppParam.gcm_appName, p_level)
        else:
            super().__init__(EcAppParam.gcm_appName + '.' + p_name, p_level)

        if EcAppParam.gcm_verboseModeOn:
            self.setLevel(logging.INFO)
        if EcAppParam.gcm_debugModeOn:
            self.setLevel(logging.DEBUG)

    @classmethod
    def log_init(cls):
        """
        Initializes the logging system by creating the root logger + 2 subclasses of :py:class:`logging.Formatter`
        to handle:

        * the in-console display of log messages.
        * their storage into a CSV file (path given in :any:`gcm_logFile`).

        Only INFO level messages and above are displayed on screen (if :any:`gcm_verboseModeOn` is set).
        DEBUG level messages, if any, are sent to the CSV file.
        """
        # initializes the logging connection pool
        try:
            cls.cm_pool = psycopg2.pool.ThreadedConnectionPool(
                EcAppParam.gcm_connectionPoolMinCount,
                EcAppParam.gcm_connectionPoolMaxCount,
                host=EcAppParam.gcm_dbServer,
                database=EcAppParam.gcm_dbDatabase,
                user=EcAppParam.gcm_dbUser,
                password=EcAppParam.gcm_dbPassword,
                connection_factory=EcConnection
            )
        except psycopg2.Error as e:
            EcMailer.send_mail(
                'Failed to create logging connection pool: {0}'.format(repr(e)),
                'Sent from EcLogger.logInit\n' + EcConnectionPool.get_psycopg2_error_block(e)
            )
            raise

        # purge TB_EC_DEBUG
        l_conn = EcLogger.cm_pool.getconn()
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute('delete from "TB_EC_DEBUG"')
            l_conn.commit()
        except psycopg2.Error as e:
            EcMailer.send_mail(
                'TB_EC_DEBUG purge failure: {0}'.format(repr(e)),
                'Sent from EcLogger.logInit\n' + EcConnectionPool.get_psycopg2_error_block(e)
            )
            raise

        l_cursor.close()
        EcLogger.cm_pool.putconn(l_conn)

        # Creates the column headers for the CSV log file
        l_f_log = open(EcAppParam.gcm_logFile, 'w')
        l_f_log.write('LOGGER_NAME;TIME;LEVEL;MODULE;FILE;FUNCTION;LINE;MESSAGE\n')
        l_f_log.close()

        # registers the EcLogger class with the logging system
        logging.setLoggerClass(EcLogger)

        # Create the main logger
        cls.cm_logger = logging.getLogger()

        # One handler for the console (only up to INFO messages) and another for the CSV file (everything)
        l_handler_console = logging.StreamHandler()
        l_handler_file = logging.FileHandler(EcAppParam.gcm_logFile, mode='a')

        # Custom Formatter for the CSV file --> eliminates multiple spaces (and \r\n)
        class EcCsvFormatter(logging.Formatter):
            def format(self, p_record):
                l_record = logging.LogRecord(
                    p_record.name,
                    p_record.levelno,
                    p_record.pathname,
                    p_record.lineno,
                    re.sub('"', '""', p_record.msg),
                    # message arguments are not allowed here
                    None,
                    # p_record.args,
                    p_record.exc_info,
                    p_record.funcName,
                    p_record.stack_info,
                )

                if LocalParam.gcm_debugToDB:
                    # log message in TB_EC_DEBUG
                    l_conn1 = EcLogger.cm_pool.getconn()
                    l_cursor1 = l_conn1.cursor()
                    try:
                        l_cursor1.execute("""
                                insert into "TB_EC_DEBUG"(
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
                            p_record.name,
                            p_record.levelname,
                            p_record.module,
                            p_record.pathname,
                            p_record.funcName,
                            p_record.lineno,
                            re.sub('\s+', ' ', p_record.msg)
                        ))
                        l_conn1.commit()
                    except psycopg2.Error as e1:
                        EcMailer.send_mail(
                            'TB_EC_DEBUG insert failure: {0}'.format(repr(e1)),
                            'Sent from EcCsvFormatter\n' + EcConnectionPool.get_psycopg2_error_block(e1)
                        )
                        raise

                    l_cursor1.close()
                    EcLogger.cm_pool.putconn(l_conn1)

                l_ret = re.sub('\s+', ' ', super().format(l_record))
                return l_ret

        # Custom Formatter for the console --> send mail if warning or worse
        class EcConsoleFormatter(logging.Formatter):
            def format(self, p_record):
                l_formatted = super().format(p_record)

                # this test is located here and not in the CSV formatter so that it does not get to be performed
                # needlessly for every debug message
                if p_record.levelno >= logging.WARNING:
                    # send mail
                    EcMailer.send_mail(
                        '{0}-{1}[{2}]/{3}'.format(
                            p_record.levelname,
                            p_record.module,
                            p_record.lineno,
                            p_record.funcName),
                        l_formatted
                    )

                    # log message in TB_EC_MSG
                    l_conn1 = EcLogger.cm_pool.getconn()
                    l_cursor1 = l_conn1.cursor()
                    try:
                        l_cursor1.execute("""
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
                            'LOG',
                            p_record.name,
                            p_record.levelname,
                            p_record.module,
                            p_record.pathname,
                            p_record.funcName,
                            p_record.lineno,
                            re.sub('\s+', ' ', p_record.msg)
                        ))
                        l_conn1.commit()
                    except psycopg2.Error as e1:
                        EcMailer.send_mail(
                            'TB_EC_MSG insert failure: {0}'.format(repr(e1)),
                            'Sent from EcConsoleFormatter\n' + EcConnectionPool.get_psycopg2_error_block(e1)
                        )
                        raise

                    l_cursor1.close()
                    EcLogger.cm_pool.putconn(l_conn1)

                return l_formatted

        # Install formatters
        l_handler_console.setFormatter(EcConsoleFormatter('ECL:%(levelname)s:%(name)s:%(message)s'))
        l_handler_file.setFormatter(
            EcCsvFormatter('"%(name)s";"%(asctime)s";"%(levelname)s";"%(module)s";' +
                           '"%(filename)s";"%(funcName)s";%(lineno)d;"%(message)s"'))

        # If verbose mode on, both handlers receive messages up to INFO
        if EcAppParam.gcm_verboseModeOn:
            cls.cm_logger.setLevel(logging.INFO)
            l_handler_console.setLevel(logging.INFO)
            l_handler_file.setLevel(logging.INFO)

        # If debug mode is on, then the console stays as it is but the CSV file now receives everything
        if EcAppParam.gcm_debugModeOn:
            cls.cm_logger.setLevel(logging.DEBUG)
            l_handler_file.setLevel(logging.DEBUG)

        # Install the handlers
        cls.cm_logger.addHandler(l_handler_console)
        cls.cm_logger.addHandler(l_handler_file)

        # Start-up Messages
        cls.cm_logger.info('-->> Start logging')
        cls.cm_logger.debug('-->> Start logging')


# -------------------------------------- e-mail messages sending -------------------------------------------------------
class EcMailer(threading.Thread):
    """
    Sends an e-mail through smtp. Can handle the following servers:

    * Amazon AWS SES (TLS auth)
    * Gmail (TLS auth)
    * Ordinary SMTP without authentication.

    The type of server is determined by :any:`LocalParam.gcm_amazonSmtp` (true for AWS) and
    :any:`LocalParam.gcm_gmailSmtp` (true for Gmail)

    For an Amazon SES howto, see this
    `blog page <http://blog.noenieto.com/blog/html/2012/06/18/using_amazon_ses_with_your_python_applications.html>`_

    For a Gmail TLS howto, see:
    `this page <http://stackabuse.com/how-to-send-emails-with-gmail-using-python/>`_
    """

    #: List of previously sent messages with timestamps, to avoid sending too many (min. 5min. deep).
    cm_sendMailGovernor = None

    #: Concurrency management lock to control access to :any:`cm_sendMailGovernor` This is necessary
    #: because each mail sending operation is executed asynchronously in its own thread
    cm_mutexGovernor = None

    #: Concurrency management lock to control access to the log files (see :any:`sendMail`). This is necessary
    #: because each mail sending operation is executed asynchronously in its own thread
    cm_mutexFiles = None

    @classmethod
    def init_mailer(cls):
        """
        Mail system initialization. Creates an empty :any:`cm_sendMailGovernor` and the associated
        Mutexes to allow critical section protection.
        """
        cls.cm_sendMailGovernor = dict()
        cls.cm_mutexGovernor = threading.Lock()
        cls.cm_mutexFiles = threading.Lock()

    @classmethod
    def send_mail(cls, p_subject, p_message):
        """
        Sends an e-mail message asynchronously (each call executed in its own thread) to avoid
        blocking the main application flow while waiting for the SMTP server to respond.

        Every sent message goes into a text file
        (same path as :any:`EcAppParam.gcm_logFile` but with 'all_msg' at the end instead of 'csv')

        Ensures that no more than 10 message with the same subject are sent every 5 minutes
        (using :any:`cm_sendMailGovernor`) Beyond this, the messages are not sent but stored in the overflow file
        (same path as :any:`EcAppParam.gcm_logFile` but with 'overflow_msg' at the end instead of 'csv')

        Errors encountered during processing are stored in a dedicated file (same path as
        :any:`EcAppParam.gcm_logFile` but with `'smtp_error'` at the end instead of `'csv'`)
        This file is in CSV format so that it can be merged with the main CSV log file.
        Yet another file receives the messages which could not ne sent due to these errors (same path as
        :any:`EcAppParam.gcm_logFile` but with 'rejected_msg' at the end instead of 'csv')

        All these files are appended to only (open mode ``'a'``) Nothing is ever removed from them. Any
        write access to them is protected by a mutex because of the asynchronous nature of the operations (several
        e-mail messages may be in the process of being sent in parallel)

        :param p_subject: Message subject.
        :param p_message: Message body.
        """
        l_thread = EcMailer(p_subject, p_message)
        l_thread.start()

    def __init__(self, p_subject, p_message):
        """
        preparing the thread to be launched (by :any:`sendMail`)

        :param p_subject: same as in :any:`sendMail`
        :param p_message: same as in :any:`sendMail`
        """
        self.m_subject = p_subject
        self.m_message = p_message

        super().__init__()

    def run(self):
        """
        Actual e-mail sending process (executed in a dedicated thread)
        """

        # message context with headers and body
        l_message = """From: {0}
            To: {1}
            Date: {2}
            Subject: {3}

            {4}
        """.format(
            EcAppParam.gcm_mailSender,
            ', '.join(EcAppParam.gcm_mailRecipients),
            email.utils.format_datetime(datetime.datetime.now(tz=pytz.utc)),
            self.m_subject,
            self.m_message
        )

        # removes spaces at the beginning of lines
        l_message = re.sub('^[ \t\r\f\v]+', '', l_message, flags=re.MULTILINE)

        # limitation of email sent
        EcMailer.cm_mutexGovernor.acquire()
        # !!!!! cm_sendMailGovernor CRITICAL SECTION START !!!!!!
        l_now = time.time()
        try:
            # the list of all UNIX timestamps when this subject was sent in the previous 5 min at least
            l_this_subject_history = EcMailer.cm_sendMailGovernor[self.m_subject]
        except KeyError:
            l_this_subject_history = [l_now]

        l_this_subject_history.append(l_now)

        l_this_subject_history_new = list()
        l_count = 0
        # count the number of messages with this subject which have been sent within the last 5 minutes
        for l_pastSend in l_this_subject_history:
            if l_now - l_pastSend < 5*60:
                l_count += 1
                l_this_subject_history_new.append(l_pastSend)

        EcMailer.cm_sendMailGovernor[self.m_subject] = l_this_subject_history_new
        # !!!!!! cm_sendMailGovernor CRITICAL SECTION END !!!!!!
        EcMailer.cm_mutexGovernor.release()

        # maximum : 10 with the same subject every 5 minutes
        if l_count > 10:
            # overflow stored the message in a separate file
            EcMailer.cm_mutexFiles.acquire()
            l_f_log = open(re.sub('\.csv', '.overflow_msg', EcAppParam.gcm_logFile), 'a')
            l_f_log.write('>>>>>>>\n' + l_message)
            l_f_log.close()
            EcMailer.cm_mutexFiles.release()
            return

        # all messages
        l_f_log_name = re.sub('\.csv', '.all_msg', EcAppParam.gcm_logFile)
        EcMailer.cm_mutexFiles.acquire()
        l_f_log = open(l_f_log_name, 'a')
        l_f_log.write('>>>>>>>\n' + l_message)
        l_f_log.close()
        EcMailer.cm_mutexFiles.release()

        # numeric value indicating the steps in the authentication process, for debug purposes
        l_step_passed = 0
        try:
            if EcAppParam.gcm_amazonSmtp:
                # Amazon AWS/SES

                # smtp client init
                l_smtp_obj = smtplib.SMTP(
                    host=EcAppParam.gcm_smtpServer,
                    port=587,
                    timeout=10)
                l_step_passed = 101

                # initialize TLS connection
                l_smtp_obj.starttls()
                l_step_passed = 102
                l_smtp_obj.ehlo()
                l_step_passed = 103

                # authentication
                l_smtp_obj.login(EcAppParam.gcm_sesUserName, EcAppParam.gcm_sesPassword)
                l_step_passed = 104
            elif EcAppParam.gcm_gmailSmtp:
                # Gmail / TLS authentication
                # Also used for Smtp2Go

                # smtp client init
                l_smtp_obj = smtplib.SMTP(EcAppParam.gcm_smtpServer, 587)
                l_step_passed = 201

                # initialize TLS connection
                l_smtp_obj.starttls()
                l_step_passed = 202
                l_smtp_obj.ehlo()
                l_step_passed = 203

                # authentication
                l_smtp_obj.login(EcAppParam.gcm_mailSender, EcAppParam.gcm_mailSenderPassword)
                l_step_passed = 204
            else:
                l_smtp_obj = smtplib.SMTP(EcAppParam.gcm_smtpServer)

            # sending message
            l_smtp_obj.sendmail(EcAppParam.gcm_mailSender, EcAppParam.gcm_mailRecipients, l_message)
            l_step_passed = 99

            # end TLS session (Amazon SES / Gmail)
            if EcAppParam.gcm_amazonSmtp or EcAppParam.gcm_gmailSmtp:
                l_smtp_obj.quit()
        except smtplib.SMTPException as l_exception:
            # if failure, stores the message in a separate file
            EcMailer.cm_mutexFiles.acquire()
            l_f_log = open(re.sub('\.csv', '.rejected_msg', EcAppParam.gcm_logFile), 'a')
            l_f_log.write('>>>>>>>\n' + l_message)
            l_f_log.close()

            # and create a log record in another separate file (distinct from the main log file)
            l_f_log = open(re.sub('\.csv', '.smtp_error', EcAppParam.gcm_logFile), 'a')
            # LOGGER_NAME;TIME;LEVEL;MODULE;FILE;FUNCTION;LINE;MESSAGE
            l_f_log.write(
                'EcMailer;{0};CRITICAL;ec_utilities;ec_utilities.py;sendMail;0;{1}-{2} [step = {3}]\n'.format(
                    datetime.datetime.now(tz=pytz.utc).strftime('%Y-%m-%d %H:%M.%S'),
                    type(l_exception).__name__,
                    re.sub('\s+', ' ', repr(l_exception)),
                    l_step_passed
                )
            )
            l_f_log.close()
            EcMailer.cm_mutexFiles.release()
        except Exception as e:
            EcMailer.cm_mutexFiles.acquire()
            l_f_log = open(l_f_log_name, 'a')
            l_f_log.write(
                '!!!!! {0}-"{1}" [Step = {2}]\n'.format(
                    type(e).__name__,
                    re.sub('\s+', ' ', repr(e)),
                    l_step_passed
                )
            )
            l_f_log.close()
            EcMailer.cm_mutexFiles.release()


# ----------------- Database connection pool ---------------------------------------------------------------------------
class EcConnectionPool(psycopg2.pool.ThreadedConnectionPool):
    """
    Database connection handling class. Uses PostgreSQL (`psycopg2 <http://initd.org/psycopg/>`_).
    `EcConnectionPool` is built as a subclass of psycopg2's
    `ThreadedConnectionPool <http://pythonhosted.org/psycopg2/pool.html>`_
    """

    @staticmethod
    def is_not_none(v):
        return v if v is not None else 'None'

    @staticmethod
    def get_psycopg2_error_block(e):
        l_block = 'pgerror : {0}\n'.format(EcConnectionPool.is_not_none(e.pgerror))
        l_block += 'pgcode : {0}\n'.format(EcConnectionPool.is_not_none(e.pgcode))
        if e.diag is not None:
            l_block += 'diag :\n'
            l_block += 'column_name : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.column_name))
            l_block += 'constraint_name : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.constraint_name))
            l_block += 'context : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.context))
            l_block += 'datatype_name : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.datatype_name))
            l_block += 'internal_position : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.internal_position))
            l_block += 'internal_query : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.internal_query))
            l_block += 'message_detail : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.message_detail))
            l_block += 'context : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.context))
            l_block += 'message_hint : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.message_hint))
            l_block += 'message_primary : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.message_primary))
            l_block += 'schema_name : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.schema_name))
            l_block += 'severity : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.severity))
            l_block += 'source_file : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.source_file))
            l_block += 'source_function : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.source_function))
            l_block += 'source_line : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.source_line))
            l_block += 'sqlstate : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.sqlstate))
            l_block += 'statement_position : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.statement_position))
            l_block += 'table_name : {0}\n'.format(EcConnectionPool.is_not_none(e.diag.table_name))

        return l_block

    @classmethod
    def get_new(cls):
        return EcConnectionPool(
            EcAppParam.gcm_connectionPoolMinCount,
            EcAppParam.gcm_connectionPoolMaxCount,
            host=EcAppParam.gcm_dbServer,
            database=EcAppParam.gcm_dbDatabase,
            user=EcAppParam.gcm_dbUser,
            password=EcAppParam.gcm_dbPassword,
            connection_factory=EcConnection
        )

    def __init__(self, p_minconn, p_maxconn, *args, **kwargs):
        """
        Creates the member attributes needed for debug purposes:

        * `m_connectionRegister`: list of all db connections "out there"
        * `m_getCalls` number of calls to :any:`EcConnectionPool.getconn`
        * `m_putCalls` number of calls to :any:`EcConnectionPool.putconn`

        :param p_minconn: Minimum number of connections
            (parameter of `ThreadedConnectionPool <http://pythonhosted.org/psycopg2/pool.html>`_ constructor).
        :param p_maxconn: Maximum number of connections
            (parameter of `ThreadedConnectionPool <http://pythonhosted.org/psycopg2/pool.html>`_ constructor).
        :param args: Other positional arguments
            (parameter of `ThreadedConnectionPool <http://pythonhosted.org/psycopg2/pool.html>`_ constructor).
        :param kwargs: Other keyword arguments
            (parameter of `ThreadedConnectionPool <http://pythonhosted.org/psycopg2/pool.html>`_ constructor).
        """
        self.m_logger = logging.getLogger('ConnectionPool')

        self.m_logger.info('args : [{0}] kwargs : [{1}]'.format(
            repr(args), repr(kwargs)
        ))

        self.m_connectionRegister = []
        self.m_getCalls = 0
        self.m_putCalls = 0
        super().__init__(p_minconn, p_maxconn, *args, **kwargs)

    # noinspection PyMethodOverriding
    def getconn(self, p_debug_data, p_key=None):
        """
        Get a connection from the pool. Overrides the `getconn` method of
        `ThreadedConnectionPool <http://pythonhosted.org/psycopg2/pool.html>`_
        but adds an argument to pass debug data.

        Beyond calling the method from the parent class, this method does the following:

        * increment the `m_getCalls` counter.
        * pass the debug data string to the newly available connection.
        * adds the connection to 'm_connectionRegister'.

        :param p_debug_data: An identification string for debug purposes.
        :param p_key: parameter of the parent class method.
        :return: a connection from the pool, wrapped as an :any:`EcConnection`
        """
        self.m_getCalls += 1
        l_new_conn = super().getconn(p_key)
        l_new_conn.debugData = p_debug_data
        self.m_connectionRegister.append(l_new_conn)

        return l_new_conn

    # noinspection PyMethodOverriding
    def putconn(self, p_conn, p_key=None, p_close=False):
        """
        Returns a connection to the pool.

        :param p_conn: the connection being returned.
        :param p_key: parameter of the parent class method.
        :param p_close: parameter of the parent class method.

        Beyond calling the method from the parent class, this method does the following:

        * increment the `m_putCalls` counter.
        * remove the connection from 'm_connectionRegister'.

        """
        # why do I get a PyCharm warning here ?!
        # It works fine and it matches the method signature given by the autocomplete.
        # For getconn, it is normal (added p_debugData) but not here.

        self.m_putCalls += 1
        self.m_connectionRegister.remove(p_conn)
        p_conn.reset_debug_data()

        super().putconn(p_conn, p_key, p_close)

    def closeall(self):
        super().closeall()

    def connection_report(self):
        """
        Uses the debug data contained in all outstanding connections (listed in `m_connectionRegister`)
        to produce a report string (1 connection per line).

        :return: The report string.
        """
        l_report = '[{0}] get/put: {1}/{2}\n'.format(
            datetime.datetime.now(tz=pytz.utc),
            self.m_getCalls, self.m_putCalls
        )

        for l_conn in self.m_connectionRegister:
            l_report += l_conn.debugData + '\n'

        return l_report


class EcConnection(psycopg2.extensions.connection):
    """
    A wrapper class around
    `psycopg2.extensions.connection <http://initd.org/psycopg/docs/advanced.html#connection-and-cursor-factories>`_
    adding the necessary data for connection usage tracking and debugging (especially when connections are
    not properly released).
    """

    #: global sequence to assign unique IDs to all instances of the class
    cm_IDCounter = 0

    def __init__(self, dsn, *more):
        """
        Beyond calling the parent constructor, this method does the following:

        * assigns the string 'Fresh' to `m_debugData` (so that if a connection is later seen with this string, it means
            that it was not obtained through the pool -- see :any:`EcConnectionPool.getconn` )
        * assigns the current date/time to `m_creationDate`
        * assigns a new ID to `m_connectionID` and increments `EcConnection.cm_IDCounter`

        :param dsn: parameter of the parent constructor.
        :param more: parameter of the parent constructor.
        """
        self.m_debugData = 'Fresh'
        self.m_creationDate = datetime.datetime.now(tz=pytz.utc)
        self.m_connectionID = EcConnection.cm_IDCounter
        EcConnection.cm_IDCounter += 1

        super().__init__(dsn, *more)

    @property
    def debug_data(self):
        """
        A property to get and set the debug data. When, setting, it simply results in an assignment to `m_debugData`
        but when getting, the attribute returns a descriptive string containing the ID and the creation date in
        addition to the debug data proper. This string is used to create :any:`EcConnectionPool.connectionReport`
        """
        return '[{0}] {1}-{2}'.format(self.m_connectionID, self.m_creationDate, self.m_debugData)

    @debug_data.setter
    def debug_data(self, p_debug_data):
        self.m_debugData += '/' + p_debug_data

    def reset_debug_data(self):
        """
        When a connection is put back into the pool, its debug data is reset by :any:`EcConnectionPool.putconn`
        Its value is set to `'Used'` (contrasting with `'Fresh'` as set by the constructor)

        As a result, the debug data of a connection looks like `'Fresh/XXXX'` if it is the first time it is used
        or `'Used/XXXX'` if the connection has been returned back to the pool at least once. In both cases,
        `'XXXX'` is the debug data string set in :any:`EcConnectionPool.getconn`

        """
        self.m_debugData = 'Used'

# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| FB scraping web service for ROAD B SCORE                   |')
    print('|                                                            |')
    print('| ec_utilities module test                                   |')
    print('|                                                            |')
    print('| v. 1.0 - 20/02/2017                                        |')
    print('+------------------------------------------------------------+')

    EcMailer.init_mailer()
    EcMailer.send_mail('Test Subject 2', 'Test Body from Smtp2Go')

    EcLogger.log_init()
    l_logger = logging.getLogger('Test')
    l_logger.debug('Debug message test')
    l_logger.info('Info message test')
    l_logger.warning('Warning message test')
    l_logger.error('Error message test')
    l_logger.critical('Critical message test')
