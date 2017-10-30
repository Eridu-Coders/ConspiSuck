#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ec_app_core import *

from cs_fb_bulk import *
# from cs_fb_connect import *

import json
import sys

__author__ = 'Pavan Mahalingam'


class CsBackgroundTask(threading.Thread):
    """
    Thread class performing the continuous batch download of Facebook stories. The thread does not start
    automatically. :any:`CsApp.start_threads()` starts both the app's own maintenance thread and the background
    tasks thread.
    """

    def __init__(self, p_pool):
        """
        Sets up class variable and launches the thread.

        :param p_pool: :any:`EcConnectionPool` passed from the main :any:`CsApp` application class.
        """
        super().__init__(daemon=True)

        #: Local logger
        self.m_logger = logging.getLogger('CsBackgroundTask')

        #: Bulk Downloader
        self.m_bulk = None

        #: Connection pool
        self.m_pool = p_pool

        #: Thread letter ID (member inherited from :any:`threading.Thread`)
        self.name = 'B'

    @staticmethod
    def get_own_ip():
        """
        Calls 4 different "what is my Ip" web services to get own IP

        :return: The IP as a 'XXX.XXX.XXX.XXX' string or `None` if no answer from any service (probably means
            Internet is down).
        """
        l_my_ip = None
        for l_ip_service in ['http://checkip.amazonaws.com/', 'https://api.ipify.org',
                             'http://icanhazip.com/', 'https://ipapi.co/ip/']:
            try:
                # 40 second timeout
                l_my_ip = urllib.request.urlopen(l_ip_service, timeout=40).read().decode('utf-8').strip()
            except urllib.error.URLError as e:
                if EcAppParam.gcm_verboseModeOn:
                    EcLogger.cm_logger.info('Cannot Open {0} service: {1}'.format(l_ip_service, repr(e)))

            if l_my_ip is not None:
                if EcAppParam.gcm_verboseModeOn:
                    EcLogger.cm_logger.info('{0} --> {1}'.format(l_ip_service, l_my_ip))
                break

        return l_my_ip

    @staticmethod
    def internet_check():
        """
        Presence of internet connection verification. Uses :any:`CsBackgroundTask.getOwnIp`

        :return: `True` if internet can be reached. `False` otherwise.
        """

        return CsBackgroundTask.get_own_ip() is not None

    def run(self):
        """
        Actual continuous FB stories download loop entry point, called by the parent :any:`threading.Thread` class.
        Performs the following tasks:

        #. Check Internet connection is up and wait if not (3 hours max)
        #. Create the :any:`BulkDownloader` class instance
        #. Launch the download (call to :any:`BulkDownloader.bulk_download()`)

        :any:`CsApp.start_threads()` starts both the app's own maintenance thread and the background
        tasks thread (which results in this method being called).

        :return: Nothing
        """

        # Spell Checker should not complain ... Grrrr
        l_long_token = 'EAAVaTJxF5KoBAOcgCLzHuyKd1jnryxefnjRW21kHO4ZAuZA9TsnnjI0JPjrAFRuT5NXUkPhuPf1FsuZCjU' \
            '49kvbqZBlpT2mCmaXA0d4JEEUppWi6sCKvt6AW3uULlJtQYHo6gfAMBIzmTdYFdAKf0FgTas2m06H8879xIdgMmwZDZD'
        l_long_token_expiry = datetime.datetime.strptime('21/12/2017', '%d/%m/%Y')

        # Make sure internet is accessible and wait otherwise
        l_sleep_time = 30
        # maximum wait = 3 hours
        l_max_attempts = int(60 * 3 * 60.0/l_sleep_time)
        l_attempt_counter = 0
        while not self.internet_check():
            self.m_logger.warning('Internet connection off')

            if l_attempt_counter >= l_max_attempts:
                self.m_logger.critical('Tried to connect for {0:0.2f} hours. Giving up.'.format(
                    (l_attempt_counter * l_sleep_time)/3600
                ))
                sys.exit(0)
            else:
                l_attempt_counter += 1

            # wait for 30 seconds
            time.sleep(l_sleep_time)

        # instantiate the bulk downloader class
        try:
            self.m_bulk = BulkDownloader(self.m_pool, l_long_token, l_long_token_expiry, self)
        except Exception as e:
            self.m_logger.warning('Unable to instantiate bulk downloader: ' + repr(e))
            raise

        self.m_logger.info('*** FB Sucking set-up complete')

        # Launch one bulk download procedure
        try:
            self.m_bulk.bulk_download()
        except Exception as e:
            self.m_logger.warning('Serious exception - Raising: ' + repr(e))
            raise


class CsApp(EcAppCore):
    """
    Main application class. Subclass of generic EC app class :any:`EcAppCore`

    This class perform two separate functions:

    * Instantiating the thread class performing the continuous downloading of FB stories (:any:`CsBackgroundTask`).
      Starting this thread is performed separately by :any:`CsApp.start_threads()`
    * Providing a response to the HTTP request for the display of results through the appropriate methods inherited
      from the base :any:`EcAppCore` class.
    """

    def __init__(self):
        super().__init__()

        #: local logger
        self.m_logger = logging.getLogger('CsApp')

        if EcAppParam.gcm_startGathering:
            #: Background task performing continuous download of FB stories
            self.m_background = CsBackgroundTask(self.m_connectionPool)
        else:
            self.m_background = None

    def start_threads(self):
        """
        Actual start of app threads. This is done separately so that the start-up process performed by
        :any:`StartApp.start_conspi_suck()` may be complete before the threads are started.

        :return: Nothing
        """

        # starting the background tasks thread
        self.m_background.start()
        self.m_logger.info('Background tasks thread started')

        # starting the generic app health check thread (as implemented in the parent's :any:`EcAppCore.run()`)
        self.start()
        self.m_logger.info('Health check thread started')

    def get_response_get(self, p_request_handler):
        """
        Build the appropriate response based on the data provided by the request handler given in parameter.

        :param p_request_handler: an :any:`EcRequestHandler` instance providing the HTTP request parameters.
        :return: A string containing the HTML of the response
        """
        # completely useless line. Only there to avoid PEP-8 pedantic complaint
        self.m_rq = p_request_handler

        self.m_logger.info('request: ' + p_request_handler.path)
        if p_request_handler.path == '/test':
            return """
                <html>
                    <head></head>
                    <body>
                        <p style="color: green;">Ok, we are in business ... </p>
                    </body>
                </html>
            """
        elif re.search('^/session/', p_request_handler.path):
            return self.one_session(p_request_handler)
        elif re.search('^/story/', p_request_handler.path):
            return self.one_story(p_request_handler)
        else:
            return self.session_list()

    def session_list(self):
        """
        Build the response for the "list of sessions" screen. No parameters necessary.

        :return: The list of sessions HTML.
        """
        l_conn = self.m_connectionPool.getconn('sessionList()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                select
                    A."ST_SESSION_ID"
                    , A."DT_CRE"
                    , B."N_STORY_COUNT"
                    , C."ST_NAME"
                    , C."ST_USER_ID"
                from "TB_SESSION" A
                    join (
                        select "ST_SESSION_ID", count(1) as "N_STORY_COUNT"
                        from "TB_STORY"
                        group by "ST_SESSION_ID"
                    ) B on A."ST_SESSION_ID" = B."ST_SESSION_ID"
                    join "TB_USER" C on C."ID_INTERNAL" = A."ID_INTERNAL"
                order by "DT_CRE" desc
                limit {0};
            """.format(EcAppParam.gcm_sessionDisplayCount))

            l_response = ''
            for l_sessionId, l_dtCre, l_count, l_userName, l_userId in l_cursor:
                l_response += """
                    <tr>
                        <td>{0}</td>
                        <td>{1}</td>
                        <td><a href="/session/{2}">{2}</a></td>
                        <td>{3}</td>
                        <td style="text-align: center;">{4}</td>
                    <tr/>
                """.format(
                    l_userName, l_userId, l_sessionId, l_dtCre.strftime('%d/%m/%Y %H:%M'), l_count)
        except Exception as e:
            self.m_logger.warning('TB_SESSION query failure: {0}'.format(repr(e)))
            raise

        l_cursor.close()
        self.m_connectionPool.putconn(l_conn)
        return """
            <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
            <head>
                <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
            </head>
            <body>
                <table>
                    <tr>
                        <td style="font-weight: bold;">ST_NAME</td>
                        <td style="font-weight: bold;">ST_USER_ID</td>
                        <td style="font-weight: bold;">ST_SESSION_ID</td>
                        <td style="font-weight: bold;">DT_CRE</td>
                        <td style="font-weight: bold;">N_STORY_COUNT</td>
                    <tr/>
                    {0}
                </table>
            </body>
            </html>
        """.format(l_response)

    def one_session(self, p_request_handler):
        """
        Build the HTML for an individual session screen, i.e. the list of stories retrieved from that session.

        :param p_request_handler: The :any:`EcRequestHandler` instance providing the session ID parameter.
        :return: The Session HTML.
        """
        # the session ID is the last member of the URL
        l_session_id = re.sub('/session/', '', p_request_handler.path)
        self.m_logger.info('l_sessionId: {0}'.format(l_session_id))
        l_conn = self.m_connectionPool.getconn('oneSession()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                        select *
                        from "TB_STORY"
                        where "ST_SESSION_ID" = '{0}'
                        order by "ID_STORY";
                    """.format(l_session_id))

            l_response = ''
            for l_id_story, l_session_id, l_dt_story, l_dt_cre, l_st_type, \
                    l_json, l_likes, l_comments, l_shares in l_cursor:

                l_story = json.loads(l_json)
                l_img_count = len(l_story['images'])
                l_text = l_story['text'][:50]
                if len(l_text) != len(l_story['text']):
                    l_text += '...'
                l_text_q = l_story['text_quoted'][:50]
                if len(l_text_q) != len(l_story['text_quoted']):
                    l_text_q += '...'
                if len(l_text + l_text_q) > 0:
                    l_display_text = l_text + '■■■' + l_text_q
                else:
                    l_display_text = ''

                l_response += """
                            <tr>
                                <td style="padding-right:1em;"><a href="/story/{0}">{0}</a></td>
                                <td style="padding-right:1em;">{1}</td>
                                <td style="padding-right:1em;">{2}</td>
                                <td style="padding-right:1em;">{3}</td>
                                <td style="padding-right:1em;">{4}</td>
                                <td style="padding-right:1em;">{5}</td>
                                <td style="padding-right:1em;">{6}</td>
                                <td style="padding-right:1em;">{7}</td>
                                <td>{8}</td>
                            <tr/>
                        """.format(
                    l_id_story,
                    l_dt_story.strftime('%d/%m/%Y&nbsp;%H:%M') if l_dt_story is not None else 'NULL',
                    l_dt_cre.strftime('%d/%m/%Y&nbsp;%H:%M'),
                    l_st_type, l_likes, l_comments, l_shares, l_img_count, l_display_text
                )
        except Exception as e:
            self.m_logger.warning('TB_STORY query failure: {0}'.format(repr(e)))
            raise

        l_cursor.close()
        self.m_connectionPool.putconn(l_conn)
        return """
                    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
                    <head>
                        <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                    </head>
                    <body>
                        <h1>Session: {0}</h1>
                        <table>
                            <tr>
                                <td style="font-weight: bold; padding-right:1em;">ID_STORY</td>
                                <td style="font-weight: bold; padding-right:1em;">DT_STORY</td>
                                <td style="font-weight: bold; padding-right:1em;">DT_CRE</td>
                                <td style="font-weight: bold; padding-right:1em;">ST_TYPE</td>
                                <td style="font-weight: bold; padding-right:1em; font-size:60%;">N_LIKES</td>
                                <td style="font-weight: bold; padding-right:1em; font-size:60%;">N_COMMENTS</td>
                                <td style="font-weight: bold; padding-right:1em; font-size:60%;">N_SHARES</td>
                                <td style="font-weight: bold; padding-right:1em; font-size:60%;">Img.&nbsp;#</td>
                                <td style="font-weight: bold;">Text■■■Quoted text</td>
                            <tr/>
                            {1}
                        </table>
                    </body>
                    </html>
                """.format(l_session_id, l_response)

    def one_story(self, p_request_handler):
        """
        Build the HTML for an individual story screen.

        :param p_request_handler: The :any:`EcRequestHandler` instance providing the story ID parameter.
        :return: The story HTML.
        """
        # the story ID is the last member of the URL
        l_story_id = re.sub('/story/', '', p_request_handler.path)
        self.m_logger.info('l_storyId: {0}'.format(l_story_id))
        l_conn = self.m_connectionPool.getconn('oneStory()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                                select *
                                from "TB_STORY"
                                where "ID_STORY" = {0};
                            """.format(l_story_id))

            l_response = ''
            for l_idStory, l_sessionId, l_dtStory, l_dtCre, \
                    l_stType, l_json, l_likes, l_comments, l_shares in l_cursor:

                l_story = json.loads(l_json)

                # <img src="data:image/jpeg;base64,
                l_img_display = ''
                for l_imgB64 in l_story['images']:
                    l_img_display += """
                        <img src="data:image/png;base64,{0}">
                    """.format(l_imgB64)

                l_html_disp = l_story['html'] if 'html' in l_story.keys() else ''
                l_html_disp = re.sub(r'<', r'&lt;', l_html_disp)
                l_html_disp = re.sub(r'>', r'&gt;&#8203;', l_html_disp)
                # l_html_disp = re.sub(r'>', r'&gt; ', l_html_disp)

                l_likes = ''
                if 'likes' in l_story.keys():
                    l_likes = repr(l_story['likes'])

                l_response += """
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">ID_STORY</td>
                        <td>{0}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Text:</td>
                        <td>{1}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Text&nbsp;quoted:</td>
                        <td>{2}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Type:</td>
                        <td>{3}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">From:</td>
                        <td>{4}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Date:</td>
                        <td>{5}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Quoted&nbsp;date(s):</td>
                        <td>{6}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Shared&nbsp;Item(s):</td>
                        <td>{7}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Sponsored:</td>
                        <td>{8}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">With:</td>
                        <td>{9}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Likes:</td>
                        <td>{10}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Comments:</td>
                        <td>[{11}] {12}</td>
                    <tr/>
                    <tr>
                        <td style="padding-right:1em; font-weight: bold; vertical-align: top;">Shares:</td>
                        <td>{13}</td>
                    <tr/>
                    <tr>
                        <td colspan="2">{14}</td>
                    <tr/>
                    <tr>
                        <td colspan="2" style="word-wrap:break-word;">{15}</td>
                    <tr/>
                """.format(
                    l_idStory,
                    l_story['text'],
                    l_story['text_quoted'],
                    l_story['type'],
                    repr(l_story['from_list']),
                    l_story['date'],
                    repr(l_story['date_quoted']),
                    repr(l_story['shared']),
                    'Yes' if l_story['sponsored'] else 'No',
                    'Yes' if l_story['with'] else 'No',
                    l_likes,
                    l_comments, repr(l_story['comments']) if 'comments' in l_story.keys() else '',
                    l_story['shares'] if 'shares' in l_story.keys() else '',
                    l_img_display,
                    l_html_disp
                )
        except Exception as e:
            self.m_logger.warning('TB_STORY query failure: {0}'.format(repr(e)))
            raise

        l_cursor.close()
        self.m_connectionPool.putconn(l_conn)
        return """
                <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
                <head>
                    <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                </head>
                <body style="font-family: sans-serif;">
                    <table>{0}</table>
                </body>
                </html>
            """.format(l_response)
