#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ec_app_core import *

from cs_fb_bulk import *
from cs_fb_nlp import *
# from cs_fb_connect import *

import sys
import collections

__author__ = 'Pavan Mahalingam'

# TODO: Warning message when approaching token end of life

# extra={'m_errno': 2006}

class CsBackgroundTask(threading.Thread):
    """
    Thread class performing the continuous batch download of Facebook stories. The thread does not start
    automatically. :any:`CsApp.start_threads()` starts both the app's own maintenance thread and the background
    tasks thread.
    """

    def __init__(self, p_likes_process_count, p_ocr_process_count, p_gat_pages):
        """
        Sets up class variable and launches the thread.

        :param p_likes_process_count: Process count for likes download
        :param p_ocr_process_count: Process count for image ocr
        :param p_gat_pages: flag to control whether :any:'CsBulkDownloader.get_pages()' is executed or not
        """
        super().__init__(daemon=True)

        #: Local logger
        self.m_logger = None

        #: Bulk Downloader
        self.m_bulk = None

        #: Thread letter ID (member inherited from :any:`threading.Thread`)
        self.name = 'B'

        #: process count for likes download
        self.m_likes_process_count = p_likes_process_count

        #: process count for image ocr
        self.m_ocr_process_count = p_ocr_process_count

        #: flag to avoid launching OCR processes
        self.m_gat_pages = p_gat_pages

    def full_init(self):
        # Local logger
        self.m_logger = logging.getLogger('CsBackgroundTask')

        self.m_bulk.full_init()

    def start_processes(self):
        print('CsBackgroundTask.start_processes()')
        # instantiate the bulk downloader class
        try:
            self.m_bulk = BulkDownloader(
                self.m_likes_process_count,
                self.m_ocr_process_count,
                self.m_gat_pages,
                self)
        except Exception as e:
            EcMailer.send_mail('Unable to instantiate bulk downloader', repr(e))
            raise

        self.m_bulk.start_processes()

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
        Presence of internet connection verification. Uses :any:`CsBackgroundTask.get_own_ip()`

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

        # Make sure internet is accessible and wait otherwise
        l_sleep_time = 30
        # maximum wait = 3 hours
        l_max_attempts = int(60 * 3 * 60.0 / l_sleep_time)
        l_attempt_counter = 0
        while not self.internet_check():
            self.m_logger.warning('Internet connection off')

            if l_attempt_counter >= l_max_attempts:
                self.m_logger.critical(
                    'Tried to connect for {0:0.2f} hours. Giving up.'.format((l_attempt_counter * l_sleep_time) / 3600),
                    extra={'m_errno': 2001})
                sys.exit(0)
            else:
                l_attempt_counter += 1

            # wait for 30 seconds
            time.sleep(l_sleep_time)

        self.m_logger.info('*** FB Sucking set-up complete')

        # Launch one bulk download procedure
        try:
            self.m_bulk.bulk_download()
        except Exception as e:
            self.m_logger.critical('Serious exception - Raising: ' + repr(e), extra={'m_errno': 2002})
            raise

    def reboot_trigger(self):
        """

        :return:
        """
        self.m_logger.info('reboot_trigger()')
        self.m_bulk.reboot_trigger()


class CsApp:
    """
    Main application class. Subclass of generic EC app class :any:`EcAppCore`

    This class perform two separate functions:

    * Instantiating the thread class performing the continuous downloading of FB stories (:any:`CsBackgroundTask`).
      Starting this thread is performed separately by :any:`CsApp.start_threads()`
    * Providing a response to the HTTP request for the display of results through the appropriate methods inherited
      from the base :any:`EcAppCore` class.
    """

    def __init__(self, p_likes_process_count, p_ocr_process_count, p_gat_pages):
        #: App core class (not subclassed)
        self.m_app_core = None

        #: logger
        self.m_logger = None

        if EcAppParam.gcm_startGathering:
            #: Background task performing continuous download of FB stories
            self.m_background = CsBackgroundTask(p_likes_process_count, p_ocr_process_count, p_gat_pages)
        else:
            self.m_background = None

        #: reboot trigger thread
        self.m_reboot_thread = None

    def full_init(self):
        # local logger
        self.m_logger = logging.getLogger('CsApp')

        # instantiate app core
        self.m_app_core = EcAppCore()

        # local logger for the background task
        if self.m_background is not None:
            self.m_background.full_init()

    def start_processes(self):
        if self.m_background is not None:
            self.m_background.start_processes()

    def start_threads(self):
        """
        Actual start of app threads. This is done separately so that the start-up process performed by
        :any:`StartApp.start_conspi_suck()` may be complete before the threads are started.

        List of one-letter thread names:

        * `µ`: application main thread (running the Http server)
        * `B`: main background tasks thread executing the bulk download process.
        * `H`: health-check thread of :any:`EcAppCore`
        * `U`: post update thread (started in :any:`BulkDownloader.start_threads`)
        * `L`: likes details download thread (started in :any:`BulkDownloader.start_threads`)
        * `I`: images download thread (started in :any:`BulkDownloader.start_threads`)
        * `O`: images OCR thread (started in :any:`BulkDownloader.start_threads`)
        * `r`: reboot trigger thread

        :return: Nothing
        """

        # starting the background tasks thread (will start the bulk download threads)
        if self.m_background is not None:
            self.m_background.start()
            self.m_logger.info('Background tasks thread started')

        # starting the generic app health check thread (as implemented in the parent's :any:`EcAppCore.run()`)
        self.m_app_core.start()
        self.m_logger.info('Health check thread started')

        # Reboot decision
        self.m_reboot_thread = Thread(target=self.reboot_check)
        # One-letter name for the posts update thread
        self.m_reboot_thread.name = 'r'
        self.m_reboot_thread.start()
        self.m_logger.info('Reboot trigger thread launched')

    def reboot_check(self):
        """

        :return:
        """

        while True:
            l_time_string = datetime.datetime.now().strftime('%H:%M')
            self.m_logger.debug('l_time_string: ' + l_time_string)

            if l_time_string == LocalParam.gcm_rebootTime:
                self.m_logger.info('requesting reboot')
                self.m_background.reboot_trigger()

            time.sleep(20)

    def get_response_get(self, p_request_handler):
        """
        Build the appropriate response based on the data provided by the request handler given in parameter.

        :param p_request_handler: an :any:`EcRequestHandler` instance providing the HTTP request parameters.
        :return: A string containing the HTML of the response
        """
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
        elif re.search('^/user/', p_request_handler.path):
            return self.one_user(p_request_handler)
        elif re.search('^/page/', p_request_handler.path):
            return self.one_page(p_request_handler)
        elif re.search('^/post/', p_request_handler.path):
            return self.one_post(p_request_handler)
        else:
            return self.dash()

    def dash(self):
        """
        Build the response for the "list of sessions" screen. No parameters necessary.

        :return: The list of sessions HTML.
        """
        l_conn = EcConnectionPool.get_global_pool().getconn('dash()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                select 
                    "R".*
                     , "CY_COUNT"/"PY_COUNT" as "RATIO"
                from (
                    select 
                        "P"."ID"
                        ,"P"."TX_NAME"
                        ,"PST"."MIN_DT"
                        ,"PST"."MAX_DT"
                        ,"PST"."PT_COUNT"
                        ,"PST"."CT_COUNT"
                        ,case when "PSM"."PM_COUNT" is null then 0 else "PSM"."PM_COUNT" end as "PM_COUNT"
                        ,case when "PSM"."CM_COUNT" is null then 0 else "PSM"."CM_COUNT" end as "CM_COUNT"
                        ,case when "PSY"."PY_COUNT" is null then 0 else "PSY"."PY_COUNT" end as "PY_COUNT"
                        ,case when "PSY"."CY_COUNT" is null then 0 else "PSY"."CY_COUNT" end as "CY_COUNT"
                        ,case when "PSW"."PW_COUNT" is null then 0 else "PSW"."PW_COUNT" end as "PW_COUNT"
                    from
                        "TB_PAGES" as "P"
                        join (
                            select 
                                "ID", 
                                min("DAY") as "MIN_DT",
                                max("DAY") as "MAX_DT",
                                sum("PD_COUNT") as "PT_COUNT", 
                                sum("CD COUNT") as "CT_COUNT"
                            from "TB_STATS_DAY"
                            group by "ID"
                        ) as "PST" on "PST"."ID" = "P"."ID"
                        left outer join (
                            select 
                                "ID", 
                                sum("PD_COUNT") as "PM_COUNT", 
                                sum("CD COUNT") as "CM_COUNT"
                            from "TB_STATS_DAY"
                            where DATE_PART('day', now()::date - "DAY") <= 30
                            group by "ID"
                        ) as "PSM" on "PSM"."ID" = "P"."ID"
                        left outer join (
                            select 
                                "ID", 
                                sum("PD_COUNT") as "PY_COUNT", 
                                sum("CD COUNT") as "CY_COUNT"
                            from "TB_STATS_DAY"
                            where DATE_PART('day', now()::date - "DAY") <= 365
                            group by "ID"
                        ) as "PSY" on "PSY"."ID" = "P"."ID"
                        left outer join (
                            select 
                                "ID", 
                                sum("PD_COUNT") as "PW_COUNT"
                            from "TB_STATS_DAY"
                            where DATE_PART('day', now()::date - "DAY") <= 7
                            group by "ID"
                        ) as "PSW" on "PSW"."ID" = "P"."ID"
                ) as "R"
                order by case when "PY_COUNT" < 5 then 0.01 else "CY_COUNT"/"PY_COUNT" end desc
            """)

            def display_ratio(p_comments, p_posts):
                if p_posts is None or p_comments is None or p_posts == 0:
                    return 'n/a'
                else:
                    return '{0:,.1f}'.format(p_comments / p_posts).replace(',', ' ')

            def fmt_int_none(p_num):
                # self.m_logger.info('p_num: {0}'.format(p_num))
                if p_num is None:
                    return ''
                else:
                    return '{0:,.0f}'.format(p_num).replace(',', ' ')
                    # return '{0:,n}'.format(p_num).replace(',', ' ')
                    # return '{:,d}'.format(p_num).replace(',', ' ')

            l_response = ''
            l_row_num = 0
            for \
                    l_page_id, l_page_name, \
                    l_dmin, l_dmax, \
                    l_count_pt, l_count_ct, \
                    l_count_pm, l_count_cm, \
                    l_count_py, l_count_cy, \
                    l_count_pw, _ in l_cursor:

                # with of the bracket of posts to be displayed initially for the page (in days)
                l_page_width = 7 if l_count_pw is not None and l_count_pw > 40 else 30

                l_response += """
                    <tr>
                        <td class="{15}">{0}</td>
                        <td class="{15}"><a href="/page/{0}/{13}/{14}">{1}</a></td>
                        <td class="{15}">{2}</td>
                        <td class="{15}">{3}</td>
                        <td class="{15}" style="text-align: right;">{4}</td>
                        <td class="{15}" style="text-align: right;">{5}</td>
                        <td class="{15}" style="text-align: right;">{6}</td>
                        <td class="{15}" style="text-align: right;">{7}</td>
                        <td class="{15}" style="text-align: right;">{8}</td>
                        <td class="{15}" style="text-align: right;">{9}</td>
                        <td class="{15}" style="text-align: right;">{10}</td>
                        <td class="{15}" style="text-align: right;">{11}</td>
                        <td class="{15}" style="text-align: right;">{12}</td>
                    <tr/>
                """.format(
                    l_page_id,
                    l_page_name,
                    l_dmin.strftime('%d/%m/%Y'),
                    l_dmax.strftime('%d/%m/%Y'),
                    fmt_int_none(l_count_pt), fmt_int_none(l_count_ct), display_ratio(l_count_ct, l_count_pt),
                    fmt_int_none(l_count_py), fmt_int_none(l_count_cy), display_ratio(l_count_cy, l_count_py),
                    fmt_int_none(l_count_pm), fmt_int_none(l_count_cm), display_ratio(l_count_cm, l_count_pm),
                    datetime.datetime.now().strftime('%Y.%m.%d'),
                    (datetime.datetime.now() - datetime.timedelta(days=l_page_width)).strftime('%Y.%m.%d'),
                    'row_{0}'.format(l_row_num % 3)
                )

                l_row_num += 1
        except Exception as e:
            self.m_logger.critical('Page stats query failure: {0}'.format(repr(e)), extra={'m_errno': 2003})
            raise

        l_cursor.close()
        EcConnectionPool.get_global_pool().putconn(l_conn)
        return """
            <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
            <head>
                <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                <style>
                    table, th, td {{
                        border: 1px solid black;
                    }}
                    th, td {{
                        padding-left: 5pt;
                        padding-right: 5pt;
                        padding-top: 2pt;
                        padding-bottom: 2pt;
                    }}
                    th {{
                        font-weight: bold;
                        font-family: sans-serif;
                    }}
                    td {{
                        font-family: monospace;
                    }}
                    tr:hover {{
                        background-color: #f5f5f5
                    }}
                    td.row_0 {{
                        background-color: #e5f5f8
                    }}
                    td.row_1 {{
                        background-color: #d0e0e8
                    }}
                    td.row_2 {{
                        background-color: #ffffff
                    }}
                </style>
            </head>
            <body>
                <table style="border-collapse: collapse;">
                    <tr>
                        <th style="text-align: center;" colspan="2">Page</td>
                        <th style="text-align: center;" colspan="2">Dates</td>
                        <th style="text-align: center;" colspan="3">Total</td>
                        <th style="text-align: center;" colspan="3">Year</td>
                        <th style="text-align: center;" colspan="3">Month</td>
                    <tr/>
                    <tr>
                        <th>ID</td>
                        <th>Name</td>
                        <th>Min</td>
                        <th>Max</td>
                        <th>Posts</td>
                        <th>Comments</td>
                        <th>Ratio</td>
                        <th>Posts</td>
                        <th>Comments</td>
                        <th>Ratio</td>
                        <th>Posts</td>
                        <th>Comments</td>
                        <th>Ratio</td>
                    <tr/>
                    {0}
                </table>
            </body>
            </html>
        """.format(l_response)

    def one_page(self, p_request_handler):
        """
        Build the HTML for an individual session screen, i.e. the list of stories retrieved from that session.

        :param p_request_handler: The :any:`EcRequestHandler` instance providing the session ID parameter.
        :return: The Session HTML.
        """
        l_match = re.search(r'/page/(\d+)/([\d\.]+)/([\d\.]+)', p_request_handler.path)
        if l_match:
            l_page_id = l_match.group(1)
            l_date_max = datetime.datetime.strptime(l_match.group(2), '%Y.%m.%d') + datetime.timedelta(days=1)
            l_date_min = datetime.datetime.strptime(l_match.group(3), '%Y.%m.%d')

            self.m_logger.info('l_page_id : {0}'.format(l_page_id))
            self.m_logger.info('l_date_max: {0}'.format(l_date_max))
            self.m_logger.info('l_date_min: {0}'.format(l_date_min))
        else:
            return """
                    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
                    <head>
                        <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                    </head>
                    <body>
                        <h1>Cannot extract Page ID + 2 dates from: {0}</h1>
                    </body>
                    </html>
                """.format(p_request_handler.path)

        l_conn = EcConnectionPool.get_global_pool().getconn('one_page()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                        select 
                            "O"."ID",
                            "O"."ST_FB_TYPE",
                            "O"."ST_FB_STATUS_TYPE",
                            "O"."DT_CRE",
                            "O"."TX_NAME",
                            "O"."TX_CAPTION", 
                            "O"."TX_DESCRIPTION",
                            "O"."TX_STORY",
                            "O"."TX_MESSAGE",
                            "O"."N_LIKES",
                            "O"."N_SHARES",
                            "O"."N_COMM",
                            "M"."MEDIA_COUNT",
                            "M"."OCR_COUNT",
                            "P"."TX_NAME",
                            "O"."ID_USER",
                            "U"."ST_NAME"
                        from 
                            "TB_OBJ" as "O" 
                            join "TB_PAGES" as "P" on "O"."ID_PAGE" = "P"."ID"
                            left outer join "TB_USER" as "U" on "U"."ID" = "O"."ID_USER"
                            left outer join (
                                select 
                                    "ID_OWNER", 
                                    count(1) as "MEDIA_COUNT",
                                    sum(case when "TX_TEXT" is not null then 1 else 0 end) "OCR_COUNT"
                                from "TB_MEDIA"
                                group by "ID_OWNER"
                            ) as "M" on "O"."ID" = "M"."ID_OWNER"
                        where 
                            "O"."ID_PAGE" = %s
                            and "O"."ST_TYPE" = 'Post'
                            and "O"."ID_PAGE" = "O"."ID_USER"
                            and "O"."DT_CRE" < %s
                            and "O"."DT_CRE" >= %s
                        order by "O"."DT_CRE" desc;
                    """,
                             #(l_page_id, l_date_max.strftime('%Y-%m-%d'), l_date_min.strftime('%Y-%m-%d'))
                             (l_page_id, l_date_max, l_date_min)
                             )

            self.m_logger.info('SQL: ' + l_cursor.query.decode('utf-8'))

            l_response = ''
            l_page_name = ''
            for l_id_post, \
                l_fb_type, \
                l_fb_status_type, \
                l_dt, \
                l_name, \
                l_caption, \
                l_desc, \
                l_story, \
                l_message, \
                l_likes, \
                l_shares,\
                l_comm_count,\
                l_media_count,\
                l_ocr_count,\
                l_page_name,\
                l_id_user,\
                l_user_name\
                    in l_cursor:

                def cut_max(s, p_max_len):
                    if s is None or len(s) == 0:
                        return ''
                    elif len(s) < p_max_len:
                        return s
                    else:
                        return s[:int(p_max_len)] + '...'

                l_total = len(l_name) + len(l_caption) + len(l_desc) + len(l_story) + 2*len(l_message)
                l_target = 100

                if l_total > 0:
                    l_name = cut_max(l_name, len(l_name) * l_target / l_total)
                    l_caption = cut_max(l_caption, len(l_caption) * l_target / l_total)
                    l_desc = cut_max(l_desc, len(l_desc) * l_target / l_total)
                    l_story = cut_max(l_story, len(l_story) * l_target / l_total)
                    l_message = cut_max(l_message, len(l_message) * 2 * l_target / l_total)

                l_display_text = '♦'.join([l_name, l_caption, l_desc, l_story, l_message])

                l_response += """
                            <tr>
                                <td><a class="FB_Link" href="https://www.facebook.com/{0}" target="_blank">{1}</a></td>
                                <td><a class="Post_Link" href="/post/{0}" target="_blank">{2}</a></td>
                                <td><a class="User_Link" href="/user/{3}" target="_blank">{4}</a></td>
                                <td style="text-align: right;">{5}</td>
                                <td style="text-align: right;">{6}</td>
                                <td style="text-align: right;">{7}</td>
                                <td style="text-align: center;">{8}</td>
                                <td>{9}</td>
                            <tr/>
                        """.format(
                    l_id_post,
                    l_dt.strftime('%d/%m/%Y %H:%M'),
                    l_fb_type + ('/' + l_fb_status_type if len(l_fb_status_type) > 0 else ''),
                    l_id_user,
                    cut_max(l_user_name, 25),
                    '{:,.0f}'.format(l_likes).replace(',', ' '),
                    '{:,.0f}'.format(l_shares).replace(',', ' '),
                    '{:,.0f}'.format(l_comm_count).replace(',', ' ') if l_comm_count is not None else '',
                    ('{0}'.format(l_media_count) if l_media_count is not None else '') +
                        ('*' if l_ocr_count is not None and l_ocr_count > 0 else ''),
                    l_display_text
                )
        except Exception as e:
            self.m_logger.critical('TB_OBJ query failure: {0}'.format(repr(e)), extra={'m_errno': 2004})
            raise

        l_cursor.close()
        EcConnectionPool.get_global_pool().putconn(l_conn)
        return """
            <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
            <head>
                <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                <style>
                    th, td {{
                        padding-right: 1em;
                    }}
                    th {{
                        font-weight: bold;
                        font-family: sans-serif;
                        text-align: left;
                    }}
                    td {{
                        font-family: monospace;
                    }}
                    h1, h2 {{
                        font-family: sans-serif;
                    }}
                    a {{
                        text-decoration: none;
                    }}
                    a.FB_Link{{
                        font-weight: bold;
                        color: RoyalBlue;
                    }}
                    a.FB_Link:hover{{
                        color: RoyalBlue;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.Post_Link{{
                        font-weight: bold;
                        color: SeaGreen;
                    }}
                    a.Post_Link:hover{{
                        color: SeaGreen;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.Post_Link:visited{{
                        color: DarkKhaki;
                        font-weight: none;
                    }}
                    a.User_Link{{
                        font-weight: bold;
                        color: IndianRed;
                    }}
                    a.User_Link:hover{{
                        color: IndianRed;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                </style>
            </head>
            <body>
                <h1>Page: {0}</h1>
                <h2>Path: {1}</h1>
                <table>
                    <tr>
                        <th>Date</th>
                        <th>FB Type</th>
                        <th>From</th>
                        <th>Likes</th>
                        <th>Shares</th>
                        <th>Comm.</th>
                        <th>Media</th>
                        <th>Text</th>
                    <tr/>
                    {2}
                </table>
            </body>
            </html>
        """.format(l_page_name, p_request_handler.path, l_response)

    def one_post(self, p_request_handler):
        """
        Build the HTML for an individual story screen.

        :param p_request_handler: The :any:`EcRequestHandler` instance providing the story ID parameter.
        :return: The story HTML.
        """

        def fmt_int_none(p_num):
            if p_num is None:
                return ''
            else:
                return '{:,d}'.format(p_num).replace(',', ' ')

        # the story ID is the last member of the URL
        l_match = re.search('/post/([\d_]+)$', p_request_handler.path)
        if l_match:
            l_post_id = l_match.group(1)
            l_comment_id = None
        else:
            l_match = re.search('/post/([\d_]+)/([\d_]+)$', p_request_handler.path)
            if l_match:
                l_post_id = l_match.group(1)
                l_comment_id = l_match.group(2)
            else:
                return """
                    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
                    <head>
                        <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                    </head>
                    <body>
                        <h1>Cannot extract Post ID (+ optionally Comm ID) from: {0}</h1>
                    </body>
                    </html>
                """.format(p_request_handler.path)

        self.m_logger.info('l_post_id: {0}'.format(l_post_id))
        self.m_logger.info('l_comment_id: {0}'.format(l_comment_id))

        l_comments_count = 0
        l_conn = EcConnectionPool.get_global_pool().getconn('one_post()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute(
                """
                    select 
                        "O"."ST_FB_TYPE",
                        "O"."ST_FB_STATUS_TYPE",
                        "O"."DT_CRE",
                        "O"."TX_NAME",
                        "O"."TX_CAPTION", 
                        "O"."TX_DESCRIPTION",
                        "O"."TX_STORY",
                        "O"."TX_MESSAGE",
                        "O"."N_LIKES",
                        "O"."N_SHARES",
                        "O"."N_COMM",
                        "M"."IMG_COUNT",
                        "U"."ST_NAME",
                        "P"."TX_NAME",
                        "O"."ID_USER"
                    from 
                        "TB_OBJ" as "O"
                        join "TB_PAGES" as "P" on "O"."ID_PAGE" = "P"."ID"
                        left outer join (
                            select "ID_OWNER", count(1) as "IMG_COUNT" 
                            from "TB_MEDIA" 
                            where not "F_FROM_PARENT"
                            group by "ID_OWNER"  
                        ) as "M" on "O"."ID" = "M"."ID_OWNER"
                        left outer join "TB_USER_UNIQUE" as "U" on "O"."ID_USER" = "U"."ID"
                    where "O"."ID" = %s;
                """, (l_post_id,)
            )

            self.m_logger.info('SQL: ' + l_cursor.query.decode('utf-8'))

            l_response = ''
            for l_fb_type, \
                l_fb_status_type, \
                l_dt, \
                l_name, \
                l_caption, \
                l_desc, \
                l_story, \
                l_message, \
                l_likes, \
                l_shares,\
                l_comments_count,\
                l_img_count,\
                l_user_name,\
                l_page_name,\
                l_id_user\
                    in l_cursor:

                # l_img_display = ''
                # for l_imgB64 in l_story['images']:
                #    l_img_display += """
                #        <img src="data:image/png;base64,{0}">
                #    """.format(l_imgB64)

                # <tr>
                #    <td colspan="2">{14}</td>
                # <tr/>
                # <tr>
                #    <td colspan="2" style="word-wrap:break-word;">{15}</td>
                # <tr/>

                # ('<img src="data:image/{1};base64,{0}" ><br/>'.format(l_base_64, l_fmt)
                #    if l_base_64 is not None else '') + \
                # ('<img src="data:image/{1};base64,{0}" ><br/>'.format(l_base_64_pic, l_fmt)
                #    if l_base_64_pic is not None else '') + \

                l_conn_media = EcConnectionPool.get_global_pool().getconn('one_post() - Media')
                l_cursor_media = l_conn.cursor()
                try:
                    l_cursor_media.execute(
                        """
                            select 
                                "TX_TARGET"
                                ,"TX_MEDIA_SRC"
                                ,"TX_PICTURE"
                                ,"TX_FULL_PICTURE"
                                ,"TX_BASE64"
                                ,"TX_BASE64_FP"
                                ,"ST_FORMAT"
                                ,"ST_FORMAT_FP"
                                ,"TX_TEXT"
                                ,"TX_VOCABULARY"
                            from "TB_MEDIA" 
                            where not "F_FROM_PARENT" and "ID_OWNER" = %s
                            order by "ID_MEDIA_INTERNAL";
                        """, (l_post_id,)
                    )
                except Exception as e:
                    self.m_logger.critical('TB_MEDIA query failure: {0}'.format(repr(e)), extra={'m_errno': 2004})
                    raise

                l_img_string = ''
                for l_media_target,\
                    l_media_src,\
                    l_picture,\
                    l_full_picture,\
                    l_base_64, \
                    l_base_64_fp,\
                    l_fmt,\
                    l_fmt_fp,\
                    l_text,\
                    l_vocabulary\
                    in l_cursor_media:

                        l_skeleton = """
                            <tr><td colspan="2">
                                <img src="data:image/{1};base64,{0}" >
                            </td></tr>
                        """
                        if l_img_count < 2:
                            if l_base_64_fp is not None:
                                l_one_img = l_skeleton.format(l_base_64_fp, l_fmt_fp)
                            elif l_base_64 is not None:
                                l_one_img = l_skeleton.format(l_base_64, l_fmt)
                            else:
                                l_one_img = ''
                        else:
                            l_one_img = None
                            if l_media_src is not None:
                                l_match = re.search(r'w\=(\d+)\&h\=(\d+)', l_media_src)
                                if l_match and l_match.group(1) == l_match.group(2) and l_base_64_fp is not None:
                                    l_one_img = l_skeleton.format(l_base_64_fp, l_fmt_fp)

                                l_match = re.search(r'/[ps](\d+)x(\d+)/', l_media_src)
                                if l_match and l_match.group(1) == l_match.group(2) and l_base_64_fp is not None:
                                    l_one_img = l_skeleton.format(l_base_64_fp, l_fmt_fp)

                            if l_one_img is None:
                                if l_base_64 is not None:
                                    l_one_img = l_skeleton.format(l_base_64, l_fmt)
                                else:
                                    l_one_img = ''

                        l_img_string += """
                            <tr>
                                <td class="Post" style="font-family: sans-serif; border-top: 2px solid black; 
                                    font-weight: bold; vertical-align: top;">Media&nbsp;Target:</td>
                                <td class="Post" style="border-top: 2px solid black;">{0}</td>
                            <tr/>
                            <tr>
                                <td class="Post" style="font-family: sans-serif; 
                                    font-weight: bold; vertical-align: top;">Media&nbsp;Src:</td>
                                <td class="Post">{1}</td>
                            <tr/>
                            <tr>
                                <td class="Post" style="font-family: sans-serif; 
                                    font-weight: bold; vertical-align: top;">Picture:</td>
                                <td class="Post">{2}</td>
                            <tr/>
                            <tr>
                                <td class="Post" style="font-family: sans-serif; 
                                    font-weight: bold; vertical-align: top;">Full&nbsp;Picture:</td>
                                <td class="Post">{3}</td>
                            <tr/>
                            <tr>
                                <td class="Post" style="font-family: sans-serif; 
                                    font-weight: bold; vertical-align: top;">Text:</td>
                                <td class="Post">{5}</td>
                            <tr/>
                            <tr>
                                <td class="Post" style="font-family: sans-serif; 
                                    font-weight: bold; vertical-align: top;">Vocabulary:</td>
                                <td class="Post">{6}</td>
                            <tr/>
                            {4}
                        """.format(
                            l_media_target,
                            l_media_src,
                            l_picture,
                            l_full_picture,
                            l_one_img,
                            l_text,
                            l_vocabulary)

                l_cursor_media.close()
                EcConnectionPool.get_global_pool().putconn(l_conn_media)

                l_response += """
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">ID</td>
                        <td class="Post">{0}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Page</td>
                        <td class="Post">{14}</td>
                    <tr/>
                    <tr>
                        <td class="Post"style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">From</td>
                        <td class="Post"><a class="User_Link" href="/user/{15}" target="_blank">{13}</a></td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">FB&nbsp;Type</td>
                        <td class="Post">{1}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; 
                            font-weight: bold; vertical-align: top;">FB&nbsp;Status&nbsp;Type</td>
                        <td class="Post">{2}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Date:</td>
                        <td class="Post">{3}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Name:</td>
                        <td class="Post">{4}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Caption:</td>
                        <td class="Post">{5}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Description:</td>
                        <td class="Post">{6}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Story:</td>
                        <td class="Post">{7}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Message:</td>
                        <td class="Post">{8}</td>
                    <tr/>
                    <tr>
                        <td class="Post" style="font-family: sans-serif; font-weight: bold; 
                            vertical-align: top;">Likes/Shares/Comments:</td>
                        <td class="Post">{9}&nbsp;/&nbsp;{10}&nbsp;/&nbsp;{11}</td>
                    <tr/>
                    {12}
                """.format(
                    l_post_id,  # 0
                    l_fb_type,  # 1
                    l_fb_status_type,  # 2
                    l_dt.strftime('%d/%m/%Y %H:%M'),  # 3
                    l_name,  # 4
                    l_caption,  # 5
                    l_desc,  # 6
                    l_story,  # 7
                    l_message,  # 8
                    fmt_int_none(l_likes),  # 9
                    fmt_int_none(l_shares),  # 10
                    fmt_int_none(l_comments_count),  # 11
                    l_img_string,  # 12
                    l_user_name,  # 13
                    l_page_name,  # 14
                    l_id_user  # 15
                )
        except Exception as e:
            self.m_logger.warning('TB_OBJ query failure: {0}'.format(repr(e)))
            raise

        l_cursor.close()
        EcConnectionPool.get_global_pool().putconn(l_conn)

        # Comments
        l_comments = ''
        if l_comments_count > 0:
            l_comments, _, _ = self.get_comments(l_post_id, l_comment_id, 1)

        return """
            <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
            <head>
                <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                <style>
                    table.Post, td.Post {{
                        border: 1px solid black;
                    }}
                    td.Post {{
                        padding-left: 5pt;
                        padding-right: 5pt;
                        padding-top: 2pt;
                        padding-bottom: 2pt;
                        font-family: monospace;
                    }}
                    p.Comment {{
                        margin:0;
                        font-family: monospace;
                        padding-top: 0;
                        padding-bottom: 4px;
                        padding-left: 0;
                    }}
                    td {{
                        margin: 0;
                        padding: 0;
                        border-spacing: 0;
                    }}
                    table {{
                        margin: 0;
                        padding: 0;
                        border-spacing: 0;
                    }}
                    a {{
                        text-decoration: none;
                    }}
                    a.FB_Link{{
                        font-weight: bold;
                        color: RoyalBlue;
                    }}
                    a.FB_Link:hover{{
                        color: RoyalBlue;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.Post_Link{{
                        font-weight: bold;
                        color: SeaGreen;
                    }}
                    a.Post_Link:hover{{
                        color: SeaGreen;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.User_Link{{
                        font-weight: bold;
                        color: IndianRed;
                    }}
                    a.User_Link:hover{{
                        color: IndianRed;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                </style>
            </head>
            <body>
                <table class="Post" style="border-collapse: collapse;">
                    {0}
                </table>
                <div style="border: 1px solid black; padding-top: 4pt;">
                    {1}
                </div>
            </body>
            </html>
        """.format(l_response, l_comments)

    def get_comments(
            self, p_parent_id, p_comment_anchor_id, p_depth, p_bkg=0, p_img_fifo=collections.deque(20*[0], 20)):
        """

        :param p_parent_id:
        :param p_depth:
        :param p_bkg:
        :param p_img_fifo:
        :return:
        """

        l_bkg_list = ['AntiqueWhite', 'Aquamarine', 'Khaki', 'Lavender', 'Thistle', 'Pink']
        l_bkg_id = p_bkg
        l_img_fifo = p_img_fifo

        l_conn = EcConnectionPool.get_global_pool().getconn('get_comments()')
        l_cursor = l_conn.cursor()
        l_html = ''
        t0 = time.perf_counter()
        try:
            l_cursor.execute(
                """
                    select 
                        "O"."ID"
                        ,"O"."DT_CRE"
                        ,"O"."TX_MESSAGE"
                        ,"O"."N_LIKES"
                        ,"O"."N_COMM"
                        ,"M"."ST_FORMAT"
                        ,"M"."TX_BASE64"
                        ,"M"."TX_TEXT"
                        ,"M"."TX_VOCABULARY"
                        ,"M"."TX_MEDIA_SRC"
                        ,"S"."ST_NAME"
                        ,"O"."ID_USER"
                        ,"S"."LIKES_COUNT" 
                        ,"S"."POSTS_COUNT" 
                        ,"S"."COMMENTS_COUNT" 
                        ,"S"."TOTAL_COUNT" 
                        ,"S"."COUNT_PAGE" 
                    from 
                        "TB_OBJ" as "O"
                        left outer join "TB_MEDIA" as "M" on "O"."ID" = "M"."ID_OWNER"
                        left outer join "TB_USER_STATS" as "S" on "O"."ID_USER" = "S"."ID"
                    where "O"."ID_FATHER" = %s and "O"."ST_TYPE"='Comm'
                    order by "O"."DT_CRE";
                """, (p_parent_id,)
            )
            t1 = time.perf_counter()

            l_time_proc = 0
            l_time_sub = 0
            for \
                    l_id, \
                    l_dt, \
                    l_msg, \
                    l_likes, \
                    l_comments_count, \
                    l_fmt, \
                    l_b64, \
                    l_txt, \
                    l_voc, \
                    l_src, \
                    l_user_name, \
                    l_id_user, \
                    l_user_likes_count, \
                    l_user_posts_count, \
                    l_user_comments_count, \
                    l_user_total_count, \
                    l_user_pages_count \
                            in l_cursor:

                s0 = time.perf_counter()
                l_color = l_bkg_list[l_bkg_id % len(l_bkg_list)]

                if l_b64 is not None and len(l_b64) > 0:
                    l_is_sticker = True if re.search('/v/t39\.1997-6', l_src) else False
                    l_previous_img_count = sum(list(l_img_fifo))
                    l_float = l_previous_img_count < 3 or l_is_sticker

                    l_para_style = ' style="background-color: {0};"'.format(l_color)
                    l_image_html = """
                        {0}<img style="{1}{2}padding: 1em; background-color: {3}; " '.format(l_color) 
                        src="data:image/{4};base64,{5}"/>
                    """.format(
                        '<br/>' if not l_float else '',
                        'float: right; ' if l_float else '',
                        'width: 50px; height: 50px; ' if l_is_sticker else '',
                        l_color,
                        l_fmt,
                        l_b64)

                    l_bkg_id += 1

                    l_vars = 'l_img_fifo:{0} l_previous_img_count:{1} l_is_sticker:{2}'.format(
                        list(l_img_fifo), l_previous_img_count, l_is_sticker)

                    if not l_is_sticker:
                        l_img_fifo.append(1)
                    else:
                        l_img_fifo.append(0)
                else:
                    l_image_html = ''
                    l_para_style = ''
                    l_img_fifo.append(0)
                    l_vars = ''

                l_user_name = '' if l_user_name is None else l_user_name
                l_user_name += ' {0}-[{1}/{2}/{3}]-{4}'.format(
                    l_user_total_count if l_user_total_count is not None else 0,
                    l_user_posts_count if l_user_posts_count is not None else 0,
                    l_user_comments_count if l_user_comments_count is not None else 0,
                    l_user_likes_count if l_user_likes_count is not None else 0,
                    l_user_pages_count if l_user_pages_count is not None else 0)

                l_categories = Dummy.get_categories(l_msg)
                l_html += """
                    <div{10} style="margin-left: {0}em;">
                        <p class="Comment"{1}>
                            <table style="display:inline; border-collapse: collapse;"><tr><td>
                            <a class="User_Link" href="/user/{9}" target="_blank">{2}</a></td><td> 
                            <span style="color: DarkGray; font-size: smaller;">[{3} / {4} likes]</span>
                            </td><td style="color: Red;">{8}<td></tr></table> 
                            {5} <span style="color: Blue;">{11}</span>
                            {6} 
                            <span style="color: Green;">{7}</span>
                        </p>
                    </div>
                """.format(
                    (p_depth-1)*2 + 1,  # 0
                    l_para_style,  # 1
                    l_user_name,  # 2
                    l_dt.strftime('%d/%m/%Y %H:%M'),  # 3
                    l_likes,  # 4
                    l_msg,  # 5
                    l_image_html,  # 6
                    '<br/>{0}/{1}'.format(l_txt, l_voc) if l_txt is not None and len(l_txt) > 0 else '',  # 7
                    '',  # l_vars   # 8
                    l_id_user,  # 9
                    ' id="K{0}"'.format(p_comment_anchor_id) if l_id == p_comment_anchor_id else '',  # 10
                    ''  # l_categories  # 11
                )

                if not LocalParam.gcm_prodEnv:
                    l_time_proc += time.perf_counter() - s0

                s0 = time.perf_counter()
                l_html_add = ''
                if l_comments_count > 0:
                    l_html_add, l_bkg_id, l_img_fifo = \
                        self.get_comments(l_id, p_comment_anchor_id, p_depth+1, l_bkg_id, l_img_fifo)

                l_html += l_html_add
                if not LocalParam.gcm_prodEnv:
                    l_time_sub += time.perf_counter() - s0

        except Exception as e:
            self.m_logger.critical('TB_OBJ query failure: {0}'.format(repr(e)), extra={'m_errno': 2005})
            raise

        l_cursor.close()
        EcConnectionPool.get_global_pool().putconn(l_conn)

        if not LocalParam.gcm_prodEnv:
            l_html += '<div style="color: red; font-family:monospace; ' + \
                      'margin-left: {0}em;">'.format((p_depth-1)*2 + 1) + \
                      '{0}/{1}/{2} --> {3}</div>'.format(
                            '{:,.3f}'.format(t1 - t0),
                            '{:,.3f}'.format(l_time_proc),
                            '{:,.3f}'.format(l_time_sub),
                            '{:,.3f}'.format(time.perf_counter() - t0)
                      )

        return l_html, l_bkg_id, l_img_fifo

    def one_user(self, p_request_handler):
        """

        :param p_request_handler:
        :return:
        """

        def cut_max(s, p_max_len):
            if s is None or len(s) == 0:
                return ''
            elif len(s) < p_max_len:
                return s
            else:
                return s[:int(p_max_len)] + '...'

        def fmt_int_none(p_num):
            if p_num is None:
                return ''
            else:
                return '{:,d}'.format(p_num).replace(',', ' ')

        l_response = ''
        # the user ID is the last member of the URL
        l_user_id = re.sub('/user/', '', p_request_handler.path)
        self.m_logger.info('l_post_id: {0}'.format(l_user_id))

        l_conn = EcConnectionPool.get_global_pool().getconn('one_user()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute(
                """
                    select "E".*
                        ,"S"."LIKES_COUNT" 
                        ,"S"."POSTS_COUNT" 
                        ,"S"."COMMENTS_COUNT" 
                        ,"S"."TOTAL_COUNT" 
                        ,"S"."COUNT_PAGE" 
                        ,"X"."N_COMM"
                    from (
                        select 
                            "O"."ID",
                            "O"."ID_POST",
                            "O"."ID_PAGE",
                            "O"."ST_TYPE",
                            "O"."ST_FB_TYPE",
                            "O"."ST_FB_STATUS_TYPE",
                            "O"."DT_CRE",
                            "O"."TX_NAME",
                            "O"."TX_CAPTION", 
                            "O"."TX_DESCRIPTION",
                            "O"."TX_STORY",
                            "O"."TX_MESSAGE",
                            "O"."N_LIKES",
                            "O"."N_SHARES",
                            "O"."N_COMM",
                            "M"."IMG_COUNT",
                            "U"."ST_NAME",
                            "U"."ID" as "ID_USER",
                            "P"."TX_NAME",
                            "N"."POST_COUNT"
                        from 
                            (
                                select 
                                    "OL"."ID",
                                    "OL"."ID_POST",
                                    "OL"."ID_PAGE",
                                    'Like' as "ST_TYPE",
                                    "OL"."ST_FB_TYPE",
                                    "OL"."ST_FB_STATUS_TYPE",
                                    "OL"."DT_CRE",
                                    "OL"."TX_NAME",
                                    "OL"."TX_CAPTION", 
                                    "OL"."TX_DESCRIPTION",
                                    "OL"."TX_STORY",
                                    "OL"."TX_MESSAGE",
                                    "OL"."N_LIKES",
                                    "OL"."N_SHARES",
                                    "OL"."N_COMM",
                                    "U"."ID" as "ID_USER"
                                from 
                                    "TB_USER_UNIQUE" "U" join "TB_LIKE" "L" on "L"."ID_USER_INTERNAL" = "U"."ID_INTERNAL"
                                    join "TB_OBJ" "OL" on "L"."ID_OBJ_INTERNAL" = "OL"."ID_INTERNAL"
                                    join "TB_USER_STATS" "S" on "U"."ID" = "S"."ID"
                            )as "O"
                            join "TB_PAGES" as "P" on "O"."ID_PAGE" = "P"."ID"
                            left outer join (
                                select "ID_OWNER", count(1) as "IMG_COUNT" 
                                from "TB_MEDIA" 
                                where not "F_FROM_PARENT"
                                group by "ID_OWNER"  
                            ) as "M" on "O"."ID" = "M"."ID_OWNER"
                            left outer join "TB_USER_UNIQUE" as "U" on "O"."ID_USER" = "U"."ID"
                            left outer join ( 
                                select "ID_PAGE", count(1) as "POST_COUNT"
                                from "TB_OBJ" 
                                where "ST_TYPE" = 'Post' and DATE_PART('day', now()::date - "DT_CRE") <= 7
                                group by "ID_PAGE"
                            ) as "N" on "O"."ID_PAGE" = "N"."ID_PAGE"
                        where "O"."ID_USER" = %s
                    union all
                        select 
                            "O"."ID",
                            "O"."ID_POST",
                            "O"."ID_PAGE",
                            "O"."ST_TYPE",
                            "O"."ST_FB_TYPE",
                            "O"."ST_FB_STATUS_TYPE",
                            "O"."DT_CRE",
                            "O"."TX_NAME",
                            "O"."TX_CAPTION", 
                            "O"."TX_DESCRIPTION",
                            "O"."TX_STORY",
                            "O"."TX_MESSAGE",
                            "O"."N_LIKES",
                            "O"."N_SHARES",
                            "O"."N_COMM",
                            "M"."IMG_COUNT",
                            "U"."ST_NAME",
                            "U"."ID" as "ID_USER",
                            "P"."TX_NAME",
                            "N"."POST_COUNT"
                        from 
                            "TB_OBJ" as "O"
                            join "TB_PAGES" as "P" on "O"."ID_PAGE" = "P"."ID"
                            left outer join (
                                select "ID_OWNER", count(1) as "IMG_COUNT" 
                                from "TB_MEDIA" 
                                where not "F_FROM_PARENT"
                                group by "ID_OWNER"  
                            ) as "M" on "O"."ID" = "M"."ID_OWNER"
                            left outer join "TB_USER_UNIQUE" as "U" on "O"."ID_USER" = "U"."ID"
                            left outer join ( 
                                select "ID_PAGE", count(1) as "POST_COUNT"
                                from "TB_OBJ" 
                                where "ST_TYPE" = 'Post' and DATE_PART('day', now()::date - "DT_CRE") <= 7
                                group by "ID_PAGE"
                            ) as "N" on "O"."ID_PAGE" = "N"."ID_PAGE"
                        where "O"."ID_USER" = %s
                    ) as "E" 
                    left outer join "TB_USER_STATS" as "S" on "S"."ID" = "E"."ID_USER"
                    left outer join "TB_OBJ" as "X" on "X"."ID" = "E"."ID_POST"
                    order by "E"."DT_CRE" desc;
                """, (l_user_id, l_user_id)
            )
        except Exception as e:
            self.m_logger.critical('TB_OBJ query failure: {0}'.format(repr(e)), extra={'m_errno': 2006})
            raise

        l_user_name = ''
        l_id_user = ''
        l_user_likes_count = 0
        l_user_posts_count = 0
        l_user_comments_count = 0
        l_user_total_count = 0
        l_user_pages_count = 0
        for l_id,\
            l_id_post,\
            l_id_page, \
            l_type, \
            l_fb_type, \
            l_fb_status_type, \
            l_dt, \
            l_name, \
            l_caption, \
            l_desc, \
            l_story, \
            l_message, \
            l_likes, \
            l_shares, \
            l_comments, \
            l_img_count, \
            l_user_name, \
            l_id_user, \
            l_page_name, \
            l_page_post_count,\
            l_user_likes_count, \
            l_user_posts_count, \
            l_user_comments_count, \
            l_user_total_count, \
            l_user_pages_count,\
            l_post_comments\
                in l_cursor:

            l_total = len(l_name) + len(l_caption) + len(l_desc) + len(l_story) + 2 * len(l_message)
            l_target = 100

            if l_total > 0:
                l_name = cut_max(l_name, len(l_name) * l_target / l_total)
                l_caption = cut_max(l_caption, len(l_caption) * l_target / l_total)
                l_desc = cut_max(l_desc, len(l_desc) * l_target / l_total)
                l_story = cut_max(l_story, len(l_story) * l_target / l_total)
                l_message = cut_max(l_message, len(l_message) * 2 * l_target / l_total)

            l_display_text = '♦'.join([l_name, l_caption, l_desc, l_story, l_message])

            # with of the bracket of posts to be displayed initially for the page (in days)
            l_page_width = 7 if l_page_post_count is not None and l_page_post_count > 40 else 30

            l_response += """
                    <tr>
                        <td><a class="FB_Link" href="https://www.facebook.com/{6}" target="_blank">{0}</a></td>
                        <td><a class="Post_Link" href="/post/{8}" target="_blank">{1}</a></td>
                        <td><a class="Page_Link" href="/page/{7}" target="_blank">{2}</a></td>
                        <td>{3}</td>
                        <td>{4}</td>
                        <td>{5}</td>
                    <tr/>
                """.format(
                    l_dt.strftime('%d/%m/%Y %H:%M'),
                    l_type,
                    l_page_name,
                    l_fb_type + ('/{0}'.format(l_fb_status_type)
                                 if l_fb_status_type is not None and len(l_fb_status_type) > 0 else ''),
                    '{0} | {1} | {2}'.format(fmt_int_none(l_likes), fmt_int_none(l_shares), fmt_int_none(l_comments))
                        if l_fb_type != 'Comment'
                        else '{0} + {1}'.format(fmt_int_none(l_likes), fmt_int_none(l_post_comments)),
                    l_display_text,
                    l_id,
                    '{0}/{1}/{2}'.format(
                        l_id_page,
                        datetime.datetime.now().strftime('%Y.%m.%d'),
                        (datetime.datetime.now() - datetime.timedelta(days=l_page_width)).strftime('%Y.%m.%d')
                    ),
                    '{0}/{1}#K{1}'.format(l_id_post, l_id) if l_fb_type == 'Comment' else l_id
                )

        l_cursor.close()
        EcConnectionPool.get_global_pool().putconn(l_conn)

        return """
            <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" >
            <head>
                <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
                <style>
                    th, td {{
                        padding-right: 1em;
                    }}
                    th {{
                        font-weight: bold;
                        font-family: sans-serif;
                        text-align: left;
                    }}
                    td {{
                        font-family: monospace;
                    }}
                    h1, h2 {{
                        font-family: sans-serif;
                        font-size: large;
                    }}
                    a {{
                        text-decoration: none;
                    }}
                    a.FB_Link{{
                        font-weight: bold;
                        color: RoyalBlue;
                    }}
                    a.FB_Link:hover{{
                        color: RoyalBlue;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.Post_Link{{
                        font-weight: bold;
                        color: SeaGreen;
                    }}
                    a.Post_Link:hover{{
                        color: SeaGreen;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.Post_Link:visited{{
                        color: DarkKhaki;
                        font-weight: none;
                    }}
                    a.User_Link{{
                        font-weight: bold;
                        color: IndianRed;
                    }}
                    a.User_Link:hover{{
                        color: IndianRed;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                    a.Page_Link{{
                        font-weight: bold;
                        color: Crimson;
                    }}
                    a.Page_Link:hover{{
                        color: Crimson;
                        font-style: italic;
                        text-decoration: underline;
                    }}
                </style>
            </head>
            <body>
                <h1>User: <a class="FB_Link" href="https://www.facebook.com/{0}" target="_blank">
                    <span style="font-family: monospace;">{1}</span></a></h1>
                <table>
                    <tr>
                        <th>Date</th>
                        <th>Type</th>
                        <th>Page</th>
                        <th>FB Type</th>
                        <th>Likes|Shares|Comm.<br/>Likes+Post Comm.</th>
                        <th>Text</th>
                    <tr/>
                        {2}
                </table>
            </body>
            </html>
        """.format(
            l_id_user,
            '{0} {1}-[{2}/{3}/{4}]-{5}'.format(l_user_name,
                                               l_user_total_count if l_user_total_count is not None else 0,
                                               l_user_posts_count if l_user_posts_count is not None else 0,
                                               l_user_comments_count if l_user_comments_count is not None else 0,
                                               l_user_likes_count if l_user_likes_count is not None else 0,
                                               l_user_pages_count if l_user_pages_count is not None else 0),
            l_response)