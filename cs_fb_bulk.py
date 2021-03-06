#!/usr/bin/python3
# -*- coding: utf-8 -*-

from PIL import ImageEnhance, ImageFilter

import json
import base64
import socket
import subprocess
from tesserocr import PyTessBaseAPI, RIL

from cs_fb_connect import *
from wrapvpn import *

__author__ = 'Pavan Mahalingam'

# extra={'m_errno': 1090}

# ----------------------------------- Tesseract -----------------------------------------------------------
# https://pypi.python.org/pypi/tesserocr
# apt-get install tesseract-ocr libtesseract-dev libleptonica-dev
# sudo pip3 install Cython
# sudo apt-get install g++
# sudo apt-get install python3-dev
# sudo pip3 install tesserocr


class BulkDownloaderException(Exception):
    def __init__(self, p_message):
        super(BulkDownloaderException, self).__init__(p_message)


class PageIDMigration(Exception):
    def __init__(self, p_message, p_old_id, p_new_id):
        self.m_old_id = p_old_id
        self.m_new_id = p_new_id

        super(PageIDMigration, self).__init__(p_message)


class BulkDownloader:
    """
    Bogus class used to isolate the bulk downloading (FB API) features.

    .. code-block:: sql

        select "P"."ID", "P"."TX_NAME", "O"."ST_TYPE", count(1), max("O"."DT_CRE"), min("O"."DT_CRE")
        from "TB_OBJ" as "O" join "TB_PAGES" as "P" on "O"."ID_PAGE" = "P"."ID"
        group by "P"."ID", "P"."TX_NAME", "O"."ST_TYPE"
        order by "P"."TX_NAME", "O"."ST_TYPE" desc;

    """
    def __init__(
            self,
            p_likes_process_count,
            p_ocr_process_count,
            p_gat_pages,
            p_background_task):

        #: process count for likes download
        self.m_likes_process_count = p_likes_process_count

        #: process count for image ocr
        self.m_ocr_process_count = p_ocr_process_count

        #: flag to control execution of :any:`get_pages()`
        self.m_gat_pages = p_gat_pages

        #: Local copy of the background task class instance to allow calling its methods.
        self.m_background_task = p_background_task

        #: Class-specific logger
        self.m_logger = None

        #: Number of times an object storage operation has been attempted
        self.m_objectStoreAttempts = 0

        #: Number of objects stored successfully
        self.m_objectStored = 0

        #: Number of posts retrieved
        self.m_postRetrieved = 0

        #: Number of comments retrieved
        self.m_commentRetrieved = 0

        #: Number of FB API requests performed
        self.m_FBRequestCount = 0

        #: Name of current page being downloaded
        self.m_page = None

        #: Posts update Thread
        self.m_posts_update_thread = None

        #: Image fetching Thread
        self.m_image_fetch_thread = None

        #: Shares fetching Thread
        self.m_shares_fetch_thread = None

        #: Process watchdog thread
        self.m_process_watchdog_thread = None

        #: Main thread
        self.m_main_thread = None

        #: Likes details download Processes
        self.m_likes_details_process = []

        #: Process lock for likes download
        self.m_likes_lock = None

        #: OCR Processes
        self.m_ocr_process = []

        #: Process lock for image OCR
        self.m_ocr_lock = None

        #: Boolean variable controlling the threads. When `False`, all threads stop.
        self.m_threads_proceed = True

        # Spell Checker should not complain ... Grrrr
        self.m_long_token = [
            ('EAAVaTJxF5KoBAIzoFZB3ONtwWSBoePun29cKECBZCwaCW60BNLesTTGZBAGtsCs0mAsHF3VF8KGtLo9ker8evLfseZCpuvf' +
             '92oknLmtBbDZAZA0C9e76ytTCUtng85sZCP5JhJwtZBaPxUC7owFq16YIuEaQvatAYGxIsKoZC6K1q7AZDZD',
                datetime.datetime.strptime('29/03/2018', '%d/%m/%Y')) ,
            ('EAAVaTJxF5KoBAMrIn22zKWUQThJIjeZBhpH2dpERciOpl4ghZAdbumZCOLel2avgdFh07X4ormLFG2c1uXY486gcqZB5nt' +
             'IYZAKqwha6EezFT0SJV1btC5BNhu1zuryqZAXek2KDNa1oU0qiE4ZCbuprhtDYn7eUYcZD',
                datetime.datetime.strptime('29/03/2018', '%d/%m/%Y')),
            ('EAAVaTJxF5KoBADuTGKnEogArzZAkC0FzohiQD6OsHyWDuPON2B6xm9yflAEcwYd4YdeL6Su93tXfG5KH7WK9FamIXhgp' +
             'Ks1G2yT39BKs1HnqdPX2GxaMVqZBZBu3sSj1VKxSDbjQvRIq0NgBRu9sv1CisuTZA3C9uEVJ9ujLigZDZD',
             datetime.datetime.strptime('29/03/2018', '%d/%m/%Y'))
        ]

        #: reboot required flag
        self.must_reboot = False

        #: Dictionary of UNICODE ligatures, to make sure none are kept in OCR text
        self.m_lig_dict = {
            'Ꜳ': 'AA',
            'ꜳ': 'aa',
            'Æ': 'AE',
            'æ': 'ae',
            'Ꜵ': 'AO',
            'ꜵ': 'ao',
            'Ꜷ': 'AJ',
            'ꜷ': 'aj',
            'Ꜹ': 'AV',
            'ꜹ': 'av',
            'Ꜻ': 'Av',
            'ꜻ': 'av',
            'Ꜽ': 'AY',
            'ꜽ': 'ay',
            'ȸ': 'db',
            'Ǳ': 'DZ',
            'ǲ': 'Dz',
            'ǳ': 'dz',
            'Ǆ': 'DZ',
            'ǅ': 'Dz',
            'ǆ': 'dz',
            'ʥ': 'dz',
            'ʤ': 'Dz',
            '🙰': 'ex',
            'ﬀ': 'ff',
            'ﬃ': 'ffi',
            'ﬄ': 'ffl',
            'ﬁ': 'fi',
            'ﬂ': 'Fl',
            'ʩ': 'fn',
            'Ĳ': 'IJ',
            'ĳ': 'ij',
            'Ǉ': 'LJ',
            'ǈ': 'Lj',
            'ǉ': 'lj',
            'ʪ': 'ls',
            'ʫ': 'lz',
            'ɮ': 'lz',
            'Œ': 'OE',
            'œ': 'oe',
            'Ꝏ': 'OO',
            'ꝏ': 'oo',
            'Ǌ': 'NJ',
            'ǋ': 'Nj',
            'ǌ': 'nj',
            'ȹ': 'op',
            'ẞ': 'SS',
            'ß': 'ss',
            'ﬆ': 'st',
            'ﬅ': 'ft',
            'ʨ': 'ta',
            'ʦ': 'ts',
            'ʧ': 'ts',
            'Ꜩ': 'Tz',
            'ꜩ': 'tz',
            'ᵫ': 'ue',
            'ꭐ': 'uil',
            'Ꝡ': 'VY',
            'ꝡ': 'vy',
        }

    def start_processes(self):
        """
        Launch the process based tasks: OCR & likes download.

        :return: Nothing.
        """
        print('BulkDownloader.start_processes()')

        # likes details download processes
        self.m_likes_lock = multiprocessing.Lock()
        # lock is acquired to block the process start
        self.m_likes_lock.acquire()
        for l_process_number in range(self.m_likes_process_count):
            p = multiprocessing.Process(target=self.repeat_get_likes_details, args=(self.m_likes_lock,))
            p.name = 'L{0}'.format(l_process_number)
            self.m_likes_details_process.append(p)
            p.start()

        # OCR process
        self.m_ocr_lock = multiprocessing.Lock()
        # lock is acquired to block the process start
        self.m_ocr_lock.acquire()
        for l_process_number in range(self.m_ocr_process_count):
            p = multiprocessing.Process(target=self.repeat_ocr_image, args=(self.m_ocr_lock,))
            p.name = 'O{0}'.format(l_process_number)
            self.m_ocr_process.append(p)
            p.start()

        print('BulkDownloader.start_processes() End: ' + multiprocessing.current_process().name)

    def process_watchdog(self):
        """

        :return:
        """
        while True:
            self.m_logger.info('Process and threads Watchdog')

            # Processes
            for l_process_number in range(self.m_ocr_process_count):
                if self.m_ocr_process[l_process_number].is_alive():
                    self.m_logger.info('Process O{0} is alive'.format(l_process_number))
                else:
                    self.m_logger.info('Process O{0} is dead'.format(l_process_number))
                    self.resume_ocr(l_process_number)

                    p = multiprocessing.Process(target=self.repeat_ocr_image, args=(self.m_ocr_lock,))
                    p.name = 'O{0}'.format(l_process_number)
                    self.m_ocr_process[l_process_number] = p
                    p.start()

            # Threads
            if self.m_main_thread is not None:
                if not self.m_main_thread.is_alive():
                    self.m_logger.error('Main Bulk Download Thread is dead', extra={'m_errno': 1087})
                    self.m_main_thread = None

                    self.m_main_thread = Thread(target=self.main_loop())
                    # One-letter name for the main loop thread
                    self.m_main_thread.name = 'B'
                    self.m_main_thread.start()
                    self.m_logger.info('main loop thread re-launched')
                else:
                    self.m_logger.info('Main Bulk Download Thread is alive')

            if self.m_posts_update_thread is not None:
                if not self.m_posts_update_thread.is_alive():
                    self.m_logger.error('Posts update thread is dead', extra={'m_errno': 1088})
                    self.m_posts_update_thread = None

                    self.m_posts_update_thread = Thread(target=self.repeat_posts_update)
                    # One-letter name for the posts update thread
                    self.m_posts_update_thread.name = 'U'
                    self.m_posts_update_thread.start()
                    self.m_logger.info('Posts update thread re-launched')
                else:
                    self.m_logger.info('Posts update Thread is alive')

            if self.m_image_fetch_thread is not None:
                if not self.m_image_fetch_thread.is_alive():
                    self.m_logger.error('Image fetch thread is dead', extra={'m_errno': 1089})
                    self.m_image_fetch_thread = None

                    self.m_image_fetch_thread = Thread(target=self.repeat_fetch_images)
                    # One-letter name for the Image fetching thread
                    self.m_image_fetch_thread.name = 'I'
                    self.m_image_fetch_thread.start()
                    self.m_logger.info('Image fetch thread re-launched')
                else:
                    self.m_logger.info('Image fetch Thread is alive')

            if self.m_shares_fetch_thread is not None:
                if not self.m_shares_fetch_thread.is_alive():
                    self.m_logger.error('Shares fetch thread is dead', extra={'m_errno': 1090})
                    self.m_shares_fetch_thread = None

                    self.m_shares_fetch_thread = Thread(target=self.repeat_fetch_shares)
                    # One-letter name for the Image fetching thread
                    self.m_shares_fetch_thread.name = 'S'
                    self.m_shares_fetch_thread.start()
                    self.m_logger.info('Shares fetch thread re-launched')
                else:
                    self.m_logger.info('Shares fetch Download Thread is alive')

            time.sleep(60)

    def full_init(self):
        # Local logger
        self.m_logger = logging.getLogger('BulkDownloader')
        self.m_logger.info('BulkDownloader.full_init() End')

    def start_threads(self):
        """
        Threads and processes Launch.

        :return: Nothing
        """
        # posts update thread
        self.m_posts_update_thread = Thread(target=self.repeat_posts_update)
        # One-letter name for the posts update thread
        self.m_posts_update_thread.name = 'U'
        self.m_posts_update_thread.start()
        self.m_logger.info('Posts update thread launched')

        # Image fetching thread
        self.m_image_fetch_thread = Thread(target=self.repeat_fetch_images)
        # One-letter name for the Image fetching thread
        self.m_image_fetch_thread.name = 'I'
        self.m_image_fetch_thread.start()
        self.m_logger.info('Image fetch thread launched')

        # Image fetching thread
        self.m_shares_fetch_thread = Thread(target=self.repeat_fetch_shares)
        # One-letter name for the Image fetching thread
        self.m_shares_fetch_thread.name = 'S'
        self.m_shares_fetch_thread.start()
        self.m_logger.info('Shares fetch thread launched')

        # Process watchdog thread
        self.m_process_watchdog_thread = Thread(target=self.process_watchdog)
        # One-letter name for the Image fetching thread
        self.m_process_watchdog_thread.name = 'w'
        self.m_process_watchdog_thread.start()
        self.m_logger.info('Process watchdog thread launched')

        self.m_main_thread = threading.current_thread()

    def new_process_init(self):
        """
        Makes the necessary adjustments to function in a new process:

        * Mailer init.
        * Connection pool init.
        * Instantiation of local logger.

        :return:
        """
        threading.current_thread().name = 'µ'

        GlobalStart.basic_env_start()

        self.full_init()

    def tasks_before_start(self, p_minimal=False):
        """

        :return:
        """

        self.m_logger.info('tasks_before_start() Start')

        # ConspiSuck DB Housekeeping queries --------------------------------------------------------------------------
        if not p_minimal:
            with open(os.path.join(LocalParam.gcm_appRoot, 'housekeeping.sql'), 'r') as f:
                for r in f.read().split(';'):
                    if r is not None:
                        r = r.strip()
                        if len(r) > 0:
                            l_conn_write = EcConnectionPool.get_global_pool().getconn(
                                'BulkDownloader.tasks_before_start() Housekeeping')
                            l_cursor_write = l_conn_write.cursor()

                            try:
                                self.m_logger.info('Executing housekeeping request : ' + r)
                                t0 = time.time()
                                l_cursor_write.execute(r)

                                l_conn_write.commit()
                                l_elapsed = time.time() - t0
                                self.m_logger.info('Elapsed : {:,.2f} s.'.format(l_elapsed).replace(',', ' '))
                            except Exception as e:
                                l_conn_write.rollback()
                                l_msg = 'bulk_download Unknown Exception (Housekeeping) : {0}/{1}'.format(
                                    repr(e),
                                    l_cursor_write.query)
                                self.m_logger.critical(l_msg, extra={'m_errno': 1001})
                                raise BulkDownloaderException(l_msg)
                            finally:
                                # release DB handles when finished
                                l_cursor_write.close()
                                EcConnectionPool.get_global_pool().putconn(l_conn_write)

            self.m_logger.info('*** End Housekeeping Queries ***')

            # Vacuum Analyse ------------------------------------------------------------------------------------------
            self.m_logger.info('*** Database Vacuum ***')
            l_connect = psycopg2.connect(
                host=EcAppParam.gcm_dbServer,
                database=EcAppParam.gcm_dbDatabase,
                user=EcAppParam.gcm_dbUser,
                password=EcAppParam.gcm_dbPassword
            )
            l_connect.autocommit = True
            l_cursor_vacuum = l_connect.cursor()
            try:
                l_cursor_vacuum.execute('VACUUM FULL ANALYSE')
                self.m_logger.info('Database Vacuum Success')
            except psycopg2.Error as e:
                self.m_logger.error('Vacuum request failure: ' + repr(e), extra={'m_errno': 1002})
            finally:
                l_cursor_vacuum.close()
                l_connect.close()

            # System backup -------------------------------------------------------------------------------------------
            if LocalParam.gcm_doSystemBackup and not p_minimal:
                self.m_logger.info('*** Performing system backup ***')
                l_bkp_cmd = '/home/fi11222/DailyBkpIII/daily_backup.py -q'
                # l_bkp_cmd = '/home/fi11222/DailyBkpIII/daily_backup.py --dbOnly'
                try:
                    subprocess.call(l_bkp_cmd.split(' '))
                except subprocess.CalledProcessError as e:
                    self.m_logger.error('System backup - Error return code: {0}'.format(e.returncode),
                                        extra={'m_errno': 1003})
                self.m_logger.info('*** End system backup ***')

        # clear the locking flags -------------------------------------------------------------------------------------
        l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.tasks_before_start() TB_OBJ')
        l_cursor_write = l_conn_write.cursor()

        self.m_logger.info('Cleaning locks on TB_OBJ')
        try:
            l_cursor_write.execute("""
                update
                    "TB_OBJ"
                set
                    "F_LOCKED" = false
                where "F_LOCKED";
            """)

            l_conn_write.commit()
        except Exception as e:
            l_conn_write.rollback()
            l_msg = 'bulk_download Unknown Exception (TB_OBJ) : {0}/{1}'.format(repr(e), l_cursor_write.query)
            self.m_logger.critical(l_msg, extra={'m_errno': 1004})
            raise BulkDownloaderException(l_msg)
        finally:
            # release DB handles when finished
            l_cursor_write.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_write)

        l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.tasks_before_start() TB_MEDIA')
        l_cursor_write = l_conn_write.cursor()

        self.m_logger.info('Cleaning locks on TB_MEDIA')
        try:
            l_cursor_write.execute("""
                update
                    "TB_MEDIA"
                set
                    "F_LOCK" = NULL
                where "F_LOCK" is not NULL;
            """)

            l_conn_write.commit()
        except Exception as e:
            l_conn_write.rollback()
            l_msg = 'bulk_download Unknown Exception (TB_MEDIA) : {0}/{1}'.format(repr(e), l_cursor_write.query)
            self.m_logger.critical(l_msg, extra={'m_errno': 1005})
            raise BulkDownloaderException(l_msg)
        finally:
            # release DB handles when finished
            l_cursor_write.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_write)

        self.m_logger.info('tasks_before_start() End')

    def bulk_download(self):
        """
        Performs the core of the bulk-downloading tasks: New pages and new posts downloading.
        
        :return: Nothing
        """
        self.m_logger.info('Start bulk_download()')

        # tasks to perform before starting actual bulk download process
        self.tasks_before_start(p_minimal=not LocalParam.gcm_prodEnv)

        # release child processes
        self.m_logger.info('Release likes processes')
        self.m_likes_lock.release()
        time.sleep(1)
        self.m_likes_lock.acquire()
        self.m_logger.info('likes processes init complete')
        self.m_likes_lock.release()

        self.m_logger.info('Release OCR processes')
        self.m_ocr_lock.release()
        time.sleep(1)
        self.m_ocr_lock.acquire()
        self.m_logger.info('OCR processes init complete')
        self.m_ocr_lock.release()

        # Launch threads
        self.start_threads()

        l_reboot_path = os.path.join(LocalParam.gcm_appRoot, EcAppParam.gcm_rebootFile)
        with open(l_reboot_path, 'w') as f:
            f.write(datetime.datetime.now().strftime('%Y%m%d'))

        # start main loop
        self.main_loop(l_reboot_path)

    def main_loop(self, p_reboot_path):
        """

        :return:
        """
        # ############################################ MAIN LOOP ######################################################
        while True:
            self.m_logger.info('TOPBLK top of bulk_download() main loop')

            if self.m_gat_pages:
                self.get_pages()

            if LocalParam.gcm_do_posts:
                try:
                    self.get_posts()
                except BulkDownloaderException as e:
                    self.m_logger.error('bulk_download main loop exception capture: ' + repr(e),
                                        extra={'m_errno': 1006})

            # reboot ?
            self.m_logger.info('*** RBTEST Reboot Test ***')
            if self.must_reboot:
                # no more than one reboot per day
                with open(p_reboot_path, 'r') as f:
                    l_already_rebooted = f.read() == datetime.datetime.now().strftime('%Y%m%d')

                if not l_already_rebooted:
                    self.m_logger.info('*** REBOOT System reboot ***')
                    try:
                        # os.system('shutdown -r now')
                        # subprocess.run('shutdown --reboot now'.split(' '), shell=True, check=True)
                        subprocess.run('/sbin/reboot'.split(' '), shell=True, check=True)
                    except subprocess.CalledProcessError as e:
                        self.m_logger.error(
                            'Failed to reboot the system: ' +
                            'returncode: {0}|'.format(e.returncode) +
                            'stdout: {0}|'.format(e.stdout) +
                            'stderr: {0}|'.format(e.stderr), extra={'m_errno': 1007}
                        )
                else:
                    self.m_logger.info('*** NRBOOT Already rebooted ***')

                self.must_reboot = False

            self.m_logger.info('BOTBLK bottom of bulk_download() main loop')

            # sleep for an hour
            time.sleep(3600)
        # ############################################ END MAIN LOOP ##################################################

    def reboot_trigger(self):
        """

        :return:
        """
        self.m_logger.info('reboot_trigger()')
        self.must_reboot = True

    def get_pages(self):
        """
        Gets the page list from shares posted to TestPage. Each post to TestPage is a share of a post from a page.

        :return: Nothing
        """
        self.m_logger.info('Start getPages()')

        # This field list is far too wide but I keep it because it can be re-used.
        l_field_list = \
            'id,caption,created_time,description,from,icon,link,message,message_tags,name,object_id,' + \
            'permalink_url,picture,place,properties,shares,source,status_type,story,to,type,' + \
            'updated_time,with_tags,parent_id'

        # build the request
        l_request = 'https://graph.facebook.com/{0}/{1}/feed?limit={2}&fields={3}'.format(
            EcAppParam.gcm_api_version,
            '706557539525134',  # FB ID of page 'TestPage'
            EcAppParam.gcm_limit,
            l_field_list)

        # perform the request
        try:
            l_response = self.perform_request(l_request)
        except BulkDownloaderException as e:
            if str(e) == 'NON_EXIST':
                self.m_logger.critical('TestPage said not to exist !!', extra={'m_errno': 1008})
                sys.exit(0)
            else:
                raise

        self.m_logger.info('l_request:' + l_request)

        # Decode the JSON response from the FB API
        l_response_data = json.loads(l_response)
        l_finished = False
        while not l_finished:
            for l_post in l_response_data['data']:
                # if there is a parent_id, it means that it is a share (or at least this is what we assume).
                if 'parent_id' in l_post.keys():
                    # l_parent_id = l_post['parent_id']
                    l_parent_id = self.get_mandatory_field(l_post, 'parent_id', 'get_pages()')
                    self.m_logger.info('l_parent_id:' + l_parent_id)

                    # new request to get the parent post (the original post, not the share)
                    l_request_post = \
                        'https://graph.facebook.com/{0}/{1}?limit={2}&fields={3}'.format(
                            EcAppParam.gcm_api_version,
                            l_parent_id,
                            EcAppParam.gcm_limit,
                            l_field_list)

                    # perform the second request
                    try:
                        l_response_post = self.perform_request(l_request_post)
                    except BulkDownloaderException as e:
                        if str(e) == 'NON_EXIST':
                            self.m_logger.error(
                                'Page with ID {0} said by FB not to exist'.format(l_parent_id),
                                extra={'m_errno': 1009})
                            continue
                        else:
                            raise

                    # decode the JSON we got from the second request
                    l_response_post_data = json.loads(l_response_post)

                    # since this should be a page post, the 'from' field identifies the page it has been posted to.
                    if 'from' in l_response_post_data.keys():
                        l_from = l_response_post_data['from']
                        # l_page_id = l_from['id']
                        # l_page_name = l_from['name']
                        l_page_id = self.get_mandatory_field(l_from, 'id', 'get_pages()')
                        l_page_name = self.get_mandatory_field(l_from, 'name', 'get_pages()')

                        self.m_logger.info('Page id   :' + l_page_id)
                        self.m_logger.info('Page name :' + l_page_name)

                        # store page information
                        self.store_object(
                            p_padding='',
                            p_type='Page',
                            p_date_creation='',
                            p_date_modification='',
                            p_id=l_page_id,
                            p_parent_id='',
                            p_page_id='',
                            p_post_id='',
                            p_fb_type='Page',
                            p_fb_status_type='Page',
                            p_share_count=0,
                            p_like_count=0,
                            p_permalink_url='',
                            p_name=l_page_name)

            # end of for l_post in l_responseData['data']:

            # standard FB API paging handling
            if 'paging' in l_response_data.keys() and 'next' in l_response_data['paging'].keys():
                l_request = l_response_data['paging']['next']
                l_response = self.perform_request(l_request)

                l_response_data = json.loads(l_response)
            else:
                l_finished = True

        # end of while not l_finished:

        self.m_logger.info('End getPages()')

    def get_posts(self):
        """
        Gets the posts from all the pages in `TB_PAGES`

        To transfer from `TB_OBJ` to `TB_PAGES`:

        .. code-block:: sql

            delete from "TB_PAGES";
            insert into "TB_PAGES"("ID","DT_CRE","ST_TYPE","ST_FB_TYPE","TX_NAME","ID_OBJ_INTERNAL")
            select "ID","DT_CRE","ST_TYPE","ST_FB_TYPE","TX_NAME","ID_INTERNAL"
            from "TB_OBJ"
            where "ST_TYPE" = 'Page';

            delete from "TB_OBJ" where "ID_PAGE" in ('108734602494994', '448319605253405');
            DELETE FROM "TB_MEDIA"
            USING "TB_MEDIA" AS "M"
            LEFT OUTER JOIN "TB_OBJ" AS "O" ON
               "M"."ID_OWNER" = "O"."ID"
            WHERE
               "TB_MEDIA"."ID_MEDIA_INTERNAL" = "M"."ID_MEDIA_INTERNAL" AND
               "O"."ID" IS NULL;
            DELETE FROM "TB_USER"
            USING "TB_USER" AS "U"
            LEFT OUTER JOIN "TB_OBJ" AS "O" ON
               "U"."ID" = "O"."ID_USER"
            WHERE
               "TB_USER"."ID_INTERNAL" = "U"."ID_INTERNAL" AND
               "O"."ID_USER" IS NULL;
        
        :return: Nothing
        """
        self.m_logger.info('Start get_posts()')

        # get a connection from the pool and a cursor from the pool
        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.get_posts()')
        l_cursor = l_conn.cursor()

        # select all pages from TB_PAGES
        l_page_list = []
        try:
            l_cursor.execute("""
                select "ID", "TX_NAME" 
                from "TB_PAGES"
                where "F_DNL" = 'Y' and not "F_NON_EXIST" 
                order by "DT_CRE";
            """)

            for l_record in l_cursor:
                l_page_list.append(l_record)
        except Exception as e:
            self.m_logger.error('Error selecting from TB_PAGES: {0}/{1}'.format(repr(e), l_cursor.query),
                                extra={'m_errno': 1010})
            raise
        finally:
            # release DB objects once finished
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        # loop through all pages
        for l_id, l_name in l_page_list:
            self.m_logger.info('$$$$$$$$$ [{0}] $$$$$$$$$$'.format(l_name))
            # store the current page name for future reference (debug displays mostly)
            self.m_page = l_name
            # get posts from the current page
            try:
                self.get_posts_from_page(l_id)
            except PageIDMigration as e:
                l_old_id = e.m_old_id
                l_new_id = e.m_new_id
                self.m_logger.info('Migrating Page index from [{0}] to [{1}] Name: {2}'.format(
                    l_old_id, l_new_id, l_name
                ))

                # record the ID change in the DB
                self.migrate_id_page(l_old_id, l_new_id, l_name)

                # redo the query with the new ID
                self.get_posts_from_page(l_new_id)

            # Wait 1 second btw pages
            time.sleep(1)

        self.m_logger.info('End get_posts()')

    def get_posts_from_page(self, p_id):
        """
        Gets all new posts (not older than :any:`gcm_days_depth`) from a page and store them in the DB. Also, will
        not retrieve more than :any:`gcm_max_post` posts.

        :param p_id: ID (API App-specific) of the page to get the posts from
        :return: Nothing
        """
        self.m_logger.info('Start getPostsFromPage()')

        l_field_list = 'id,application,caption,created_time,description,from,icon,link,message,message_tags,name,' + \
                       'object_id,parent_id,permalink_url,picture,full_picture,place,properties,shares,' + \
                       'source,status_type,story,to,type,updated_time,with_tags'

        # API request: get all posts from the page :any:`p_id` in batches of size :any:`gcm_limit`
        l_request = 'https://graph.facebook.com/{0}/{1}/feed?limit={2}&fields={3}'.format(
            EcAppParam.gcm_api_version,
            p_id,
            EcAppParam.gcm_limit,
            l_field_list)

        # perform the request
        try:
            l_response = self.perform_request(l_request)
        except BulkDownloaderException as e:
            if str(e) == 'NON_EXIST':
                self.m_logger.error('Page with ID {0} said by FB not to exist'.format(p_id), extra={'m_errno': 1011})
                self.set_non_exist(p_id, p_page=True)
                return
            else:
                raise
        # decode the JSON request response
        l_response_data = json.loads(l_response)

        if 'data' not in l_response_data.keys():
            l_msg = 'malformed FB response: no "data" array in [{0}]'.format(l_response)
            self.m_logger.error(l_msg)
            raise BulkDownloaderException(l_msg)

        if len(l_response_data['data']) > 0:
            # l_latest_date, _ = BulkDownloader.get_optional_field(l_response_data['data'][0], 'created_time')
            l_latest_date = \
                self.get_mandatory_field(l_response_data['data'][0], 'created_time', 'get_posts_from_page()')
            # if len(l_latest_date) == 0:
            #    self.m_logger.warning('No "created_time" in [{0}]'.format(repr(l_response_data['data'][0])))
            self.m_logger.info('   Latest date: ' + l_latest_date)

        l_post_count = 0
        l_finished = False
        # loop through all returned posts
        while not l_finished:
            # 2 nested loops because of FB-specific paging mechanism (see below)
            for l_post in l_response_data['data']:
                # increment the total number of posts retrieved
                self.m_postRetrieved += 1

                self.m_logger.info(
                    '   =====[ {0}/{1} ]================POST========================='.format(
                        l_post_count, self.m_page))

                l_finished = self.process_one_post(p_id, p_id, l_post, p_shared_post=False)

                # break the inner for loop
                if l_finished:
                    break

                l_post_count += 1
                if l_post_count > EcAppParam.gcm_max_post:
                    self.m_logger.info(
                        '   ---> Maximum number of posts ({0}) reached, stop this page'.format(l_post_count))
                    l_finished = True
                    break
            # End of loop: for l_post in l_responseData['data']: (looping through current batch of posts)

            # FB API paging mechanism
            if 'paging' in l_response_data.keys() and 'next' in l_response_data['paging'].keys():
                self.m_logger.info('   *** Getting next post block ...')
                l_request = l_response_data['paging']['next']
                l_response = self.perform_request(l_request)

                l_response_data = json.loads(l_response)
            else:
                # if no more 'pages' (batches of posts) to load, break the loop
                break
        # end while not l_finished: (outer loop to handle paging)

        self.m_logger.info('End getPostsFromPage()')

    def process_one_post(self, p_id_page, p_id_parent, p_post_json, p_shared_post=False):
        """

        :param p_id_page:
        :param p_id_parent:
        :param p_post_json:
        :param p_shared_post:
        :return:
        """
        l_finished = False

        # basic post data items
        # l_post_id = p_post_json['id']
        l_post_id = self.get_mandatory_field(p_post_json, 'id', 'process_one_post()')
        # l_post_date, _ = BulkDownloader.get_optional_field(p_post_json, 'created_time')
        l_post_date = self.get_mandatory_field(p_post_json, 'created_time', 'process_one_post()')
        # l_type = p_post_json['type']
        l_type = self.get_mandatory_field(p_post_json, 'type', 'process_one_post()')
        l_shares = int(p_post_json['shares']['count']) if 'shares' in p_post_json.keys() else 0

        # if len(l_post_date) == 0:
        #     self.m_logger.warning('No "created_time" in [{0}]'.format(repr(p_post_json)))

        self.m_logger.info('   shared ?    : ' + 'yes' if p_shared_post else 'no')
        self.m_logger.info('   id          : ' + l_post_id)
        self.m_logger.info('   date        : ' + l_post_date)

        # decode the date format of the post creation date --> Python datetime
        # 2016-04-22T12:03:06+0000
        l_msg_date = datetime.datetime.strptime(
            re.sub(r'\+\d+$', '', l_post_date), '%Y-%m-%dT%H:%M:%S')
        self.m_logger.info('   date (P)    : {0}'.format(l_msg_date))

        # if message older than gcm_days_depth days ---> break loop
        l_days_old = (datetime.datetime.now() - l_msg_date).days
        self.m_logger.info('   Days old    : {0}'.format(l_days_old))
        if l_days_old > EcAppParam.gcm_days_depth and not p_shared_post:
            # the 'too old' criterion only applies for posts downloaded directly from pages
            self.m_logger.info(
                '   ---> Too old, stop getting posts from page [{0}]'.format(self.m_page))
            l_finished = True  # break the outer paging loop
        else:
            # gets the author (FB user) of the post
            l_user_id = ''
            if 'from' in p_post_json.keys():
                l_user_id, x = BulkDownloader.get_optional_field(p_post_json['from'], 'id')
                l_user_name, l_user_name_short = BulkDownloader.get_optional_field(p_post_json['from'], 'name')

                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info('   from        : {0} [{1}]'.format(l_user_name_short, l_user_id))

                # store user data
                self.store_user(l_user_id, l_user_name, l_post_date, '   ')

            # get additional data from the post
            l_name, l_name_short = BulkDownloader.get_optional_field(p_post_json, 'name')
            l_caption, l_caption_short = BulkDownloader.get_optional_field(p_post_json, 'caption')
            l_description, l_description_sh = BulkDownloader.get_optional_field(p_post_json, 'description')
            l_story, l_story_short = BulkDownloader.get_optional_field(p_post_json, 'story')
            l_message, l_message_short = BulkDownloader.get_optional_field(p_post_json, 'message')

            l_object_id, x = BulkDownloader.get_optional_field(p_post_json, 'object_id')
            l_parent_id, x = BulkDownloader.get_optional_field(p_post_json, 'parent_id')
            l_link, x = BulkDownloader.get_optional_field(p_post_json, 'link')
            l_picture, x = BulkDownloader.get_optional_field(p_post_json, 'picture')
            l_full_picture, x = BulkDownloader.get_optional_field(p_post_json, 'full_picture')
            l_source, x = BulkDownloader.get_optional_field(p_post_json, 'source')

            l_icon, x = BulkDownloader.get_optional_field(p_post_json, 'icon')
            l_permalink_url, x = BulkDownloader.get_optional_field(p_post_json, 'permalink_url')
            l_status_type, x = BulkDownloader.get_optional_field(p_post_json, 'status_type')
            l_updated_time, x = BulkDownloader.get_optional_field(p_post_json, 'updated_time')

            # additional post data requiring JSON decoding
            l_place = ''
            if 'place' in p_post_json.keys():
                l_place = json.dumps(p_post_json['place'])

            l_tags = ''
            if 'message_tags' in p_post_json.keys():
                l_tags = json.dumps(p_post_json['message_tags'])

            l_with_tags = ''
            if 'with_tags' in p_post_json.keys():
                l_with_tags = json.dumps(p_post_json['with_tags'])

            l_properties = ''
            if 'properties' in p_post_json.keys():
                l_properties = json.dumps(p_post_json['properties'])

            # debug display of post data
            self.m_logger.info('   name        : ' + l_name_short)
            if EcAppParam.gcm_verboseModeOn:
                self.m_logger.info('   caption     : ' + l_caption_short)
                self.m_logger.info('   description : ' + l_description_sh)
                self.m_logger.info('   story       : ' + l_story_short)
                self.m_logger.info('   message     : ' + l_message_short)
                self.m_logger.info('   permalink   : ' + l_permalink_url)
                self.m_logger.info('   icon        : ' + l_icon)
                self.m_logger.info('   object_id   : ' + l_object_id)
                self.m_logger.info('   parent_id   : ' + l_parent_id)
                self.m_logger.info('   shares      : {0}'.format(l_shares))
                self.m_logger.info('   type        : ' + l_type)
                self.m_logger.info('   updated time: ' + l_updated_time)
                self.m_logger.info('   with        : {0}'.format(l_with_tags))
                self.m_logger.info('   tags        : {0}'.format(l_tags))
                self.m_logger.info('   place       : {0}'.format(l_place))
                self.m_logger.info('   picture     : {0}'.format(l_picture))
                self.m_logger.info('   full pic.   : {0}'.format(l_full_picture))
                self.m_logger.info('   keys        : {0}'.format(p_post_json.keys()))

            # store post information
            if self.store_object(
                p_padding='   ',
                p_type='Post',
                p_date_creation=l_post_date,
                p_date_modification=l_updated_time,
                p_id=l_post_id,
                p_parent_id=p_id_parent,
                p_page_id=p_id_page,
                p_post_id='',
                p_fb_type=l_type,
                p_fb_status_type=l_status_type,
                p_share_count=l_shares,
                p_like_count=0,
                p_permalink_url=l_permalink_url,
                p_name=l_name,
                p_caption=l_caption,
                p_desc=l_description,
                p_story=l_story,
                p_message=l_message,
                p_fb_parent_id=l_parent_id,
                p_fb_object_id=l_object_id,
                p_link=l_link,
                p_place=l_place,
                p_source=l_source,
                p_user_id=l_user_id,
                p_tags=l_tags,
                p_with_tags=l_with_tags,
                p_properties=l_properties,
                p_shared=p_shared_post
            ):

                # get attachments and comments only if the storage of the post was successful, i.e. if
                # the post was a new one
                self.get_post_attachments(
                    l_post_id, l_post_id, l_status_type, l_source,
                    l_link, l_picture, l_full_picture, l_properties)

                self.get_comments(l_post_id, l_post_id, p_id_page, 0)

                # no need to get parent post if we already know it is shared
                if len(l_parent_id) > 0 and not p_shared_post:
                    self.get_parent_post(l_post_id, l_parent_id)
            else:
                # if already in DB ---> break loop because it means new posts are now exhausted
                self.m_logger.info(
                    '   ---> Post already in DB {0}'.format(
                        ', stop getting posts from page [{0}]'.format(self.m_page) if not p_shared_post
                        else ', probably a cross-share'
                    ))
                l_finished = True  # also break outer paging loop

        return l_finished

    def get_parent_post(self, p_post_id, p_fb_parent_id):
        """
        Gets data from the parent post of a post. The attachment are currently recorded, as well as the author
        (in case it is a page).

        :param p_post_id: ID of the post to which the attachments are to be linked.
        :param p_fb_parent_id: ID of the post from which the attachments are to be downloaded.
        :return:
        """
        self.m_logger.info('   get_parent_post() start. p_fb_parent_id: {0}'.format(p_fb_parent_id))

        l_field_list = 'id,application,caption,created_time,description,from,icon,link,message,message_tags,name,' + \
                       'object_id,parent_id,permalink_url,picture,full_picture,place,properties,shares,' + \
                       'source,status_type,story,to,type,updated_time,with_tags'

        # API request: get all posts from the page :any:`p_id` in batches of size :any:`gcm_limit`
        l_request = 'https://graph.facebook.com/{0}/{1}/?limit={2}&fields={3}'.format(
            EcAppParam.gcm_api_version,
            p_fb_parent_id,
            EcAppParam.gcm_limit,
            l_field_list)

        # perform the request
        try:
            l_response = self.perform_request(l_request)
        except BulkDownloaderException as e:
            if str(e) == 'NON_EXIST':
                self.m_logger.warning('Parent post with ID {0} said by FB not to exist'.format(p_fb_parent_id),
                                      extra={'m_errno': 1012})
                return
            else:
                raise
        # decode the JSON request response
        l_response_data = json.loads(l_response)

        # basic post data items
        # l_post_id = l_response_data['id']
        l_post_id = self.get_mandatory_field(l_response_data, 'id', 'get_parent_post()')
        # l_post_date, _ = BulkDownloader.get_optional_field(l_response_data, 'created_time')
        l_post_date = self.get_mandatory_field(l_response_data, 'created_time', 'get_parent_post()')
        # l_type = l_response_data['type']
        l_type = self.get_mandatory_field(l_response_data, 'type', 'get_parent_post()')

        # if len(l_post_date) == 0:
        #     self.m_logger.warning('No "created_time" in [{0}]'.format(repr(l_response_data)))

        # get additional data from the post
        l_name, l_name_short = BulkDownloader.get_optional_field(l_response_data, 'name')
        l_caption, l_caption_short = BulkDownloader.get_optional_field(l_response_data, 'caption')
        l_description, l_description_sh = BulkDownloader.get_optional_field(l_response_data, 'description')
        l_story, l_story_short = BulkDownloader.get_optional_field(l_response_data, 'story')
        l_message, l_message_short = BulkDownloader.get_optional_field(l_response_data, 'message')

        l_object_id, x = BulkDownloader.get_optional_field(l_response_data, 'object_id')
        l_parent_id, x = BulkDownloader.get_optional_field(l_response_data, 'parent_id')
        l_link, x = BulkDownloader.get_optional_field(l_response_data, 'link')
        l_picture, x = BulkDownloader.get_optional_field(l_response_data, 'picture')
        l_full_picture, x = BulkDownloader.get_optional_field(l_response_data, 'full_picture')
        l_source, x = BulkDownloader.get_optional_field(l_response_data, 'source')

        l_status_type, x = BulkDownloader.get_optional_field(l_response_data, 'status_type')

        # gets the author (FB user) of the post
        if 'from' in l_response_data.keys():
            l_user_id, x = BulkDownloader.get_optional_field(l_response_data['from'], 'id')
            l_user_name, l_user_name_short = BulkDownloader.get_optional_field(l_response_data['from'], 'name')

            if EcAppParam.gcm_verboseModeOn:
                self.m_logger.info('   from        : {0} [{1}]'.format(l_user_name_short, l_user_id))

            # API request: get metadata of poster to determine if it is a page
            l_request_from = 'https://graph.facebook.com/{0}/{1}/?metadata=1'.format(
                EcAppParam.gcm_api_version,
                l_user_id)

            # perform the request
            try:
                l_response_from = self.perform_request(l_request_from)
            except BulkDownloaderException as e:
                if str(e) == 'NON_EXIST':
                    self.m_logger.warning('User ID {0} said by FB not to exist'.format(l_user_id),
                                          extra={'m_errno': 1013})
                    return
                else:
                    raise
            # decode the JSON request response
            l_response_data_from = json.loads(l_response_from)

            if 'metadata' in l_response_data_from.keys():
                # l_type = l_response_data_from['metadata']['type']
                l_type = self.get_mandatory_field(l_response_data_from['metadata'], 'type', 'get_parent_post() 2')
                self.m_logger.info('      type     : {0} FRMTYP [{1}]'.format(l_type, l_user_name))

            # store user data
            self.store_user(l_user_id, l_user_name, l_post_date, '   ', l_type)

        l_properties = ''
        if 'properties' in l_response_data.keys():
            l_properties = json.dumps(l_response_data['properties'])

        self.m_logger.info(
            '   +++++++++++++++++++++++++POST PARENT [{0}]++++++++++++++++++++++++++++++++'.format(p_fb_parent_id))
        self.m_logger.info('   id          : ' + l_post_id)
        self.m_logger.info('   date        : ' + l_post_date)
        self.m_logger.info('   type        : ' + l_type)
        self.m_logger.info('   status type : ' + l_status_type)
        self.m_logger.info('   name        : ' + l_name_short)
        if EcAppParam.gcm_verboseModeOn:
            self.m_logger.info('   caption     : ' + l_caption_short)
            self.m_logger.info('   description : ' + l_description_sh)
            self.m_logger.info('   story       : ' + l_story_short)
            self.m_logger.info('   message     : ' + l_message_short)
            self.m_logger.info('   object_id   : ' + l_object_id)
            self.m_logger.info('   parent_id   : ' + l_parent_id)
            self.m_logger.info('   type        : ' + l_type)
            self.m_logger.info('   keys        : {0}'.format(l_response_data.keys()))

        # get the attachments (whole purpose of the function)
        self.get_post_attachments(
            p_fb_parent_id, p_post_id, l_status_type, l_source, l_link, l_picture, l_full_picture, l_properties)

        self.m_logger.info('   get_parent_post() end. p_fb_parent_id: {0}'.format(p_fb_parent_id))

    def get_post_attachments(
            self, p_post_id, p_owner_id, p_status_type, p_source, p_link, p_picture, p_full_picture, p_properties):
        """
        Gets all attachments from a post. Gets a number of parameters from the caller (:any:`get_posts_from_page`)
        which contain data related to media to be stored along attachments. This function calls
        :any:`scan_attachments` which does most of the work. The reason for this is that :any:`scan_attachments` may
        need to call itself recursively if there are sub-attachments.

        Two different ID parameters (:any:`p_post_id` and :any:`p_owner_id`) are provided to allow for the case
        in which attachments are obtained from the parent post, and thus need to be linked in `TB_MEDIA` to
        a different post from which they were obtained (see :any:`get_parent_post`).

        :param p_post_id: API App-specific ID of the post from which the attachments are to be obtained.
        :param p_owner_id: API App-specific ID of the post to which the attachments are to be linked.
        :param p_status_type: parent post related data
        :param p_source: parent post related data
        :param p_link: parent post related data
        :param p_picture: parent post related data
        :param p_full_picture: parent post related data
        :param p_properties: parent post related data
        :return: Nothing
        """
        self.m_logger.debug('Start get_post_attachments()')

        l_field_list = 'description,description_tags,media,target,title,type,url,attachments,subattachments'

        # API request: get list of attachments linked to this post
        l_request = 'https://graph.facebook.com/{0}/{1}/attachments?limit={2}&fields={3}'.format(
            EcAppParam.gcm_api_version,
            p_post_id,
            EcAppParam.gcm_limit,
            l_field_list)

        # perform the request
        try:
            l_response = self.perform_request(l_request)
        except BulkDownloaderException as e:
            if str(e) == 'NON_EXIST':
                self.m_logger.warning(
                    'Post with ID {0} said by FB not to exist (attachments query)'.format(p_post_id),
                    extra={'m_errno': 1014})
                self.set_non_exist(p_post_id)
                return
            else:
                raise
        # decode the JSON response
        l_response_data = json.loads(l_response)

        # call the recursive function that will do the data extraction and storage work
        self.scan_attachments(
            l_response_data['data'],
            p_owner_id,
            p_status_type,
            p_source,
            p_link,
            p_picture,
            p_full_picture,
            p_properties,
            1, 1,
            p_post_id != p_owner_id)  # the value of p_from_parent is true if the two post IDs are different

        self.m_logger.info('End get_post_attachments()')

    def scan_attachments(self,
                         p_attachment_list,
                         p_post_id,
                         p_status_type, p_source, p_link, p_picture, p_full_picture, p_properties,
                         p_depth_display, p_depth, p_from_parent=False):
        """
        Scans a JSON response fragment in order to get attachments and (through recursion) sub-attachments, if any.

        :param p_attachment_list: The dictionary resulting from the decoding of an API response.
        :param p_post_id: FB API App-specific ID of the parent post
        :param p_status_type: Post-related data to be stored alongside the attachment data
        :param p_source: Post-related data to be stored alongside the attachment data
        :param p_link: Post-related data to be stored alongside the attachment data
        :param p_picture: Post-related data to be stored alongside the attachment data
        :param p_full_picture: Post-related data to be stored alongside the attachment data
        :param p_properties: Post-related data to be stored alongside the attachment data
        :param p_depth_display: indentation depth for debug purposes. May be different from :any:`p_depth` if
            comment attachment
        :param p_depth: Sub-attachment depth. 1 if directly under post/comment
        :param p_from_parent: If true --> the attachment is taken from the parent but linked to the son.
        :return: Nothing
        """
        self.m_logger.debug('Start scan_attachments()')
        # spaces padding string for debug purposes
        l_depth_padding = ' ' * (p_depth_display * 3)

        l_attachment_count = 0
        # loop through the attachments in the fragment passed from the caller
        for l_attachment in p_attachment_list:
            # basic attachment data
            l_description, x = BulkDownloader.get_optional_field(l_attachment, 'description')
            l_title, x = BulkDownloader.get_optional_field(l_attachment, 'title')
            l_type, x = BulkDownloader.get_optional_field(l_attachment, 'type')
            l_url, x = BulkDownloader.get_optional_field(l_attachment, 'url')

            # list of tags requiring JSON decoding
            l_description_tags = None
            if 'description_tags' in l_attachment.keys():
                l_description_tags = json.dumps(l_attachment['description_tags'])

            l_src = None
            l_width = None
            l_height = None
            l_media = None
            # extraction of media-specific data items, if any
            if 'media' in l_attachment.keys():
                l_media = l_attachment['media']
                if list(l_media.keys()) == ['image']:
                    try:
                        # l_src = l_media['image']['src']
                        l_src = self.get_mandatory_field(l_media['image'], 'src', 'scan_attachments()')
                        # l_width = int(l_media['image']['width'])
                        l_width = int(self.get_mandatory_field(l_media['image'], 'width', 'scan_attachments()'))
                        # l_height = int(l_media['image']['height'])
                        l_height = int(self.get_mandatory_field(l_media['image'], 'height', 'scan_attachments()'))
                    except ValueError:
                        l_width_s = self.get_mandatory_field(l_media['image'], 'width', 'scan_attachments()')
                        l_height_s =self.get_mandatory_field(l_media['image'], 'height', 'scan_attachments()')

                        self.m_logger.warning('Cannot convert [{0}] or [{1}]'.format(l_width_s, l_height_s),
                                              extra={'m_errno': 1081})
                    except KeyError:
                        self.m_logger.warning('Missing key in: {0}'.format(l_media['image']), extra={'m_errno': 1082})
                l_media = json.dumps(l_attachment['media'])

            # target link, if any
            l_target = None
            if 'target' in l_attachment.keys():
                l_target = json.dumps(l_attachment['target'])

            # debug display of attachment data (alongside post-related data passed as parameters)
            self.m_logger.info(
                '{0}++++[ {1}/{2} ]++++++++++++{3}ATTACHMENT++++++++++++++++++++++++'.format(
                    l_depth_padding, l_attachment_count, self.m_page, 'SUB' if p_depth >= 2 else ''))

            self.m_logger.info('{0}Type        : {1}'.format(l_depth_padding, l_type))
            self.m_logger.info('{0}status type : {1}'.format(l_depth_padding, p_status_type))
            self.m_logger.info('{0}Description : {1}'.format(l_depth_padding, l_description))
            self.m_logger.info('{0}Title       : {1}'.format(l_depth_padding, l_title))
            self.m_logger.info('{0}Tags        : {1}'.format(l_depth_padding, l_description_tags))
            self.m_logger.info('{0}Target      : {1}'.format(l_depth_padding, l_target))
            self.m_logger.info('{0}Url         : {1}'.format(l_depth_padding, l_url))
            self.m_logger.info('{0}link        : {1}'.format(l_depth_padding, p_link))
            self.m_logger.info('{0}Media       : {1}'.format(l_depth_padding, l_media))
            self.m_logger.info('{0}Media/src   : {1}'.format(l_depth_padding, l_src))
            self.m_logger.info('{0}Media/width : {1}'.format(l_depth_padding, l_width))
            self.m_logger.info('{0}Media/height: {1}'.format(l_depth_padding, l_height))
            self.m_logger.info('{0}source      : {1}'.format(l_depth_padding, p_source))
            self.m_logger.info('{0}picture     : {1}'.format(l_depth_padding, p_picture))
            self.m_logger.info('{0}picture     : {1}'.format(l_depth_padding, p_full_picture))
            self.m_logger.info('{0}properties  : {1}'.format(l_depth_padding, p_properties))
            self.m_logger.info('{0}keys        : {1}'.format(l_depth_padding, l_attachment.keys()))

            # store attachment data in TB_MEDIA
            self.store_media(p_post_id, l_type, l_description, l_title, l_description_tags,
                             l_target, l_media, l_src, l_width, l_height, p_picture, p_full_picture, p_from_parent)

            # recursive call for sub-attachment, if any
            if 'subattachments' in l_attachment.keys():
                self.m_logger.info('$$$$$$$$ SUBATTACHEMENTS : {0}'.format(l_attachment['subattachments']['data']))
                self.scan_attachments(l_attachment['subattachments']['data'],
                                      p_post_id, p_status_type, p_source, p_link,
                                      p_picture, p_full_picture, p_properties,
                                      p_depth_display + 1, p_depth + 1, p_from_parent)

            l_attachment_count += 1
        # end of loop: for l_attachment in p_attachment_list:

        self.m_logger.debug('End scan_attachments()')

    def get_comments(self, p_id, p_post_id, p_page_id, p_depth):
        """
        Gets comments from a post or another comment.

        :param p_id: API App-specific ID of the parent post or comment.
        :param p_post_id: API App-specific ID of the parent post.
        :param p_page_id: API App-specific ID of the parent page of the post.
        :param p_depth: 1 if directly under post, >1 if under another comment.
        :return: Nothing.
        """
        self.m_logger.debug('Start scan_attachments()')
        # spaces padding string for debug purposes
        l_depth_padding = ' ' * ((p_depth + 2) * 3)

        # get list of comments attached to this post (or this comment)
        l_field_list = 'id,attachment,created_time,comment_count,from,like_count,message,message_tags,user_likes'

        l_request = 'https://graph.facebook.com/{0}/{1}/comments?limit={2}&fields={3}'.format(
            EcAppParam.gcm_api_version,
            p_id,
            EcAppParam.gcm_limit,
            l_field_list)

        # perform request
        try:
            l_response = self.perform_request(l_request)
        except BulkDownloaderException as e:
            if str(e) == 'NON_EXIST':
                self.m_logger.warning('Post with ID {0} said by FB not to exist (comments query)'.format(p_id),
                                      extra={'m_errno': 1015})
                self.set_non_exist(p_id)
                return
            else:
                raise
        # decode JSON request response
        l_response_data = json.loads(l_response)

        if 'data' not in l_response_data.keys():
            l_msg = 'malformed FB response: no "data" array in [{0}]'.format(l_response)
            self.m_logger.error(l_msg)
            raise BulkDownloaderException(l_msg)

        if len(l_response_data['data']) > 0:
            # l_latest_date, _ = BulkDownloader.get_optional_field(l_response_data['data'][0], 'created_time')
            l_latest_date = self.get_mandatory_field(l_response_data['data'][0], 'created_time', 'get_comments()')
            # if len(l_latest_date) == 0:
            #     self.m_logger.warning('No "created_time" in [{0}]'.format(repr(l_response_data['data'][0])))
            self.m_logger.info('{0}Latest date: '.format(l_depth_padding) + l_latest_date)

        l_comm_count = 0
        while True:
            # double loop for FB API paging handling
            for l_comment in l_response_data['data']:
                self.m_commentRetrieved += 1

                # basic comment data
                # l_comment_id = l_comment['id']
                l_comment_id = self.get_mandatory_field(l_comment, 'id', 'get_comments()')
                # l_comment_date, _ = BulkDownloader.get_optional_field(l_comment, 'created_time')
                l_comment_date = self.get_mandatory_field(l_comment, 'created_time', 'get_comments()')
                try:
                    # l_comment_likes = int(l_comment['like_count'])
                    l_comment_likes = int(self.get_mandatory_field(l_comment, 'like_count', 'get_comments()'))
                    # l_comment_c_count = int(l_comment['comment_count'])
                    l_comment_c_count = int(self.get_mandatory_field(l_comment, 'comment_count', 'get_comments()'))
                except ValueError:
                    l_comment_likes_s = self.get_mandatory_field(l_comment, 'like_count', 'get_comments()')
                    l_comment_c_count_s = self.get_mandatory_field(l_comment, 'comment_count', 'get_comments()')

                    self.m_logger.error(
                        'incorrect values for like_count [{0}] and comment_count [{1}]'.format(
                            l_comment_likes_s, l_comment_c_count_s))

                # if len(l_comment_date) == 0:
                #     self.m_logger.warning('No "created_time" in [{0}]'.format(repr(l_comment)))

                # debug display
                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info(
                        '{0}----[{1}]-----------COMMENT------------------------------'.format(
                            l_depth_padding, self.m_page))
                    self.m_logger.info('{0}id      : '.format(l_depth_padding) + l_comment_id)
                    self.m_logger.info('{0}date    : '.format(l_depth_padding) + l_comment_date)
                    self.m_logger.info('{0}likes   : {1}'.format(l_depth_padding, l_comment_likes))
                    self.m_logger.info('{0}sub com.: {1}'.format(l_depth_padding, l_comment_c_count))

                # comment author (user) data
                l_user_id = ''
                if 'from' in l_comment.keys():
                    l_user_id, x = BulkDownloader.get_optional_field(l_comment['from'], 'id')
                    l_user_name, l_user_name_short = BulkDownloader.get_optional_field(l_comment['from'], 'name')

                    if EcAppParam.gcm_verboseModeOn:
                        self.m_logger.info(
                            '{0}from    : {1} [{2}]'.format(l_depth_padding, l_user_name_short, l_user_id))

                    # store user data
                    self.store_user(l_user_id, l_user_name, l_comment_date, l_depth_padding)

                # comment text
                l_message, l_message_short = BulkDownloader.get_optional_field(l_comment, 'message')

                # comment tags list
                l_tags = ''
                if 'message_tags' in l_comment.keys():
                    l_tags = json.dumps(l_comment['message_tags'])

                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info('{0}message : '.format(l_depth_padding) + l_message_short)
                    self.m_logger.info('{0}tags    : '.format(l_depth_padding) + l_tags)

                # store comment information
                if self.store_object(
                        p_padding=l_depth_padding,
                        p_type='Comm',
                        p_date_creation=l_comment_date,
                        p_date_modification='',
                        p_id=l_comment_id,
                        p_parent_id=p_id,
                        p_page_id=p_page_id,
                        p_post_id=p_post_id,
                        p_fb_type='Comment',
                        p_fb_status_type='',
                        p_share_count=0,
                        p_like_count=l_comment_likes,
                        p_permalink_url='',
                        p_message=l_message,
                        p_user_id=l_user_id,
                        p_tags=l_tags,
                ):
                    l_comm_count += 1
                    # scan possible attachments if storage successful (i.e. it is a new comment)
                    if 'attachment' in l_comment.keys():
                        self.scan_attachments(
                            [l_comment['attachment']], l_comment_id, '', '', '', '', '', '', p_depth + 2, 1)

                # get sub-comments if any (recursive call)
                if l_comment_c_count > 0:
                    self.get_comments(l_comment_id, p_post_id, p_page_id, p_depth + 1)
            # end of loop: for l_comment in l_response_data['data']:

            # paging handling in the outer loop
            if 'paging' in l_response_data.keys() and 'next' in l_response_data['paging'].keys():
                self.m_logger.info('{0}[{1}] *** Getting next comment block ...'.format(l_depth_padding, l_comm_count))
                l_request = l_response_data['paging']['next']
                l_response = self.perform_request(l_request)

                l_response_data = json.loads(l_response)
            else:
                break
        # end of loop: while True:

        self.m_logger.info('{0}[End get_comments()] comment download count --> {1}'.format(
            l_depth_padding[:-3], l_comm_count))

    def repeat_fetch_shares(self):
        """

        :return:
        """
        self.m_logger.info('Start repeat_fetch_shares()')

        while True:
            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.repeat_fetch_shares()')
            l_cursor = l_conn.cursor()

            # counting posts to be updated
            l_count = 0
            try:
                l_cursor.execute("""
                    select count(1) as "COUNT"
                    from "TB_OBJ"
                    where
                        "ST_TYPE" = 'Post'
                        and DATE_PART('day', now()::date - "DT_CRE") >= %s
                        and not "F_SHARES_DOWNLOADED"
                        and "N_SHARES" > 0
                        and not "F_NON_EXIST";
                """, (EcAppParam.gcm_shares_depth,))

                for l_count, in l_cursor:
                    pass

                self.m_logger.info('PRGMTR-S Posts to be share downloaded: {0}'.format(l_count))
            except Exception as e:
                self.m_logger.critical(
                    'repeat_fetch_shares() Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                    extra={'m_errno': 1016})
                raise BulkDownloaderException('repeat_fetch_shares() Unknown Exception: {0}'.format(repr(e)))
            finally:
                # close DB access handles when finished
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            if l_count > 0 and LocalParam.gcm_do_shares:
                try:
                    self.shares_download()
                except BulkDownloaderException:
                    self.m_logger.info('Caught a BulkDownloaderException thrown from below - nothing to do')
                except Exception as e:
                    self.m_logger.error('Caught exception in repeat_fetch_shares() loop: ' + repr(e),
                                        extra={'m_errno': 1017})
                time.sleep(1)
            else:
                # no posts to update --> wait 5 minutes
                time.sleep(5 * 60)

    def shares_download(self):
        """

        :return:
        """
        self.m_logger.info('Start shares_download()')

        # get DB connection and cursor
        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.shares_download()')
        l_cursor = l_conn.cursor()

        l_post_list = []
        try:
            l_cursor.execute("""
                select "ID", "ID_PAGE", "N_SHARES"
                from "TB_OBJ"
                   where
                        "ST_TYPE" = 'Post'
                        and DATE_PART('day', now()::date - "DT_CRE") >= %s
                        and "N_SHARES" > 0
                        and not "F_SHARES_DOWNLOADED"
                        and not "F_NON_EXIST"
                limit 100;
            """, (EcAppParam.gcm_shares_depth,))

            # loop through the posts obtained from the DB
            for l_record in l_cursor:
                l_post_list.append(l_record)

            self.m_logger.info('Number of posts to process: {0}'.format(len(l_post_list)))
        except Exception as e:
            self.m_logger.critical(
                'Shares download Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query), extra={'m_errno': 1018})
            raise BulkDownloaderException('Shares download Unknown Exception: {0}'.format(repr(e)))
        finally:
            # close DB access handles when finished
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        for l_post_id, l_page_id, l_share_count in l_post_list:
            # thread abort
            if not self.m_threads_proceed:
                break

            self.m_logger.info('===== Downloading {0} shares from post ID: {1}'.format(l_share_count, l_post_id))

            # get post data
            l_field_list = \
                'id,application,caption,created_time,description,from,icon,link,message,message_tags,name,' + \
                'object_id,parent_id,permalink_url,picture,full_picture,place,properties,shares,' + \
                'source,status_type,story,to,type,updated_time,with_tags'

            # FB API request to get the data for this post
            l_request = 'https://graph.facebook.com/{0}/{1}/sharedposts?limit={2}&fields={3}'.format(
                EcAppParam.gcm_api_version,
                l_post_id,
                EcAppParam.gcm_limit,
                l_field_list)

            # perform request
            try:
                l_response = self.perform_request(l_request)
            except BulkDownloaderException as e:
                if str(e) == 'NON_EXIST':
                    self.m_logger.warning('Post with ID {0} said by FB not to exist (shares)'.format(l_post_id),
                                          extra={'m_errno': 1019})
                    self.set_non_exist(l_post_id)
                    continue
                else:
                    raise
            # decode request's JSON response
            l_response_data = json.loads(l_response)
            self.m_logger.info('Count of posts received: {0}'.format(len(l_response_data['data'])))

            l_post_count = 0
            l_finished = False
            # loop through all returned posts
            while not l_finished:
                # 2 nested loops because of FB-specific paging mechanism (see below)
                for l_post in l_response_data['data']:
                    # increment the total number of posts retrieved
                    self.m_postRetrieved += 1

                    self.m_logger.info(
                        '   =====[ {0}/{1} ]================POST (SHARED)========================='.format(
                            l_post_count, l_share_count))

                    l_already = self.process_one_post(l_page_id, l_post_id, l_post, p_shared_post=True)
                    l_post_count += 1

                    # post already in the DB
                    if l_already:
                        self.m_logger.info('Cross share with another page ?')

                # End of loop: for l_post in l_responseData['data']: (looping through current batch of posts)

                # FB API paging mechanism
                if 'paging' in l_response_data.keys() and 'next' in l_response_data['paging'].keys():
                    self.m_logger.info('   *** Getting next post block ...')
                    l_request = l_response_data['paging']['next']
                    l_response = self.perform_request(l_request)

                    l_response_data = json.loads(l_response)
                else:
                    # if no more 'pages' (batches of posts) to load, break the loop
                    break

            # end while not l_finished: (outer loop to handle paging)
            self.set_shares_downloaded(l_post_id)

        self.m_logger.info('End shares_download()')

    def repeat_posts_update(self):
        """
        Calls :any:`BulkDownloader.update_posts()` repeatedly, with a 1 second delay between calls. Meant to be
        the posts update thread initiated in :any:`BulkDownloader.start_threads()`. The loop stops (and the thread
        terminates) when :any:`m_threads_proceed` is set to `False`

        :return: Nothing
        """
        self.m_logger.info('Start repeat_posts_update()')
        while True:
            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.repeat_posts_update()')
            l_cursor = l_conn.cursor()

            # counting posts to be updated
            l_count = 0
            try:
                l_cursor.execute("""
                    select count(1) as "COUNT"
                    from "TB_OBJ"
                    where
                        "ST_TYPE" = 'Post'
                        and DATE_PART('day', now()::date - "DT_CRE") <= %s
                        and (
                            "DT_LAST_UPDATE" is null
                            or DATE_PART('day', now()::date - "DT_LAST_UPDATE") >= 2
                        )
                        and not "F_NON_EXIST"
                """, (EcAppParam.gcm_days_depth,))

                for l_count, in l_cursor:
                    pass

                self.m_logger.info('PRGMTR-U Posts to be updated: {0}'.format(l_count))
            except Exception as e:
                self.m_logger.critical(
                    'repeat_posts_update() Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                    extra={'m_errno': 1020})
                raise BulkDownloaderException('repeat_posts_update() Unknown Exception: {0}'.format(repr(e)))
            finally:
                # close DB access handles when finished
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            if l_count > 0 and LocalParam.gcm_do_update:
                try:
                    self.update_posts()
                except BulkDownloaderException:
                    self.m_logger.info('Caught a BulkDownloaderException thrown from below - nothing to do')
                except Exception as e:
                    self.m_logger.error('Caught exception in repeat_posts_update() loop: ' + repr(e),
                                        extra={'m_errno': 1021})
                time.sleep(1)
            else:
                # no posts to update --> wait 5 minutes
                time.sleep(5 * 60)

    def update_posts(self):
        """
        Update existing posts (100 at a time): Text modifications, new comments, likes count.

        The posts selected for update are those not older than :any:`gcm_days_depth` days and not
        already updated in the last 48 hours or those which were just created today (`DT_LAST_UPDATE` null).
        Among these, comments will be downloaded only for those which were **not** created today
        (`DT_LAST_UPDATE` not null as defined in the calculated column `COMMENT_FLAG`)

        :return: Nothing
        """
        self.m_logger.info('Start update_posts()')

        # get DB connection and cursor
        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.update_posts()')
        l_cursor = l_conn.cursor()

        l_post_list = []
        try:
            l_cursor.execute("""
                select
                    "ID"
                    , "ID_PAGE"
                    , case when "DT_LAST_UPDATE" is null then '' else 'X' end "COMMENT_FLAG"
                from "TB_OBJ"
                where
                    "ST_TYPE" = 'Post'
                    and DATE_PART('day', now()::date - "DT_CRE") <= %s
                    and (
                        "DT_LAST_UPDATE" is null
                        or DATE_PART('day', now()::date - "DT_LAST_UPDATE") >= 2
                    )
                    and not "F_NON_EXIST"
                limit 100;
            """, (EcAppParam.gcm_days_depth,))

            # loop through the posts obtained from the DB
            for l_record in l_cursor:
                l_post_list.append(l_record)

        except Exception as e:
            self.m_logger.critical('Post Update Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1022})
            raise BulkDownloaderException('Post Update Unknown Exception: {0}'.format(repr(e)))
        finally:
            # close DB access handles when finished
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        # loop through the posts obtained from the DB
        for l_post_id, l_page_id, l_comment_flag in l_post_list:
            # thread abort
            if not self.m_threads_proceed:
                break

            self.m_postRetrieved += 1
            # get post data
            l_field_list = 'id,created_time,from,story,message,' + \
                           'caption,description,icon,link,name,object_id,picture,place,shares,source,type'

            # FB API request to get the data for this post
            l_request = 'https://graph.facebook.com/{0}/{1}?limit={2}&fields={3}'.format(
                EcAppParam.gcm_api_version,
                l_post_id,
                EcAppParam.gcm_limit,
                l_field_list)

            # perform request
            try:
                l_response = self.perform_request(l_request)
            except BulkDownloaderException as e:
                if str(e) == 'NON_EXIST':
                    self.m_logger.warning('Post with ID {0} said by FB not to exist (update)'.format(l_post_id),
                                          extra={'m_errno': 1023})
                    self.set_non_exist(l_post_id)
                    continue
                else:
                    raise
            # decode request's JSON response
            l_response_data = json.loads(l_response)

            self.m_logger.info('============= UPDATE ==============================================')
            self.m_logger.info('Post ID     : {0}'.format(l_post_id))
            if 'created_time' in l_response_data.keys():
                self.m_logger.info('Post date   : {0}'.format(l_response_data['created_time']))
                self.m_logger.info('Comm. dnl ? : {0}'.format(l_comment_flag))

            # basic post data
            l_name, l_name_short = BulkDownloader.get_optional_field(l_response_data, 'name')
            l_caption, l_caption_short = BulkDownloader.get_optional_field(l_response_data, 'caption')
            l_description, l_description_sh = BulkDownloader.get_optional_field(l_response_data, 'description')
            l_story, l_story_short = BulkDownloader.get_optional_field(l_response_data, 'story')
            l_message, l_message_short = BulkDownloader.get_optional_field(l_response_data, 'message')

            # shares count for the post
            l_shares = int(l_response_data['shares']['count']) if 'shares' in l_response_data.keys() else 0

            # debug display
            self.m_logger.info('name        : {0}'.format(l_name_short))
            if EcAppParam.gcm_verboseModeOn:
                self.m_logger.info('caption     : {0}'.format(l_caption_short))
                self.m_logger.info('description : {0}'.format(l_description_sh))
                self.m_logger.info('story       : {0}'.format(l_story_short))
                self.m_logger.info('message     : {0}'.format(l_message_short))
                self.m_logger.info('shares      : {0}'.format(l_shares))

            # FB API request to get post likes count (actually will get the first page of the full likes list
            # because there is no way to get the count by itself)
            l_request = \
                'https://graph.facebook.com/{0}/{1}/likes?limit={2}&summary=true'.format(
                    EcAppParam.gcm_api_version,
                    l_post_id,
                    25,
                    
                    l_field_list)

            # performs the request
            l_response = self.perform_request(l_request)
            # decodes the request's JSON response
            l_response_data = json.loads(l_response)

            # get the count if present, otherwise 0
            l_like_count = 0
            if 'summary' in l_response_data.keys():
                l_like_count = int(l_response_data['summary']['total_count'])

            if EcAppParam.gcm_verboseModeOn:
                self.m_logger.info('likes       : {0}'.format(l_like_count))

            l_update_ok = self.update_object(
                l_post_id, l_shares, l_like_count, l_name, l_caption, l_description, l_story, l_message)

            # get comments if l_comment_flag is set and the post update was successful
            if l_update_ok and l_comment_flag == 'X':
                self.get_comments(l_post_id, l_post_id, l_page_id, 0)
        # end loop: for l_post_id, l_page_id, l_comment_flag in l_post_list:

        self.m_logger.info('End update_posts()')

    def repeat_get_likes_details(self, p_lock):
        """
        Calls :any:`BulkDownloader.get_likes_detail()` repeatedly, with a 1 second delay between calls. Meant to be
        the likes detail download thread initiated in :any:`BulkDownloader.start_threads()`.
        The loop stops (and the thread terminates) when :any:`m_threads_proceed` is set to `False`

        This function is executed in a separate process and must therefore re-initialize the connection pool
        (:any:`new_process_init()`)

        :param p_lock: Lock protecting `F_LOCK` in `TB_OBJ`
        :return: Nothing
        """
        # to block process start until released
        p_lock.acquire()
        # Env set-up after process start (logger & mailer)
        self.new_process_init()
        self.m_logger.info('repeat_get_likes_details() lock released')
        p_lock.release()
        time.sleep(.01)

        while True:
            # get the total count of objects that remains to be processed

            # get DB connection and cursor
            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.get_likes_detail() Read')
            l_cursor = l_conn.cursor()

            l_count = 0
            try:
                l_cursor.execute("""
                    SELECT
                        count(1)
                    FROM
                        "TB_OBJ"
                    WHERE
                        "ST_TYPE" != 'Page'
                        AND DATE_PART('day', now()::date - "DT_CRE") >= %s
                        AND NOT "F_LOCKED"
                        AND NOT "F_NON_EXIST"
                        AND "F_LIKE_DETAIL" is null
                """, (EcAppParam.gcm_likes_depth,))

                for l_count, in l_cursor:
                    pass

                self.m_logger.info('PRGMTR-L posts ready for likes download: {0}'.format(l_count))
            except Exception as e:
                l_msg = 'Likes detail download Unknown Exception (Read) : {0}/{1}'.format(repr(e), l_cursor.query)
                self.m_logger.critical(l_msg, extra={'m_errno': 1024})
                raise BulkDownloaderException(l_msg)
            finally:
                # release DB handles when finished
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            if l_count > 0 and LocalParam.gcm_do_likes:
                try:
                    self.get_likes_detail(p_lock)
                    time.sleep(1)
                except BulkDownloaderException:
                    self.m_logger.info('Caught a BulkDownloaderException thrown from below - nothing to do')
                except Exception as e:
                    self.m_logger.error('Exception caught in repeat_get_likes_details() loop' + repr(e),
                                        extra={'m_errno': 1025})
            else:
                # no likes details to download --> wait 5 minutes
                time.sleep(5 * 60)

    def get_likes_detail(self, p_lock):
        """
        Get the likes details of sufficiently old posts and comments (100 at a time). "Sufficiently old" means,
        older than :any:`gcm_likes_depth` days.

        :param p_lock: Lock protecting `F_LOCK` in `TB_OBJ`
        :return: Nothing
        """
        self.m_logger.info('Start get_likes_detail()')

        # CRITICAL SECTION ENTRY --------------------------------------------------------------------------------------
        self.m_logger.info('CS Entry -------B-------')
        p_lock.acquire()
        self.m_logger.info('CS Entry -------A-------')

        l_obj_list = []
        # try block to ensure that the CS lock will be released
        try:
            # get DB connection and cursor
            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.get_likes_detail() Read')
            l_cursor = l_conn.cursor()

            # get the list of objects that will be processed in this run (500 or less)
            # l_total_count = 0
            try:
                l_cursor.execute("""
                    SELECT
                        "ID", "ID_INTERNAL", "DT_CRE"
                    FROM
                        "TB_OBJ"
                    WHERE
                        "ST_TYPE" != 'Page'
                        AND DATE_PART('day', now()::date - "DT_CRE") >= %s
                        AND NOT "F_LOCKED"
                        AND NOT "F_NON_EXIST"
                        AND "F_LIKE_DETAIL" is null
                    LIMIT 500
                """, (EcAppParam.gcm_likes_depth,))

                for l_record in l_cursor:
                    l_obj_list.append(l_record)

                # get the total count of objects that will be processed in this run (500 or less)
                l_total_count = len(l_obj_list)
            except Exception as e:
                l_msg = 'Likes detail download Unknown Exception (Read) : {0}/{1}'.format(repr(e), l_cursor.query)
                self.m_logger.critical(l_msg, extra={'m_errno': 1026})
                raise BulkDownloaderException(l_msg)
            finally:
                # release DB handles when finished
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            # get DB connection and cursor
            l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.get_likes_detail() Write')
            l_cursor_write = l_conn_write.cursor()

            def lock_unlock_batch(p_lock_value=True):
                try:
                    l_cursor_write.execute("""
                        update
                            "TB_OBJ"
                        set
                            "F_LOCKED" = {0}
                        where
                            "ID_INTERNAL" in ({1})
                    """.format(
                        'true' if p_lock_value else 'false',
                        ','.join([str(l_internal_id0) for _, l_internal_id0, _ in l_obj_list])))

                    l_conn_write.commit()
                except Exception as e1:
                    l_conn_write.rollback()
                    l_msg0 = 'Likes detail download Unknown Exception (Write) : {0}/{1}'.format(
                        repr(e1), l_cursor_write.query)
                    self.m_logger.critical(l_msg0, extra={'m_errno': 1027})
                    raise BulkDownloaderException(l_msg0)
                finally:
                    # release DB handles when finished
                    l_cursor_write.close()
                    EcConnectionPool.get_global_pool().putconn(l_conn_write)

            lock_unlock_batch(p_lock_value=True)
        except Exception as e:
            self.m_logger.info('Catching exception to ensure lock release :' + repr(e))
            raise
        finally:
            # CRITICAL SECTION EXIT -----------------------------------------------------------------------------------
            self.m_logger.info('CS Exit -------B-------')
            p_lock.release()
            time.sleep(.01)
            self.m_logger.info('CS Exit -------A-------')

        # all non page objects older than gcm_likes_depth days and not already processed
        l_obj_count = 0

        # loop through the list of objects obtained from the DB
        for l_id, l_internal_id, l_dt_msg in l_obj_list:
            # l_id: FB ID
            # l_internal_id: DB ID

            self.m_logger.info('{0}/{1} {2} ----->'.format(l_obj_count, l_total_count, l_id))

            # FB API request to get the list of likes for the given object
            l_request = 'https://graph.facebook.com/{0}/{1}/likes?limit={2}'.format(
                EcAppParam.gcm_api_version,
                l_id,
                EcAppParam.gcm_limit)

            # perform request
            try:
                l_response = self.perform_request(l_request)
            except BulkDownloaderException as e:
                if str(e) == 'NON_EXIST':
                    self.m_logger.warning(
                        'Post/comment with ID {0} said by FB not to exist (likes detail)'.format(l_id),
                        extra={'m_errno': 1077})
                    self.set_non_exist(l_id)
                    continue
                else:
                    lock_unlock_batch(p_lock_value=False)
                    raise
            # decode request's JSON response
            l_response_data = json.loads(l_response)

            l_like_count = 0
            # loop through all likes returned by the request
            while True:
                # double loop to handle FB API paging mechanism
                for l_liker in l_response_data['data']:
                    # ID of the liker, if any (otherwise skip)
                    try:
                        l_liker_id = l_liker['id']
                    except KeyError:
                        self.m_logger.warning('No Id found in Liker: {0}'.format(l_liker), extra={'m_errno': 1083})
                        continue

                    # Name of the liker, if any (otherwise skip)
                    try:
                        l_liker_name = l_liker['name']
                    except KeyError:
                        self.m_logger.warning('No name found in Liker: {0}'.format(l_liker), extra={'m_errno': 1084})
                        continue

                    # Parent object date in string form for database insertion
                    l_dt_msg_str = l_dt_msg.strftime('%Y-%m-%dT%H:%M:%S+000')
                    # store the liker in the DB
                    self.store_user(l_liker_id, l_liker_name, l_dt_msg_str, '')

                    # get the DB ID of the liker
                    l_liker_internal_id = self.get_user_internal_id(l_liker_id)

                    # create a like link in the DB btw the liker and the object
                    self.create_like_link(l_liker_internal_id, l_internal_id, l_dt_msg_str)

                    # debug display
                    if EcAppParam.gcm_verboseModeOn:
                        self.m_logger.debug('   {0}/{1} [{2} | {3}] {4}'.format(
                            l_obj_count, l_total_count, l_liker_id, l_liker_internal_id, l_liker_name))

                    l_like_count += 1
                # end of loop: for l_liker in l_response_data['data']:

                # FB API paging mechanics
                if 'paging' in l_response_data.keys() and 'next' in l_response_data['paging'].keys():
                    self.m_logger.info('   *** {0}/{1} Getting next likes block ...'.format(
                        l_obj_count, l_total_count))
                    l_request = l_response_data['paging']['next']
                    l_response = self.perform_request(l_request)

                    l_response_data = json.loads(l_response)
                else:
                    break
            # end of loop: while True:

            # mark the object to indicated likes download complete
            self.set_like_flag(l_id)

            self.m_logger.info('   {0}/{1} --> {2} Likes:'.format(l_obj_count, l_total_count, l_like_count))
            l_obj_count += 1
        # end loop: for l_id, l_internal_id, l_dt_msg in l_cursor:

        self.m_logger.info('End get_likes_detail()')

    def repeat_fetch_images(self):
        """
        Calls :any:`BulkDownloader.fetch_images()` repeatedly, with a 1 second delay between calls. Meant to be
        the image fetching thread initiated in :any:`BulkDownloader.m_threads_proceed()`.
        The loop stops (and the thread terminates) when :any:`m_threads_proceed` is set to `False`
        
        :return: Nothing 
        """
        self.m_logger.info('Start repeat_fetch_images()')
        while True:
            # get DB connection and cursor
            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.fetch_images()')
            l_cursor = l_conn.cursor()

            l_count = 0
            try:
                l_cursor.execute("""
                    select count(1) as "COUNT" 
                    from "TB_MEDIA"
                    where not "F_LOADED" and not "F_ERROR";
                """)

                for l_count, in l_cursor:
                    pass

                self.m_logger.info('PRGMTR-I Images available for download: {0}'.format(l_count))
            except Exception as e:
                l_msg = 'Error selecting from TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor.query)
                self.m_logger.critical(l_msg, extra={'m_errno': 1028})
                raise BulkDownloaderException(l_msg)
            finally:
                # releases outer DB cursor and connection
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            if l_count > 0 and LocalParam.gcm_do_img:
                try:
                    self.fetch_images()
                except BulkDownloaderException:
                    self.m_logger.info('Caught a BulkDownloaderException thrown from below - nothing to do')
                except Exception as e:
                    self.m_logger.error('Caught error in repeat_fetch_images() loop: ' + repr(e),
                                        extra={'m_errno': 1029})

                time.sleep(1)
            else:
                # if there were no images to process --> wait 5 minutes
                time.sleep(5 * 60)

    def get_image(self, p_src, p_internal):
        """

        :param p_src:
        :param p_internal:
        :return:
        """
        self.m_logger.info('get_image() start: ' + p_src)

        l_fmt_list = ['png', 'jpg', 'jpeg', 'gif', 'svg']
        l_fmt_string = '|'.join(l_fmt_list) + '|' + \
                       '|'.join([f.upper() for f in l_fmt_list])

        # try to isolate the image name and format from the source string
        # l_match = re.search(r'/([^/]+_[no]\.(png|jpg|jpeg|gif|svg|PNG|JPG|JPEG|GIF|SVG))', l_src)
        l_match = re.search(r'/([^/]+_[no]\.({0}))'.format(l_fmt_string), p_src)
        if l_match:
            l_img = l_match.group(1)
            l_fmt = l_match.group(2)
        else:
            # l_match = re.search(r'url=([^&]+\.(png|jpg|jpeg|gif|svg|PNG|JPG|JPEG|GIF|SVG))[&%]', l_src)
            l_match = re.search(r'url=([^&]+\.({0}))[&%]'.format(l_fmt_string), p_src)
            if l_match:
                l_img = (urllib.parse.unquote(l_match.group(1))).split('/')[-1]
                l_fmt = l_match.group(2)
            else:
                self.m_logger.warning('Image not found in:' + p_src, extra={'m_errno': 1080})
                l_img = '__ConspiSuck_IMG__{0}.png'.format(p_internal)
                l_fmt = 'png'

        # final adjustments to the format data
        l_fmt = l_fmt.lower()
        l_fmt = 'jpeg' if l_fmt == 'jpg' else l_fmt

        if len(l_img) > 200:
            l_img = l_img[-200:]

        # make 10 attempts (max) at downloading the image
        l_attempts = 0
        l_error = False
        l_image_txt = ''
        while True:
            l_attempts += 1
            if l_attempts > 10:
                if self.m_background_task.internet_check():
                    l_msg = 'Cannot download image [{0}] Too many failed attempts'.format(l_img)
                    self.m_logger.warning(l_msg, extra={'m_errno': 1085})
                    l_error = True
                    # l_image_txt will finally contain a succession of error messages + the one below
                    # separated by '|'
                    l_image_txt += l_msg
                    break
                    # raise BulkDownloaderException(l_msg)
                else:
                    # internet down situations do not count as attempts
                    self.m_logger.info('Internet Down. Waiting ...')
                    time.sleep(5 * 60)
                    l_attempts = 0

            l_step = 0
            try:
                # download attempt
                l_img_content = Image.open(io.BytesIO(urllib.request.urlopen(p_src, timeout=20).read()))
                l_step = 10
                if l_img_content.mode != 'RGB':
                    l_img_content = l_img_content.convert('RGB')

                # determine format from image and compare with previous format guess
                l_step = 20
                if len(l_fmt) == 0:
                    l_fmt = 'png'
                self.m_logger.info('--> [{0}] {1}'.format(l_fmt, l_img))

                # save image locally
                if not LocalParam.gcm_prodEnv:
                    l_img_content.save(os.path.join('./images_fb', l_img))
                    self.m_logger.debug('Saved')
                    l_step = 30

                # converts image to a base64 string
                l_output_buffer = io.BytesIO()
                l_step = 40
                l_img_content.save(l_output_buffer, format=l_fmt)
                l_step = 50
                l_buffer_value = l_output_buffer.getvalue()
                l_step = 60
                l_image_txt = base64.b64encode(l_buffer_value).decode()
                l_step = 70
                self.m_logger.info('Base64: [{0}] {1}'.format(len(l_image_txt), l_image_txt[:100]))
                break
            except urllib.error.URLError as e:
                # if a HTTP error 404 occurs --> certainty of error
                if re.search(r'HTTPError 404', repr(e)):
                    l_msg = '[{0}/{1}] Trapped urllib.error.URLError/HTTPError 404: '.format(l_step, l_fmt) + repr(e)
                    self.m_logger.warning(l_msg, extra={'m_errno': 1030})
                    l_image_txt = l_msg
                    l_error = True
                    break
                else:
                    # other type of error --> worth trying again
                    l_msg = '[{0}/{1}] Trapped urllib.error.URLError: '.format(l_step, l_fmt) + repr(e)
                    self.m_logger.warning(l_msg, extra={'m_errno': 1031})
                    l_image_txt += l_msg + '|'
                    continue
            except socket.timeout as e:
                # if download timed out --> worth trying again
                l_msg = '[{0}/{1}] Trapped socket.timeout: '.format(l_step, l_fmt) + repr(e)
                self.m_logger.warning(l_msg, extra={'m_errno': 1032})
                l_image_txt += l_msg + '|'
                continue
            except TypeError as e:
                # ???
                l_msg = '[{0}/{1}] Trapped TypeError (probably pillow UserWarning: '.format(l_step, l_fmt) +\
                        'Could not allocate palette entry for transparency): ' + repr(e)
                self.m_logger.warning(l_msg, extra={'m_errno': 1033})
                l_image_txt = l_msg
                l_error = True
                break
            except KeyError as e:
                # ???
                l_msg = '[{0}/{1}] Error downloading image: {2}'.format(l_step, l_fmt, repr(e))
                self.m_logger.warning(l_msg, extra={'m_errno': 1034})
                l_image_txt = l_msg
                l_error = True
                break
            except Exception as e:
                l_msg = '[{0}/{1}] Unknown error while downloading image: {2}'.format(l_step, l_fmt, repr(e))
                self.m_logger.warning(l_msg, extra={'m_errno': 1035})
                l_image_txt = l_msg
                l_error = True
                break

        self.m_logger.info('get_image() end: ' + p_src)
        return l_fmt, l_image_txt, l_error

    def fetch_images(self):
        """
        Image fetching. Take a block of 100 records in `TB_MEDIA` and attempts to download the pictures they reference
        (if any). Flags are positioned in `TB_MEDIA` if the download was completed successfully or if an error
        occurred.

        If an error was encountered during download the error message(s) end(s) up where the Base64 representation
        of the image would normally go.

        :return: Nothing 
        """
        self.m_logger.info('Start fetch_images()')

        # get DB connection and cursor
        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.fetch_images()')
        l_cursor = l_conn.cursor()

        # load 100 `TB_MEDIA` which have an image link but have not been loaded or had an error while loading
        # previously.
        l_image_list = []
        try:
            l_cursor.execute("""
                select "TX_MEDIA_SRC", "N_WIDTH", "N_HEIGHT", "ID_MEDIA_INTERNAL", "TX_PICTURE", "TX_FULL_PICTURE" 
                from "TB_MEDIA"
                where not "F_LOADED" and not "F_ERROR"
                limit 100;
            """)

            for l_record in l_cursor:
                l_image_list.append(l_record)

        except Exception as e:
            l_msg = 'Error selecting from TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor.query)
            self.m_logger.critical(l_msg, extra={'m_errno': 1036})
            raise BulkDownloaderException(l_msg)
        finally:
            # releases outer DB cursor and connection
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        l_img_count = 0
        # loop through the batch of records
        for l_src, l_width, l_height, l_internal, l_picture, l_full_picture in l_image_list:
            # thread abort
            if not self.m_threads_proceed:
                break

            self.m_logger.info('media src : {0}'.format(l_src))
            self.m_logger.info('picture   : {0}'.format(l_picture))
            self.m_logger.info('full pic. : {0}'.format(l_full_picture))

            l_error, l_error_pic, l_error_fp = False, False, False
            l_fmt, l_image_txt = '', ''
            # l_fmt_pic, l_image_txt_pic = '', ''
            l_fmt_fp, l_image_txt_fp = '', ''
            if l_src is not None and len(l_src) > 0:
                l_fmt, l_image_txt, l_error = self.get_image(l_src, l_internal)
            # if l_picture is not None and len(l_picture) > 0:
            #     l_fmt_pic, l_image_txt_pic, l_error_pic = self.get_image(l_picture, l_internal)
            if l_full_picture is not None and len(l_full_picture) > 0:
                l_fmt_fp, l_image_txt_fp, l_error_fp = self.get_image(l_full_picture, l_internal)

            # get new DB connection and cursor to perform the write operation
            l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.fetch_images()')
            l_cursor_write = l_conn_write.cursor()

            # stores the results (which may be an error) into `TB_MEDIA`
            try:
                l_cursor_write.execute("""
                    update "TB_MEDIA"
                    set 
                        "F_LOADED" = true, 
                        "TX_BASE64" = %s, 
                        "F_ERROR" = %s,
                        "TX_BASE64_FP" = %s,
                        "ST_FORMAT" = %s, 
                        "ST_FORMAT_FP" = %s 
                    where "ID_MEDIA_INTERNAL" = %s;
                """, (l_image_txt,
                      l_error or l_error_fp,
                      l_image_txt_fp,
                      l_fmt,
                      l_fmt_fp,
                      l_internal))
                l_conn_write.commit()
            except Exception as e:
                l_conn_write.rollback()
                l_msg = 'Error updating TB_MEDIA: {0}'.format(repr(e))
                self.m_logger.critical(l_msg, extra={'m_errno': 1037})
                raise BulkDownloaderException(l_msg)
            finally:
                # releases write operation DB connection and cursor
                l_cursor_write.close()
                EcConnectionPool.get_global_pool().putconn(l_conn_write)

            self.m_logger.info('Fetched image for internal ID: {0}'.format(l_internal))

            l_img_count += 1
        # end loop: for l_src, l_width, l_height, l_internal in l_image_list:

        self.m_logger.info('End fetch_images()')
        return l_img_count

    def repeat_ocr_image(self, p_lock):
        """
        Calls :any:`BulkDownloader.ocr_images()` repeatedly, with a 30 second delay between calls. Meant to be 
        the image OCR thread initiated in :any:`BulkDownloader.bulk_download()`.

        This function is executed in a separate process and must therefore re-initialize the connection pool
        (:any:`new_process_init()`)

        :param p_lock: Lock protecting `F_LOCK` in `TB_MEDIA`
        :return: Nothing
        """
        print('repeat_ocr_image() start (before acquiring lock) : ' + multiprocessing.current_process().name)
        # to block process start until released
        p_lock.acquire()
        # set-up logging environment, etc
        self.new_process_init()
        self.m_logger.info('repeat_ocr_image() lock released')
        p_lock.release()
        time.sleep(.01)

        while True:
            self.m_logger.info('top of repeat_ocr_image() loop')
            l_count = 0

            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.fetch_images()')
            l_cursor = l_conn.cursor()
            try:
                l_cursor.execute("""
                    select count(1) as "COUNT"
                    from 
                        "TB_MEDIA"
                    where 
                        "F_LOADED" 
                        and not "F_ERROR" 
                        and not "F_OCR" 
                        and "F_LOCK" is NULL
                        and not "F_FROM_PARENT";
                """)

                for l_count, in l_cursor:
                    pass

                self.m_logger.info('PRGMTR-O Images ready for OCR: {0}'.format(l_count))
            except Exception as e:
                l_msg = 'Error selecting from TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor.query)
                self.m_logger.critical(l_msg, extra={'m_errno': 1038})
                raise BulkDownloaderException(l_msg)
            finally:
                # DB handles released after use
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            if l_count > 0 and LocalParam.gcm_do_ocr:
                try:
                    self.ocr_images(p_lock)
                except BulkDownloaderException:
                    self.m_logger.info('Caught a BulkDownloaderException thrown from below - nothing to do')
                except Exception as e:
                    self.m_logger.error('Caught Exception in repeat_ocr_image() loop :' + repr(e),
                                        extra={'m_errno': 1078})

                time.sleep(1)
            else:
                # no OCR --> wait 5 minutes
                time.sleep(5 * 60)

    def resume_ocr(self, p_process_number):
        """

        :param p_process_number:
        :return:
        """
        l_id_file_name = os.path.join(LocalParam.gcm_appRoot, 'O{0}_internal_ID.txt'.format(p_process_number))
        l_internal_id = None
        try:
            with open(l_id_file_name, 'r') as f:
                l_internal_id = int(f.read())
        except Exception as e:
            l_msg = 'Could not open [{0}] for reading: {1}'.format(l_id_file_name, repr(e))
            self.m_logger.critical(l_msg, extra={'m_errno': 1039})

        if l_internal_id is not None:
            l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.resume_ocr()')
            l_cursor_write = l_conn_write.cursor()
            try:
                l_cursor_write.execute("""
                    update "TB_MEDIA"
                    set 
                        "F_LOCK" = NULL
                        , "F_ERROR" = true
                        , "F_OCR" = true
                    where "ID_MEDIA_INTERNAL" = %s;
                """, (l_internal_id, ))
                l_conn_write.commit()

                l_cursor_write.execute("""
                    update "TB_MEDIA"
                    set 
                        "F_LOCK" = NULL
                    where "F_LOCK" = %s;
                """, ('{0}'.format(p_process_number), ))
                l_conn_write.commit()

                self.m_logger.info('Marked TB_MEDIA #{0} as OCR error'.format(l_internal_id))
            except Exception as e:
                l_conn_write.rollback()

                l_msg = 'Error writing to TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor_write.query)
                self.m_logger.critical(l_msg, extra={'m_errno': 1040})
                raise BulkDownloaderException(l_msg)
            finally:
                # DB handles released after use
                l_cursor_write.close()
                EcConnectionPool.get_global_pool().putconn(l_conn_write)

    def ocr_images(self, p_lock):
        """
        Takes a block of (normally 500) `TB_MEDIA` records with downloaded images and attempts OCR. Stores the results
        in appropriate fields in `TB_MEDIA`.

        The OCR process generates several versions of the image through various filters (contrast, brightness,
        cutoff, etc.). The final OCR text is taken from the 'best' version as determined by various heuristics.

        The `joh` Tesserocr training data is taken from here: `https://github.com/johnlinp/meme-ocr`_.

        .. https://github.com/johnlinp/meme-ocr: http://example.com/

        :param p_lock: Lock protecting `F_LOCK` in `TB_MEDIA`
        :return: Nothing
        """
        self.m_logger.info('Start ocr_images()')

        # SQL Offset (why ?)
        # l_offset = 0
        # image batch max size
        l_max_img_count = 500

        # threshold for cutoff filters
        l_threshold = 180
        # If = 2, brightness and contrast processing will be permuted, if = 1, they will not
        l_order_range = 2
        # list of possible values for brightness and contrast enhancement
        v = [.75, 1.5]
        # v = [.75, 1.25, 1.5, 2.0]
        # all images will be enlarged by this factor
        l_enlarge_factor = 2

        # parameters used to determine the bracket in the list of results to be selected at the end
        # width of the bracket
        l_width = 6
        # number of elements at the end to discard
        l_clip = 2

        # the temp directory for the ocr operations
        l_img_path = os.path.join(LocalParam.gcm_appRoot, 'images_ocr')
        self.m_logger.info('OCR images path : ' + l_img_path)
        # controls the display of extra debug messages
        l_debug_messages = False

        # CRITICAL SECTION ENTRY --------------------------------------------------------------------------------------
        self.m_logger.info('CS Entry ----B----')
        p_lock.acquire()
        self.m_logger.info('CS Entry ----A----')

        l_media_list = []
        # try block to ensure that CS lock is released
        try:
            # get 500 images (if available)
            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.fetch_images()')
            l_cursor = l_conn.cursor()
            try:
                l_cursor.execute("""
                    select 
                        "M"."ID_MEDIA_INTERNAL"
                        ,"M"."TX_MEDIA_SRC"
                        ,"M"."TX_FULL_PICTURE" 
                        ,"M"."TX_BASE64" 
                        ,"M"."TX_BASE64_FP"
                        ,"N"."ATT_COUNT" 
                    from 
                        "TB_MEDIA" as "M"
                        join (
                            select "ID_OWNER", count(1) as "ATT_COUNT" 
                            from "TB_MEDIA" 
                            where not "F_FROM_PARENT"
                            group by "ID_OWNER"
                        ) as "N" on "M"."ID_OWNER" = "N"."ID_OWNER"
                    where 
                        "M"."F_LOADED" 
                        and not "M"."F_ERROR" 
                        and not "M"."F_OCR" 
                        and "M"."F_LOCK" is NULL
                        and not "M"."F_FROM_PARENT"
                    limit %s;
                """, (l_max_img_count, ))

                for l_record in l_cursor:
                    l_media_list.append(l_record)

            except Exception as e:
                l_msg = 'Error selecting from TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor.query)
                self.m_logger.critical(l_msg, extra={'m_errno': 1041})
                raise BulkDownloaderException(l_msg)
            finally:
                # DB handles released after use
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            if len(l_media_list) > 0:
                l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.fetch_images()')
                l_cursor_write = l_conn_write.cursor()
                try:
                    l_cursor_write.execute("""
                        update "TB_MEDIA"
                        set "F_LOCK" = %s
                        where "ID_MEDIA_INTERNAL" in ({0});
                    """.format(','.join([str(l_internal) for l_internal, _, _, _, _, _ in l_media_list])),
                                           (multiprocessing.current_process().name[1],))

                    l_conn_write.commit()
                    self.m_logger.info('LCKOCR Marked {0} images as locked for OCR'.format(len(l_media_list)))
                    # with open('tmp.sql', 'w') as f:
                    #     f.write(l_cursor_write.query.decode('utf-8'))
                except Exception as e:
                    l_conn_write.rollback()

                    l_msg = 'Error writing to TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor_write.query)
                    self.m_logger.critical(l_msg, extra={'m_errno': 1042})
                    raise BulkDownloaderException(l_msg)
                finally:
                    # DB handles released after use
                    l_cursor_write.close()
                    EcConnectionPool.get_global_pool().putconn(l_conn_write)
            # end: if len(l_media_list) > 0:
        except Exception as e:
            self.m_logger.info('Catching exception to ensure lock release: ' + repr(e))
            raise
        finally:
            # CRITICAL SECTION EXIT -----------------------------------------------------------------------------------
            self.m_logger.info('CS Exit ----B----')
            p_lock.release()
            time.sleep(.01)
            self.m_logger.info('CS Exit ----A----')

        # image index within the batch
        l_img_count = 0
        # score by suffix: number of times each suffix was in the selection bracket
        l_suf_score = dict()
        # loop through the 100 images
        for l_internal, l_media_src, l_full_picture, l_base64, l_base64_fp, l_att_count in l_media_list:
            # thread abort
            if not self.m_threads_proceed:
                break

            # store image internal ID to allow resumption in case of process death
            l_id_file_name = os.path.join(
                LocalParam.gcm_appRoot,
                '{0}_internal_ID.txt'.format(multiprocessing.current_process().name))
            with open(l_id_file_name, 'w') as f:
                f.write('{0}'.format(l_internal))

            self.m_logger.info('len(l_base64/l_base64_fp): {0}/{1}'.format(
                len(l_base64) if l_base64 is not None else 'None',
                len(l_base64_fp) if l_base64_fp is not None else 'None'
            ))
            if l_debug_messages:
                print('+++++++++++[{0}]++++++++++++++++++++++++++++++++++++++++++++++++++++++'.format(l_img_count))
            else:
                os.system('rm -f {0}/{1}_img*.png'.format(l_img_path, multiprocessing.current_process().name))

            l_file_list = []

            # special case: there are no images ---> mark record as OCRed
            if (l_base64 is None or len(l_base64) == 0) and (l_base64 is None or len(l_base64) == 0):
                self.mark_as_ocred(l_internal)
                continue

            # internal function handling th task required when creating a new image version
            def add_image(p_image, p_suffix):
                l_file = os.path.join(l_img_path, '{0}_img{1:03}_{2}.png'.format(
                    multiprocessing.current_process().name,
                    l_img_count, p_suffix))
                p_image.save(l_file)
                l_file_list.append(l_file)
                return p_image

            # determine what image to take
            if l_att_count < 2:
                if l_base64_fp is not None and len(l_base64_fp) > 0:
                    l_bas64_select = l_base64_fp
                elif l_base64 is not None and len(l_base64) > 0:
                    l_bas64_select = l_base64
                else:
                    # theoretically, this cannot happen
                    self.mark_as_ocred(l_internal)
                    continue
            else:
                l_bas64_select = None
                if l_media_src is not None:
                    l_match = re.search(r'w=(\d+)&h=(\d+)', l_media_src)
                    if l_match and l_match.group(1) == l_match.group(2) \
                            and l_base64_fp is not None and len(l_base64_fp) > 0:
                        l_bas64_select = l_base64_fp

                    # p130x130
                    l_match = re.search(r'/[ps](\d+)x(\d+)/', l_media_src)
                    if l_match and l_match.group(1) == l_match.group(2) \
                            and l_base64_fp is not None and len(l_base64_fp) > 0:
                        l_bas64_select = l_base64_fp

                if l_bas64_select is None:
                    if l_base64 is not None and len(l_base64) > 0:
                        l_bas64_select = l_base64
                    else:
                        self.mark_as_ocred(l_internal)
                        continue

            self.m_logger.info('len(l_bas64_select): {0}'.format(
                len(l_bas64_select) if l_bas64_select is not None else 'None'))

            # the raw image as it is in the DB
            l_raw = Image.open(io.BytesIO(base64.b64decode(l_bas64_select)))
            if l_raw.mode != 'RGB':
                l_raw = l_raw.convert('RGB')

            # base image: no other modification except enlargement
            l_base = add_image(
                l_raw.resize((int(l_raw.width*l_enlarge_factor), int(l_raw.height*l_enlarge_factor))), 'base')
            # black and white version
            l_bw = add_image(ImageEnhance.Color(l_base).enhance(0.0), 'bw')

            # creation of various image versions by applying brightness/contrast changes and cutoff filters
            # suffixes are added here to the file names
            for l_order in range(l_order_range):
                for c1 in range(len(v)):
                    for c2 in range(len(v)):
                        p1 = v[c1]
                        p2 = v[c2]

                        # do not do cases in which both brightness and contrast would be reduced
                        if p1 < 1.0 and p2 < 1.0:
                            continue

                        # brightness and contrast variants
                        if l_order == 1:
                            l_img_s1 = add_image(ImageEnhance.Contrast(l_bw).enhance(p1),
                                                 'a{0}{1}{2}'.format(l_order, c1, c2))
                            l_img_s2 = add_image(ImageEnhance.Brightness(l_img_s1).enhance(p2),
                                                 'b{0}{1}{2}'.format(l_order, c1, c2))
                        else:
                            l_img_s1 = add_image(ImageEnhance.Brightness(l_bw).enhance(p1),
                                                 'a{0}{1}{2}'.format(l_order, c1, c2))
                            l_img_s2 = add_image(ImageEnhance.Contrast(l_img_s1).enhance(p2),
                                                 'b{0}{1}{2}'.format(l_order, c1, c2))

                        # Median filter
                        l_img_s3 = add_image(l_img_s2.filter(ImageFilter.MedianFilter()),
                                             'd{0}{1}{2}'.format(l_order, c1, c2))

                        # cutoff filters
                        add_image(l_img_s2.convert('L').point(lambda x: 0 if x < l_threshold else 255, '1'),
                                  'thr{0}{1}{2}'.format(l_order, c1, c2))
                        add_image(l_img_s2.convert('L').point(lambda x: 255 if x < l_threshold else 0, '1'),
                                  'inv{0}{1}{2}'.format(l_order, c1, c2))

                        add_image(l_img_s3.convert('L').point(lambda x: 0 if x < l_threshold else 255, '1'),
                                  'dthr{0}{1}{2}'.format(l_order, c1, c2))
                        add_image(l_img_s3.convert('L').point(lambda x: 255 if x < l_threshold else 0, '1'),
                                  'dinv{0}{1}{2}'.format(l_order, c1, c2))
            # end for l_order in range(l_order_range):

            # internal function generating a list of results based on the different image variants
            def get_result_list(p_file_list, p_api, p_lang):
                if l_debug_messages:
                    print('get_resultList() p_lang: ' + p_lang)

                # list of tuples that will be returned to the caller
                l_result_list = []
                # best average Tesserocr quality ratio
                l_max_avg = 0
                # best percentage of words found in dictionary
                l_max_dict_ratio = 0

                # loop through all file versions
                for l_file in p_file_list:
                    if l_debug_messages:
                        print(l_file)
                    else:
                        self.m_logger.info('Variant File: ' + l_file)

                    # issue the file to the Tesserocr instance
                    self.m_logger.debug('before SetImageFile()')
                    p_api.SetImageFile(l_file)

                    # get the full OCR text
                    self.m_logger.debug('before GetUTF8Text()')
                    l_txt = re.sub(r'\s+', r' ', p_api.GetUTF8Text()).strip()
                    self.m_logger.debug('l_txt: ' + l_txt)
                    # if text longer than 10 characters --> analyze word by word
                    if len(l_txt) > 10:
                        # get the result iterator from the Tesserocr API instance
                        self.m_logger.debug('before GetIterator()')
                        ri = p_api.GetIterator()

                        # list of all words
                        l_raw_list = []
                        # list of words minus empties and non fully alphabetic or numeric
                        l_list = []
                        # list of words more than 3 characters long (only the 'found in dictionary' flag)
                        l_more_3 = []
                        while True:
                            try:
                                # the word
                                self.m_logger.debug('before ri.GetUTF8Text()')
                                l_word_ocr = re.sub('\s+', ' ', ri.GetUTF8Text(RIL.WORD)).strip()
                                # its confidence value
                                l_conf = ri.Confidence(RIL.WORD)
                                # whether it was found in the dictionary
                                l_dict = ri.WordIsFromDictionary()

                                # ligatures removal
                                l_list_char = []
                                for c in list(l_word_ocr):
                                    try:
                                        l_list_char.append(self.m_lig_dict[c])
                                    except KeyError:
                                        l_list_char.append(c)
                                l_word_ocr = ''.join(l_list_char)

                                # whether the word contains only alphabetic characters (and possibly a
                                # punctuation mark at the end))
                                l_full_alpha = re.match(
                                    r'(^[a-zA-Z]+[\'’][a-zA-Z]+|[a-zA-Z]+)[.,;:?!\'"]*$', l_word_ocr)
                                # l_full_alpha = False
                                # l_match = re.search(r'([a-zA-Z]+[\'’][a-zA-Z]+|[a-zA-Z]+)[\.,;:\?!]*', l_word)
                                # if l_match:
                                #     l_full_alpha = (l_match.group(0) == l_word)

                                # whether the word contains only numeric characters (and possibly a
                                # punctuation mark at the end))
                                l_full_num = re.match(r'(^[0-9]+[:,.][0-9]+|[0-9]+)[.,;:?!\'"]*$', l_word_ocr)
                                # l_full_num = False
                                # l_match = re.search(r'([0-9]+[:,\.][0-9]+|[0-9]+)[\.,;:\?!]*', l_word)
                                # if l_match:
                                #     l_full_num = (l_match.group(0) == l_word)

                                if l_debug_messages:
                                    print('{5} {0:.2f} {1} {2} {3} {4}'.format(
                                        l_conf,
                                        'D' if l_dict else ' ',
                                        'A' if l_full_alpha else ' ',
                                        'N' if l_full_num else ' ',
                                        l_word_ocr,
                                        p_lang))

                                # add a triplet to the raw list
                                l_raw_list.append((l_word_ocr, int(l_conf), l_dict))

                                # add a triplet to the filtered list if conditions apply
                                if (l_full_num or l_full_alpha) and len(l_word_ocr) > 0:
                                    l_list.append((l_word_ocr, int(l_conf), l_dict))

                                    # words over 3 characters long
                                    if len(l_word_ocr) > 2:
                                        l_more_3.append(l_dict)

                            # break loop on any exception
                            except Exception as e1:
                                if l_debug_messages:
                                    print(repr(e1))
                                break

                            # move the iterator one notch forward
                            self.m_logger.debug('before ri.Next()')
                            if not ri.Next(RIL.WORD):
                                break
                        # end of loop: while True:

                        # if less than 3 proper words, don't bother
                        if len(l_list) <= 3:
                            continue

                        # calculate the average confidence value
                        l_conf_list = [l[1] for l in l_list]
                        l_avg = sum(l_conf_list) / float(len(l_conf_list))

                        # calculate the percentage of words (more than 3 characters long) found in Tesserocr dictionary
                        if len(l_more_3) > 0:
                            l_dict_ratio = sum([1 if l else 0 for l in l_more_3])/float(len(l_more_3))
                        else:
                            l_dict_ratio = 0.0

                        if l_debug_messages:
                            print('Average Confidence : {0:.2f}'.format(l_avg))
                            print('Dictionary ratio   : {0:.2f}'.format(l_dict_ratio))
                        if l_avg < 75.0:
                            continue

                        # the final text, rebuilt from the list of proper words
                        l_txt = ' '.join([l[0] for l in l_list])

                        # add everything as a tuple in the results list
                        l_result_list.append((l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list))

                        # updates best values
                        if l_avg > l_max_avg:
                            l_max_avg = l_avg
                        if l_dict_ratio > l_max_dict_ratio:
                            l_max_dict_ratio = l_dict_ratio
                    # end of: if len(l_txt) > 10:

                    # release control to other threads
                    time.sleep(.001)
                # end of loop: for l_file in p_file_list

                # calculate the average ratio of dictionary hits
                if len(l_result_list) > 0:
                    l_avg_dict_ratio = sum([l[1] for l in l_result_list])/float(len(l_result_list))
                else:
                    l_avg_dict_ratio = 0.0
                if l_debug_messages:
                    print(
                        ('[{3}] {0} results, max avg: {1:.2f}, max dict. ' +
                         'ratio {2:.2f}, avg. dict. ratio {4:.2f}').format(
                            len(l_result_list), l_max_avg, l_max_dict_ratio, p_lang, l_avg_dict_ratio))

                # return the results
                return l_result_list, l_max_avg, l_max_dict_ratio, l_avg_dict_ratio
            # end def get_resultList(p_fileList, p_api, p_lang):

            # for debug purposes only
            def display_results(p_result_list, p_lang):
                print('-----[{0} / {1}]----------------------------------------------'.format(l_img_count, p_lang))
                # sort by increasing average confidence value
                p_result_list.sort(key=lambda l_tuple: l_tuple[0])
                for l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list in p_result_list:
                    l_file = re.sub(r'{0}/img\d+_'.format(l_img_path), '', l_file)
                    l_file = re.sub(r'\.png', '', l_file)
                    print('{1:.2f} "{2}" [{0}]'.format(l_file, l_avg, l_txt))
                for l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list in p_result_list:
                    l_file = re.sub(r'{0}/img\d+_'.format(l_img_path), '', l_file)
                    l_file = re.sub(r'\.png', '', l_file)
                    print('{1:.2f} "{2}" [{0}]'.format(l_file, l_avg, l_txt))
                    print('     {0}'.format(l_list))
                    print('     {0}'.format(l_raw_list))

            try:
                # build two result lists of results with the ordinary English training data and the joh data
                # OCR - eng
                with PyTessBaseAPI(lang='eng') as l_api_eng:
                    l_result_list_eng, l_max_avg_eng, l_max_dict_ratio_eng, l_avg_dict_ratio_eng = \
                        get_result_list(l_file_list, l_api_eng, 'eng')
                    if l_debug_messages and len(l_result_list_eng) > 0:
                        display_results(l_result_list_eng, 'eng')

                # OCR - joh
                with PyTessBaseAPI(lang='joh') as l_api_joh:
                    l_result_list_joh, l_max_avg_joh, l_max_dict_ratio_joh, l_avg_dict_ratio_joh = \
                        get_result_list(l_file_list, l_api_joh, 'joh')
                    if l_debug_messages and len(l_result_list_joh) > 0:
                        display_results(l_result_list_joh, 'joh')
            except Exception as e:
                self.m_logger.error('OCR error [{0}] l_internal = {1}'.format(repr(e), l_internal),
                                    extra={'m_errno': 1043})
                self.mark_as_ocred(l_internal)
                continue

            # internal function for selecting the final version
            def select_final_version(p_result_list):
                l_txt = ''
                l_vocabulary_select = []

                # boundaries of the bracket of values to be selected
                l_min_select = len(p_result_list) - 1 - l_clip - l_width
                l_max_select = len(p_result_list) - 1 - l_clip

                # safety bumpers
                if l_min_select < 0:
                    l_min_select = 0
                if l_max_select < 0:
                    l_max_select = len(p_result_list) - 1

                # calculate max length of result list (within selection bracket)
                # and also update the score by suffix and build the vocabulary list
                l_max_len = 0
                for i in range(l_min_select, l_max_select+1):
                    l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list = p_result_list[i]
                    if len(l_list) > l_max_len:
                        l_max_len = len(l_list)

                    # suffix extraction from the image filename
                    l_file = re.sub(
                        r'images_ocr/{0}_img\d+_'.format(multiprocessing.current_process().name), '', l_file)
                    l_suffix = re.sub(r'\.png', '', l_file)

                    # suffix score accounting
                    if len(p_result_list) > l_clip + l_width:
                        if l_suffix in l_suf_score.keys():
                            l_suf_score[l_suffix] += 1
                        else:
                            l_suf_score[l_suffix] = 1

                    # building the vocabulary list
                    for l_word_v, _, _ in l_list:
                        l_word_v = re.sub(r'[.,;:?!]*$', '', l_word_v)
                        if l_word_v not in l_vocabulary_select:
                            l_vocabulary_select.append(l_word_v)

                # select the longest (within selection bracket)
                for i in range(l_min_select, l_max_select+1):
                    l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list = p_result_list[i]
                    if len(l_list) == l_max_len:
                        break

                # print('l_vocabulary:', l_vocabulary, file=sys.stderr)
                return l_txt, l_vocabulary_select
            # end def select_final_version(p_result_list):

            # final selection
            l_text = ''
            if l_debug_messages:
                print('======[{0}]==================================================='.format(l_img_count))

            if l_max_avg_eng < l_max_avg_joh and l_avg_dict_ratio_eng < l_avg_dict_ratio_joh:
                # case where joh is clearly better than eng
                l_text, l_vocabulary = select_final_version(l_result_list_joh)
                if l_debug_messages:
                    print('RESULT (joh):', l_text)
                    print('[{0}] RESULT (joh):'.format(l_img_count), l_text, file=sys.stderr)
            elif l_max_avg_joh < l_max_avg_eng and l_avg_dict_ratio_joh < l_avg_dict_ratio_eng:
                # case where eng is clearly better than joh
                l_text, l_vocabulary = select_final_version(l_result_list_eng)
                if l_debug_messages:
                    print('RESULT (eng):', l_text)
                    print('[{0}] RESULT (eng):'.format(l_img_count), l_text, file=sys.stderr)
            else:
                # unclear cases
                l_txt_eng, l_vocabulary_eng = select_final_version(l_result_list_eng)
                l_txt_joh, l_vocabulary_joh = select_final_version(l_result_list_joh)

                # merge vocabularies
                l_vocabulary = l_vocabulary_eng
                for l_word in l_vocabulary_joh:
                    if l_word not in l_vocabulary:
                        l_vocabulary.append(l_word)

                # take the longest text
                if len(l_txt_eng) > len(l_txt_joh):
                    if l_debug_messages:
                        print('RESULT (Undecided/eng):', l_txt_eng)
                        print('[{0}] RESULT (Undecided/eng):'.format(l_img_count), l_txt_eng, file=sys.stderr)
                    l_text = l_txt_eng
                elif len(l_txt_joh) > 0:
                    if l_debug_messages:
                        print('RESULT (Undecided/joh):', l_txt_joh)
                        print('[{0}] RESULT (Undecided/joh):'.format(l_img_count), l_txt_joh, file=sys.stderr)
                    l_text = l_txt_joh

            # store in the database
            if len(l_vocabulary) > 0:
                # there is something to store
                if l_debug_messages:
                    print('VOCABULARY:', ' '.join(l_vocabulary))
                    print('[{0}] VOCABULARY:'.format(l_img_count), ' '.join(l_vocabulary), file=sys.stderr)

                self.update_media_ocr(l_text, l_vocabulary, l_internal)
                self.m_logger.info('OCR complete on [{0}]: {1}'.format(l_internal, l_text))
            else:
                # nothing to store, just mark the ocr as done
                self.mark_as_ocred(l_internal)

            l_img_count += 1
            if l_img_count == l_max_img_count:
                break
        # end of loop: for l_internal, l_media_src, l_full_picture, l_base64, l_base64_fp, l_att_count in l_media_list:

        # final results
        if l_debug_messages:
            print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
            l_suf_list = list(l_suf_score.items())
            l_suf_list.sort(key=lambda l_tuple: l_tuple[1])
            print(l_suf_list)
            for l_suf, l_count in l_suf_list:
                print('{0:4} {1}'.format(l_count, l_suf))

        self.m_logger.info('End ocr_images()')
        return len(l_media_list)

    def perform_request(self, p_request):
        """
        Calls Facebook's HTTP API and traps errors if any.
          
        :param p_request: The API request 
        :return: The response to the request from the FB API server (some JSON string).
        """

        self.m_logger.info('Start perform_request() Count: {0:,.0f}'.format(self.m_FBRequestCount))

        l_token = self.m_long_token[self.m_FBRequestCount % len(self.m_long_token)][0]

        l_request = p_request + '&access_token={0}'.format(l_token)
        l_request = re.sub(r'/&', '/?', l_request)  # in case there were no parameters at the end of the request

        self.m_logger.info('l_request : ' + l_request[:100])

        # this used to be the token replacement code when short-duration tokens were used

        # print('g_FBRequestCount:', g_FBRequestCount)

        # replace access token with the latest (this is necessary because
        # some old tokens may remain in the 'next' parameters kept from previous requests)
        # l_request = self.m_browserDriver.freshen_token(l_request)

        # request new token every G_TOKEN_LIFESPAN API requests, or when token is stale
        # if (self.m_FBRequestCount > 0 and self.m_FBRequestCount % EcAppParam.gcm_token_lifespan == 0)\
        #         or self.m_browserDriver.token_is_stale():
        #     l_request = self.m_browserDriver.renew_token_and_request(l_request)

        # request counter increment
        self.m_FBRequestCount += 1

        # FB API response
        l_response = None

        # number of errors encountered, to decide when to give up
        l_err_count = 0
        # special counter for session expiry (no longer used with long duration tokens)
        l_expiry_tries = 0

        # main loop
        l_finished = False
        while not l_finished:
            try:
                # test code - for FB page ID migration error handling
                if not LocalParam.gcm_prodEnv and re.search('100000010000001', l_request):
                    raise urllib.error.HTTPError(
                        'xx', 400, 'toto',
                        json.loads(
                            '{"x-fb-trace-id": "BZjHtxR+TzV", ' +
                            '"Access-Control-Allow-Origin": "*", ' +
                            '"x-fb-rev": "3550898", ' +
                            '"WWW-Authenticate": "OAuth -Facebook Platform- -invalid_request- ' +
                            '-Page ID 100000010000001 was migrated to page ID 100000010000002. ' +
                            'Please update your API calls to the new ID-", ' +
                            '"facebook-api-version": "v2.10", ' +
                            '"Cache-Control": "no-store", ' +
                            '"Content-Type": "text/javascript; charset=UTF-8", ' +
                            '"Vary": "Accept-Encoding", ' +
                            '"Pragma": "no-cache", ' +
                            '"Strict-Transport-Security": "max-age=15552000; preload", ' +
                            '"X-FB-Debug": "VbA2F2ssEfZzQVrH9ec9dseJz3bg9oo49wTXE6WYvxa' +
                            '4G5Lt+y8Oj2LWP/sfgC6WcFcmLSDwp8L+TYQJqnf7Dg==", ' +
                            '"Connection": "close", '
                            '"Date": "Fri, 29 Dec 2017 10:00:50 GMT", ' +
                            '"Expires": "Sat, 01 Jan 2000 00:00:00 GMT", ' +
                            '"Content-Length": "194"}'
                        ), None)

                l_response = urllib.request.urlopen(l_request, timeout=20).read().decode('utf-8').strip()
                # self.m_logger.info('l_response: {0}'.format(l_response))

                self.m_logger.info('perform_request() success : ' + l_response[:50])
                # if we reached this point, it means that no errors were encountered --> exit
                l_finished = True

            # here we are trapping so called 'HTTP errors' but in fact it may include API errors from FB
            except urllib.error.HTTPError as e:
                l_headers_dict = dict(e.headers.items())

                # log the error
                self.m_logger.warning(
                    ('HTTPError (Non Critical)\n{0} {1}\n{2} {3}\n{4} {5}\n{6} {7}' +
                     '\n{8} {9}\n{10} {11}\n{12} {13}').format(
                        'l_err_count    :', l_err_count,
                        'Request Problem:', repr(e),
                        '   Code        :', e.code,
                        '   Errno       :', e.errno,
                        '   Headers     :', l_headers_dict,
                        '   Message     :', e.msg,
                        'p_request      :', p_request
                    ), extra={'m_errno': 1053}
                )

                # Facebook API error
                if 'WWW-Authenticate' in l_headers_dict.keys():
                    l_fb_message = l_headers_dict['WWW-Authenticate']

                    # Request limit reached --> wait for 5 minutes and retry
                    if re.search(r'\(#17\) User request limit reached', l_fb_message):
                        l_wait = EcAppParam.gcm_wait_fb
                        self.m_logger.warning('FB request limit msg: {0} --> Waiting for {1} seconds'.format(
                            l_fb_message, l_wait), extra={'m_errno': 1086})

                        l_sleep_period = 5 * 60
                        for i in range(int(l_wait / l_sleep_period)):
                            time.sleep(l_sleep_period)
                            # l_request = self.m_browserDriver.renew_token_and_request(l_request)

                    # Unknown FB error --> wait 10 s. and retry 3 times max then return empty result
                    if re.search(r'An unexpected error has occurred', l_fb_message) \
                            or re.search(r'An unknown error has occurred', l_fb_message):
                        if l_err_count < 3:
                            l_wait = 10
                            self.m_logger.error(
                                'FB unknown error: {0} --> Waiting for {1} seconds'.format(l_fb_message, l_wait),
                                extra={'m_errno': 1044})

                            time.sleep(l_wait)
                            # l_request = self.m_browserDriver.renew_token_and_request(l_request)
                        else:
                            l_response = '{"data": []}'

                            self.m_logger.critical(
                                'FB unknown error: {0} --> Returned: {1}\n'.format(l_fb_message, l_response),
                                extra={'m_errno': 1045})

                            l_finished = True

                    # Session expired ---> nothing to do (this should no longer happen with long duration tokens)
                    elif re.search(r'Session has expired', l_fb_message):
                        if l_expiry_tries < 3:
                            # l_request = self.m_browserDriver.renew_token_and_request(l_request)
                            l_expiry_tries += 1
                        else:
                            l_msg = 'FB session expiry msg: {0}'.format(l_fb_message)
                            self.m_logger.critical(l_msg, extra={'m_errno': 1046})
                            raise BulkDownloaderException(l_msg)

                    # Object does not exist
                    elif re.search(r'Object\s+with\s+ID\s+\'[\d_]+\'\s+does\s+not\s+exist', l_fb_message):
                        self.m_logger.warning(
                            'NON_EXIST Non existent FB object : {0}'.format(l_fb_message), extra={'m_errno': 1047})

                        raise BulkDownloaderException('NON_EXIST')

                    # Unsupported get request ---> return empty data and abandon request attempt
                    elif re.search(r'Unsupported get request', l_fb_message):
                        l_response = '{"data": []}'

                        self.m_logger.error(
                            'FB unsupported get msg: {0} --> Returned: {1}'.format(l_fb_message, l_response),
                            extra={'m_errno': 1048})

                        l_finished = True

                    # Page has changed ID
                    elif re.search('Page\s+ID\s+\d+\s+was\s+migrated\s+to\s+page\s+ID\s+\d+', l_fb_message):
                        l_match = re.search(
                            'Page\s+ID\s+(\d+)\s+was\s+migrated\s+to\s+page\s+ID\s+(\d+)', l_fb_message)
                        l_old_id = l_match.group(1)
                        l_new_id = l_match.group(2)

                        l_msg = 'Page migrated: {0} --> {1}'.format(l_old_id, l_new_id)
                        self.m_logger.warning(l_msg, extra={'m_errno': 1049})
                        raise PageIDMigration(l_msg, l_old_id, l_new_id)

                    # Other (unknown) FB error --> critical log msg + raise
                    else:
                        self.m_logger.error('FB msg: {0}'.format(l_fb_message), extra={'m_errno': 1050})
                        raise BulkDownloaderException('FB msg: {0}'.format(l_fb_message))

                # Non FB HTTPError: either internet is down and wait 5 min or use get_wait to
                # determine how long to wait
                else:
                    if self.m_background_task.internet_check():
                        l_wait = self.get_wait(l_err_count)
                        l_msg = 'Unknown reason'
                    else:
                        l_wait = 5 * 60
                        l_msg = 'Internet down'
                    self.m_logger.warning(
                        'Non FB HTTPError {0} ({2}) --> Waiting for {1} seconds'.format(repr(e), l_wait, l_msg),
                        extra={'m_errno': 1051})

                    time.sleep(l_wait)
                    # if l_wait > 60 * 15:
                    #     l_request = self.m_browserDriver.renew_token_and_request(l_request)

                l_err_count += 1

            # URL Errors, considered non critical (why ?) wait one second
            except urllib.error.URLError as e:
                self.m_logger.warning('URLError (Non Critical)\n{0} {1}\n{2} {3}\n{4} {5}\n{6} {7}\n{8} {9}'.format(
                    'l_errCount     :', l_err_count,
                    'Request Problem:', repr(e),
                    '   Errno       :', e.errno,
                    '   Message     :', e.reason,
                    'p_request      :', p_request
                ), extra={'m_errno': 1052})

                time.sleep(1)
                l_err_count += 1

            # Other Errors, wait one second
            except Exception as e:
                self.m_logger.warning('Unknown Error\n{0} {1}\n{2} {3}\n{4} {5}\n{6} {7}'.format(
                    'l_errCount     :', l_err_count,
                    'Request Problem:', repr(e),
                    '   Message     :', e.args,
                    'p_request      :', p_request
                ), extra={'m_errno': 1054})

                time.sleep(1)
                l_err_count += 1

        self.m_logger.info('End perform_request() Count: {0:,.0f}'.format(self.m_FBRequestCount))
        return l_response

    def get_wait(self, p_error_count):
        """
        Selects the appropriate wait-time depending on the number of accumulated errors 
        (for :any:`BulkDownloader.perform_request`)
        
        :param p_error_count: Number of errors so far.
        :return: The wait delay in seconds.
        """
        if p_error_count < 3:
            return 5
        elif p_error_count < 6:
            return 30
        elif p_error_count < 9:
            return 60 * 2
        elif p_error_count < 12:
            return 60 * 5
        elif p_error_count < 15:
            return 60 * 15
        elif p_error_count < 18:
            return 60 * 30
        elif p_error_count < 21:
            return 60 * 60
        else:
            self.m_logger.critical('Too many errors: {0}'.format(p_error_count), extra={'m_errno': 1055})
            raise BulkDownloaderException('Too many errors: {0}'.format(p_error_count))

    def store_object(self,
                     p_padding,
                     p_type,
                     p_date_creation,
                     p_date_modification,
                     p_id,
                     p_parent_id,
                     p_page_id,
                     p_post_id,
                     p_fb_type,
                     p_fb_status_type,
                     p_share_count,
                     p_like_count,
                     p_permalink_url,
                     p_name='',
                     p_caption='',
                     p_desc='',
                     p_story='',
                     p_message='',
                     p_fb_parent_id='',
                     p_fb_object_id='',
                     p_link='',
                     p_place='',
                     p_source='',
                     p_user_id='',
                     p_tags='',
                     p_with_tags='',
                     p_properties='',
                     p_shared=False):

        """
        DB storage of a new object (page, post or comment).
        
        :param p_padding: 
        :param p_type: 
        :param p_date_creation: 
        :param p_date_modification: 
        :param p_id: 
        :param p_parent_id:
        :param p_page_id:
        :param p_post_id:
        :param p_fb_type: 
        :param p_fb_status_type: 
        :param p_share_count:
        :param p_like_count:
        :param p_permalink_url: 
        :param p_name: 
        :param p_caption: 
        :param p_desc: 
        :param p_story: 
        :param p_message: 
        :param p_fb_parent_id:
        :param p_fb_object_id:
        :param p_link:
        :param p_place:
        :param p_source: 
        :param p_user_id:
        :param p_tags: 
        :param p_with_tags: 
        :param p_properties:
        :param p_shared:
        :return: `True` if insertion occurred
        """
        self.m_logger.debug('Start store_object()')

        # increment attempt counter
        self.m_objectStoreAttempts += 1

        l_stored = False

        # Creation date
        # date format: 2016-04-22T12:03:06+0000 ---> 2016-04-22 12:03:06
        l_date_creation = re.sub('T', ' ', p_date_creation)
        l_date_creation = re.sub(r'\+\d+$', '', l_date_creation).strip()

        if len(l_date_creation) == 0:
            l_date_creation = datetime.datetime.now()
        else:
            l_date_creation = datetime.datetime.strptime(l_date_creation, '%Y-%m-%d %H:%M:%S')

        # Last mod date
        # date format: 2016-04-22T12:03:06+0000 ---> 2016-04-22 12:03:06
        l_date_modification = re.sub('T', ' ', p_date_modification)
        l_date_modification = re.sub(r'\+\d+$', '', l_date_modification).strip()

        if len(l_date_modification) == 0:
            l_date_modification = datetime.datetime.now()
        else:
            l_date_modification = datetime.datetime.strptime(l_date_modification, '%Y-%m-%d %H:%M:%S')

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.store_object()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                INSERT INTO "TB_OBJ"(
                    "ID"
                    ,"ID_FATHER"
                    ,"ID_PAGE"
                    ,"ID_POST"
                    ,"DT_CRE"
                    ,"DT_MOD"
                    ,"TX_PERMALINK"
                    ,"ST_TYPE"
                    ,"ST_FB_TYPE"
                    ,"ST_FB_STATUS_TYPE"
                    ,"TX_NAME"
                    ,"TX_CAPTION"
                    ,"TX_DESCRIPTION"
                    ,"TX_STORY"
                    ,"TX_MESSAGE"
                    ,"ID_USER"
                    ,"N_LIKES"
                    ,"N_SHARES"
                    ,"TX_PLACE"
                    ,"TX_TAGS"
                    ,"TX_WITH_TAGS"
                    ,"TX_PROPERTIES"
                    ,"ST_FB_PARENT_ID"
                    ,"ST_FB_OBJECT_ID"
                    ,"TX_LINK"
                    ,"TX_SOURCE"
                    ,"P_IS_SHARED_POST")
                VALUES(
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s) 
            """, (
                p_id,
                p_parent_id,
                p_page_id,
                p_post_id,
                l_date_creation,
                l_date_modification,
                p_permalink_url,
                p_type,
                p_fb_type,
                p_fb_status_type,
                p_name,
                p_caption,
                p_desc,
                p_story,
                p_message,
                p_user_id,
                p_like_count,
                p_share_count,
                p_place,
                p_tags,
                p_with_tags,
                p_properties,
                p_fb_parent_id,
                p_fb_object_id,
                p_link,
                p_source,
                p_shared
                )
            )

            self.m_objectStored += 1
            l_conn.commit()
            l_stored = True
        except psycopg2.IntegrityError as e:
            self.m_logger.info('{0}Object already in TB_OBJ [{1}]'.format(p_padding, repr(e)))
            l_conn.rollback()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical(
                'TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query), extra={'m_errno': 1056})
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.info(
            '{0}Object counts: {1} attempts / {2} stored / {3} posts retrieved / {4} comments retrieved'.format(
                p_padding,
                self.m_objectStoreAttempts,
                self.m_objectStored,
                self.m_postRetrieved,
                self.m_commentRetrieved))

        self.m_logger.debug('End store_object()')
        return l_stored

    def migrate_id_page(self, p_old_id, p_new_id, p_name):
        """

        :param p_old_id:
        :param p_new_id:
        :param p_name:
        :return:
        """
        self.m_logger.info('migrate_id_page() Start')

        # update TB_PAGES to indicate that the old page ID is no longer to be downloaded
        l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.migrate_id_page() migration 1')
        l_cursor_write = l_conn_write.cursor()

        try:
            l_cursor_write.execute("""
                update "TB_PAGES"
                set "F_DNL" = 'N', "F_NON_EXIST" = true
                where "ID" = %s;
            """, (p_old_id,))

            l_conn_write.commit()
        except Exception as e:
            l_conn_write.rollback()
            self.m_logger.critical('Error updating TB_PAGES: {0}/{1}'.format(repr(e), l_cursor_write.query),
                                   extra={'m_errno': 1057})
            raise
        finally:
            # release DB objects once finished
            l_cursor_write.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_write)

        # insert the new ID in TB_PAGES and TB_OBJ -- treated like new page
        l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.migrate_id_page() migration 2')
        l_cursor_write = l_conn_write.cursor()

        try:
            l_cursor_write.execute("""
                insert into "TB_OBJ"(
                    "ID"
                    , "DT_CRE"
                    , "TX_NAME"
                    , "DT_MOD"
                    , "ST_TYPE"
                    , "ST_FB_TYPE"
                    , "ST_FB_STATUS_TYPE") 
                values( %s, %s, %s, %s, 'Page', 'Page', 'Page');
            """, (
                p_new_id,
                datetime.datetime.now(),
                p_name,
                datetime.datetime.now()
            ))

            l_conn_write.commit()
        except Exception as e:
            l_conn_write.rollback()
            self.m_logger.critical('Error updating TB_PAGES: {0}/{1}'.format(repr(e), l_cursor_write.query),
                                   extra={'m_errno': 1058})
            raise
        finally:
            # release DB objects once finished
            l_cursor_write.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_write)

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.migrate_id_page() migration 3')
        l_cursor = l_conn.cursor()

        l_internal_id = None
        try:
            l_cursor.execute("""
                select "ID_INTERNAL" 
                from "TB_OBJ"
                where "ID" = %s;
            """, (p_new_id,))

            for l_internal_id, in l_cursor:
                pass
        except Exception as e:
            self.m_logger.critical('Error selecting from TB_OBJ: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1059})
            raise
        finally:
            # release DB objects once finished
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        if l_internal_id is None:
            l_msg = 'Cannot find internal ID of new page just inserted into TB_OBJ - ID: ' + p_new_id
            self.m_logger.critical(l_msg, extra={'m_errno': 1060})
            raise BulkDownloaderException(l_msg)

        l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.migrate_id_page() migration 4')
        l_cursor_write = l_conn_write.cursor()

        try:
            l_cursor_write.execute("""
                insert into "TB_PAGES"(
                    "ID"
                    , "ID_OBJ_INTERNAL"
                    , "DT_CRE"
                    , "TX_NAME"
                    , "ST_TYPE"
                    , "ST_FB_TYPE"
                    , "F_DNL"
                    , "F_NON_EXIST") 
                values( %s, %s, %s, %s, 'Page', 'Page', 'Y', false);
            """, (
                p_new_id,
                l_internal_id,
                datetime.datetime.now(),
                p_name
            ))

            l_conn_write.commit()
        except Exception as e:
            l_conn_write.rollback()
            self.m_logger.critical('Error updating TB_PAGES: {0}/{1}'.format(repr(e), l_cursor_write.query),
                                   extra={'m_errno': 1061})
            raise
        finally:
            # release DB objects once finished
            l_cursor_write.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_write)

        self.m_logger.info('migrate_id_page() End')

    def set_shares_downloaded(self, p_id):
        """

        :param p_id:
        :return:
        """
        self.m_logger.info('Start set_shares_downloaded()')
        l_stored = False

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.set_shares_downloaded()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                UPDATE "TB_OBJ"
                SET
                    "F_SHARES_DOWNLOADED" = true
                WHERE "ID" = %s
            """, (p_id, ))
            l_conn.commit()
            l_stored = True
        except psycopg2.IntegrityError as e:
            self.m_logger.error('Object Cannot be updated: {0}/{1}'.format(repr(e), l_cursor.query),
                                extra={'m_errno': 1062})
            l_conn.rollback()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1063})
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.info('End set_shares_downloaded()')
        return l_stored

    def set_non_exist(self, p_id, p_page=False):
        """

        :param p_id:
        :param p_page:
        :return:
        """
        self.m_logger.debug('Start set_non_exist()')
        l_stored = False

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.set_non_exist() TB_OBJ')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                UPDATE "TB_OBJ"
                SET
                    "F_NON_EXIST" = true,
                    "F_LOCKED" = false
                WHERE "ID" = %s
            """, (p_id, ))
            l_conn.commit()
            l_stored = True
        except psycopg2.IntegrityError as e:
            self.m_logger.error('Object Cannot be updated: {0}/{1}'.format(repr(e), l_cursor.query),
                                extra={'m_errno': 1064})
            l_conn.rollback()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1065})
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        if p_page:
            l_stored_page = False

            l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.set_non_exist() TB_PAGES')
            l_cursor = l_conn.cursor()

            try:
                l_cursor.execute("""
                    UPDATE "TB_PAGES"
                    SET
                        "F_NON_EXIST" = true
                    WHERE "ID" = %s
                """, (p_id,))
                l_conn.commit()
                l_stored_page = True
            except psycopg2.IntegrityError as e:
                self.m_logger.error('Object Cannot be updated: {0}/{1}'.format(repr(e), l_cursor.query),
                                    extra={'m_errno': 1066})
                l_conn.rollback()
            except Exception as e:
                l_conn.rollback()
                self.m_logger.critical('TB_PAGES Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                       extra={'m_errno': 1067})
                raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))
            finally:
                l_cursor.close()
                EcConnectionPool.get_global_pool().putconn(l_conn)

            l_stored = l_stored and l_stored_page

        self.m_logger.debug('End set_non_exist()')
        return l_stored

    def update_object(self, p_id, p_share_count, p_like_count, p_name, p_caption, p_desc, p_story, p_message):
        """
        Update of an existing object (page, post or comment).
        
        :param p_id: 
        :param p_share_count:
        :param p_like_count:
        :param p_name: 
        :param p_caption: 
        :param p_desc: 
        :param p_story: 
        :param p_message: 
        :return: `True` if update completed.
        """
        self.m_logger.debug('Start update_object()')
        l_stored = False

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.update_object()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                UPDATE "TB_OBJ"
                SET
                    "N_LIKES" = %s
                    ,"N_SHARES" = %s
                    ,"TX_NAME" = %s
                    ,"TX_CAPTION" = %s
                    ,"TX_DESCRIPTION" = %s
                    ,"TX_STORY" = %s
                    ,"TX_MESSAGE" = %s
                    ,"DT_LAST_UPDATE" = CURRENT_TIMESTAMP
                WHERE "ID" = %s
            """, (p_like_count, p_share_count, p_name, p_caption, p_desc, p_story, p_message, p_id))
            l_conn.commit()
            l_stored = True
        except psycopg2.IntegrityError as e:
            self.m_logger.error('Object Cannot be updated: {0}/{1}'.format(repr(e), l_cursor.query),
                                extra={'m_errno': 1068})
            l_conn.rollback()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1069})
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.debug('End update_object()')
        return l_stored

    @staticmethod
    def cut_max(s, p_max_len):
        l_cut_padding = '[...]'

        if s is None or len(s) == 0:
            return ''
        elif len(s) < p_max_len - len(l_cut_padding):
            return s
        else:
            return s[:int(p_max_len) - len(l_cut_padding) - 1] + l_cut_padding

    def store_user(self, p_id, p_name, p_date, p_padding, p_type='User'):
        """
        DB Storage of a new user. If user already in the BD, traps the integrity violation error and returns `False`.
        
        :param p_id: User ID (API App. specific)
        :param p_name: User Name
        :param p_date: Date of the object in which user first appeared.
        :param p_padding: Debug/Info massage left padding.
        :param p_type: `page` or `user`
        :return: `True` if insertion occurred.
        """
        self.m_logger.debug('Start store_user()')
        # date format: 2016-04-22T12:03:06+0000 ---> 2016-04-22 12:03:06
        l_date = re.sub('T', ' ', p_date)
        l_date = re.sub(r'\+\d+$', '', l_date)

        if len(l_date) == 0:
            l_date = datetime.datetime.now()
        else:
            l_date = datetime.datetime.strptime(l_date, '%Y-%m-%d %H:%M:%S')

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.store_user()')
        l_cursor = l_conn.cursor()

        l_inserted = False
        try:
            l_cursor.execute("""
                INSERT INTO "TB_USER"("ID", "ST_NAME", "DT_CRE", "DT_MSG", "ST_TYPE")
                VALUES( %s, %s, %s, %s, %s )
            """, (
                p_id,
                BulkDownloader.cut_max(p_name, 250),
                datetime.datetime.now(),
                l_date,
                p_type
            ))
            l_conn.commit()
            l_inserted = True
        except psycopg2.IntegrityError as e:
            self.m_logger.debug('{0}User already known: [{1}]'.format(p_padding, repr(e)))
            # print('{0}PostgreSQL: {1}'.format(p_padding, e))
            l_conn.rollback()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_USER Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1070})
            raise BulkDownloaderException('TB_USER Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.debug('End store_user()')
        return l_inserted

    def get_user_internal_id(self, p_id):
        """
        Looks up the internal ID of an user based on its API App-specific ID.
        
        :param p_id: API App-specific ID.
        :return: Internal ID
        """
        self.m_logger.debug('Start get_user_internal_id()')
        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.get_user_internal_id()')
        l_cursor = l_conn.cursor()

        l_ret_id = None
        try:
            l_cursor.execute("""
                select "ID_INTERNAL"
                from "TB_USER"
                where "ID" = %s
            """, (p_id, ))

            for l_internal_id, in l_cursor:
                l_ret_id = l_internal_id

        except Exception as e:
            self.m_logger.critical('TB_USER Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1071})
            raise BulkDownloaderException('TB_USER Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.debug('End get_user_internal_id()')
        return l_ret_id

    def create_like_link(self, p_user_id_internal, p_obj_id_internal, p_date):
        """
        DB storage of a link between a liked object and the author of the like.
        
        :param p_user_id_internal: Internal ID of the user.
        :param p_obj_id_internal:  Internal ID of the object.
        :param p_date: Date the like was placed.
        :return: `True` if insertion occurred.
        """
        self.m_logger.debug('Start create_like_link()')
        # date format: 2016-04-22T12:03:06+0000 ---> 2016-04-22 12:03:06
        l_date = re.sub('T', ' ', p_date)
        l_date = re.sub(r'\+\d+$', '', l_date)

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.create_like_link()')
        l_cursor = l_conn.cursor()

        l_inserted = False
        try:
            l_cursor.execute("""
                INSERT INTO "TB_LIKE"("ID_USER_INTERNAL","ID_OBJ_INTERNAL","DT_CRE")
                VALUES( %s, %s, %s )
            """, (p_user_id_internal, p_obj_id_internal, l_date))
            l_conn.commit()
            l_inserted = True
        except psycopg2.IntegrityError:
            l_conn.rollback()
            if EcAppParam.gcm_verboseModeOn:
                self.m_logger.info('Like link already exists')
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_LIKE Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1072})
            raise BulkDownloaderException('TB_LIKE Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.debug('End create_like_link()')
        return l_inserted

    def set_like_flag(self, p_id, p_unlock_only=False):
        """
        Sets a flag on an object to indicate that the like details have been fetched.
        
        :param p_id: API App-specific ID of the object.
        :param p_unlock_only:
        :return: Nothing
        """
        self.m_logger.debug('Start set_like_flag()')
        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.set_like_flag()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                update "TB_OBJ"
                set 
                    {0}
                    "F_LOCKED" = FALSE
                where "ID" = %s
            """.format(
                '"F_LIKE_DETAIL" = \'X\', ' if not p_unlock_only else ''
            ), (p_id,))
            l_conn.commit()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1073})
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.debug('End set_like_flag()')

    def store_media(
            self, p_id, p_fb_type, p_desc, p_title, p_tags, p_target,
            p_media, p_media_src, p_width, p_height, p_picture, p_full_picture, p_from_parent):
        """
        Db storage of a media element.
        
        :param p_id: 
        :param p_fb_type: 
        :param p_desc: 
        :param p_title: 
        :param p_tags: 
        :param p_target: 
        :param p_media: 
        :param p_media_src: 
        :param p_width: 
        :param p_height: 
        :param p_picture:
        :param p_full_picture:
        :param p_from_parent:
        :return: Nothing (the insertion will always succeed except for technical malfunction)
        """
        self.m_logger.debug('Start store_media()')

        l_conn = EcConnectionPool.get_global_pool().getconn('BulkDownloader.store_user()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                INSERT INTO "TB_MEDIA"(
                    "ID_OWNER"
                    ,"ST_FB_TYPE"
                    ,"TX_DESC"
                    ,"TX_TITLE"
                    ,"TX_TAGS"
                    ,"TX_TARGET"
                    ,"TX_MEDIA"
                    ,"TX_MEDIA_SRC"
                    ,"N_WIDTH"
                    ,"N_HEIGHT"
                    ,"TX_PICTURE"
                    ,"TX_FULL_PICTURE"
                    ,"F_FROM_PARENT"
                )
                VALUES( 
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s)
            """, (
                p_id, p_fb_type, p_desc, p_title, p_tags, p_target,
                p_media, p_media_src, p_width, p_height, p_picture, p_full_picture, p_from_parent))
            l_conn.commit()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_MEDIA Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query),
                                   extra={'m_errno': 1074})
            raise BulkDownloaderException('TB_MEDIA Unknown Exception: {0}'.format(repr(e)))
        finally:
            l_cursor.close()
            EcConnectionPool.get_global_pool().putconn(l_conn)

        self.m_logger.debug('Start store_media()')

    def mark_as_ocred(self, p_internal):
        """

        :param p_internal:
        :return:
        """

        l_conn_w = EcConnectionPool.get_global_pool().getconn('BulkDownloader.mark_as_ocred() UPDATE')
        l_cursor_w = l_conn_w.cursor()

        self.m_logger.info('OCR complete on [{0}]'.format(p_internal))
        try:
            l_cursor_w.execute("""
                        update "TB_MEDIA"
                        set 
                            "F_OCR" = true,
                            "F_LOCK" = NULL
                        where "ID_MEDIA_INTERNAL" = %s
                    """, (p_internal,))
            l_conn_w.commit()
        except Exception as e1:
            l_conn_w.rollback()
            l_msg0 = 'Error updating TB_MEDIA: {0}/{1}'.format(repr(e1), l_cursor_w.query, extra={'m_errno': 1075})
            self.m_logger.critical(l_msg0)
            raise BulkDownloaderException(l_msg0)
        finally:
            l_cursor_w.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_w)

    def update_media_ocr(self, p_text, p_vocabulary, p_internal):
        """

        :param p_text:
        :param p_vocabulary:
        :param p_internal:
        :return:
        """
        self.m_logger.info('update_media_ocr() start')

        l_conn_write = EcConnectionPool.get_global_pool().getconn('BulkDownloader.update_media_ocr() UPDATE')
        l_cursor_write = l_conn_write.cursor()

        try:
            l_cursor_write.execute("""
                update "TB_MEDIA"
                set 
                    "F_OCR" = true
                    , "F_LOCK" = NULL
                    , "TX_TEXT" = %s
                    , "TX_VOCABULARY" = %s
                where "ID_MEDIA_INTERNAL" = %s
            """, (p_text, ' '.join(p_vocabulary), p_internal))
            l_conn_write.commit()
        except Exception as e:
            l_conn_write.rollback()
            l_msg = 'Error updating TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor_write.query)
            self.m_logger.critical(l_msg, extra={'m_errno': 1076})
            raise BulkDownloaderException(l_msg)
        finally:
            l_cursor_write.close()
            EcConnectionPool.get_global_pool().putconn(l_conn_write)

        self.m_logger.info('update_media_ocr() start')

    def get_mandatory_field(self, p_json, p_field, p_context=''):
        """
        Macro to get a mandatory field from an API response.

        :param p_json: The API response JSON fragment
        :param p_field: The requested field
        :param p_context: An information string to be included in the error message if any
        :return: The field contents if present.
        """
        l_value = ''

        if p_field in p_json.keys():
            l_value_raw = p_json[p_field]
            if type(l_value_raw) is str:
                l_value = re.sub('\s+', ' ', l_value_raw).strip()
            else:
                l_value = l_value_raw
        else:
            self.m_logger.error('Missing key [{0}] {1}'.format(p_field, p_context), extra={'m_errno': 1079})

        return l_value

    @classmethod
    def get_optional_field(cls, p_json, p_field):
        """
        Macro to get a field from an API response that may or may not be present.

        :param p_json: The API response JSON fragment
        :param p_field: The requested field
        :return: The field contents if present (full + shortened to 100 char). Empty strings otherwise.
        """
        l_value = ''
        l_value_short = ''

        if p_field in p_json.keys():
            l_value = re.sub('\s+', ' ', p_json[p_field]).strip()
            if len(l_value) > 100:
                l_value_short = l_value[0:100] + ' ... ({0})'.format(len(l_value))
            else:
                l_value_short = l_value

        return l_value, l_value_short

# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| FB scraping web service for ROAD B SCORE                   |')
    print('|                                                            |')
    print('| POST request sending test client                           |')
    print('|                                                            |')
    print('| v. 1.0 - 28/02/2017                                        |')
    print('+------------------------------------------------------------+')

    random.seed()

    def raiser():
        raise BulkDownloaderException('toto')

    try:
        raiser()
    except BulkDownloaderException as e0:
        print(str(e0))
