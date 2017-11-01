#!/usr/bin/python3
# -*- coding: utf-8 -*-

from PIL import ImageEnhance, ImageFilter

import json
import base64
import socket
from tesserocr import PyTessBaseAPI, RIL

from cs_fb_connect import *
from wrapvpn import *

__author__ = 'Pavan Mahalingam'


# ----------------------------------- Tesseract -----------------------------------------------------------
# https://pypi.python.org/pypi/tesserocr
# apt-get install tesseract-ocr libtesseract-dev libleptonica-dev
# sudo pip3 install Cython
# sudo apt-get install g++
# sudo apt-get install python3-dev
# sudo pip3 install tesserocr

class BulkDownloaderException(Exception):
    def __init__(self, p_msg):
        self.m_msg = p_msg


class BulkDownloader:
    """
    Bogus class used to isolate the bulk downloading (FB API) features.
    """
    def __init__(self, p_pool, p_long_token, p_long_token_expiry, p_background_task):
        #: Local copy of the FB long-duration access token.
        self.m_long_token = p_long_token

        #: Local copy of the FB long-duration access token expiry date
        self.m_long_token_expiry = p_long_token_expiry

        #: Local copy of the background task class instance to allow calling its methods.
        self.m_background_task = p_background_task

        #: Connection pool
        self.m_pool = p_pool

        #: Class-specific logger
        self.m_logger = logging.getLogger('BulkDownloader')

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

        #: OCR Thread
        self.m_ocr_thread = None

        #: Image fetching Thread
        self.m_image_fetch_thread = None

        #: Boolean variable controlling the threads. When `False`, all threads stop.
        self.m_threads_proceed = True

        #: Bictionary of UNICODE ligatures, to make sure none are kept in OCR text
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

    def start_threads(self):
        """
        Thread Launching.

        :return: Nothing
        """
        # OCR Thread
        if EcAppParam.gcm_ocr_thread:
            self.m_logger.info('starting Image ocr thread ....')
            self.m_ocr_thread = Thread(target=self.repeat_ocr_image)
            # One-letter name for the OCR thread
            self.m_ocr_thread.name = 'O'
            self.m_ocr_thread.start()
            self.m_logger.info('Image OCR thread started')

        # Image fetching thread
        self.m_image_fetch_thread = Thread(target=self.repeat_fetch_images)
        # One-letter name for the Image thread
        self.m_image_fetch_thread.name = 'I'
        self.m_image_fetch_thread.start()
        self.m_logger.info('Image fetch thread launched')

    def stop_threads(self):
        """
        Stops all threads.

        :return: Nothing.
        """

        # Positioning this flag will stop all threads (hopefully).
        self.m_threads_proceed = False

        # stopping OCR thread
        self.m_ocr_thread.join()
        if self.m_ocr_thread.is_alive():
            self.m_logger.critical('Could not stop ocr thread ....')
            exit(0)

        # stopping OCR thread
        self.m_image_fetch_thread.join()
        if self.m_image_fetch_thread.is_alive():
            self.m_logger.critical('Could not stop image fetch thread ....')
            exit(0)

    def bulk_download(self):
        """
        Performs the core of the bulk-downloading tasks: New pages and new posts downloading.
        
        :return: Nothing
        """
        self.m_logger.info('Start bulk_download()')

        # self.m_logger.info('Getting FB token')
        # self.m_browserDriver.get_fb_token()

        self.get_pages()
        # self.get_posts()
        # self.updatePosts()
        # self.getLikesDetail()

        self.m_logger.info('End bulk_download()')

    def get_pages(self):
        """
        Gets the page list from shares posted to TestPage.

        :return: Nothing
        """
        self.m_logger.info('Start getPages()')

        # list of shared posts in TestPage feed

        l_fieldList = 'id,caption,created_time,description,from,icon,link,message,message_tags,name,object_id,' + \
                      'permalink_url,picture,place,properties,shares,source,status_type,story,to,type,' + \
                      'updated_time,with_tags,parent_id'

        l_request = ('https://graph.facebook.com/{0}/{1}/feed?limit={2}&' +
                     'access_token={3}&fields={4}').format(
            EcAppParam.gcm_api_version, '706557539525134', EcAppParam.gcm_limit, self.m_long_token, l_fieldList)
        l_response = self.performRequest(l_request)

        self.m_logger.info('l_request:' + l_request)
        # self.m_logger.info('l_response:' + l_response)

        # each like is a page
        l_responseData = json.loads(l_response)
        l_finished = False
        while not l_finished:
            for l_post in l_responseData['data']:
                if 'parent_id' in l_post.keys():
                    l_parent_id = l_post['parent_id']
                    self.m_logger.info('l_parent_id:' + l_parent_id)

                    l_request_post = ('https://graph.facebook.com/{0}/{1}?limit={2}&' +
                                      'access_token={3}&fields={4}').format(
                        EcAppParam.gcm_api_version, l_parent_id, EcAppParam.gcm_limit, self.m_long_token, l_fieldList)
                    l_response_post = self.performRequest(l_request_post)
                    # self.m_logger.info('l_response_post:' + l_response_post)

                    l_response_post_data = json.loads(l_response_post)
                    if 'from' in l_response_post_data.keys():
                        l_from = l_response_post_data['from']
                        l_page_id = l_from['id']
                        l_page_name = l_from['name']

                        self.m_logger.info('id   :' + l_page_id)
                        self.m_logger.info('name :' + l_page_name)

                        # store page information
                        self.storeObject(
                            p_padding='',
                            p_type='Page',
                            p_date_creation='',
                            p_date_modification='',
                            p_id=l_page_id,
                            p_parentId='',
                            p_pageId='',
                            p_postId='',
                            p_fb_type='Page',
                            p_fb_status_type='Page',
                            p_shareCount=0,
                            p_likeCount=0,
                            p_permalink_url='',
                            p_name=l_page_name)

            # end of for l_post in l_responseData['data']:

            if 'paging' in l_responseData.keys() and 'next' in l_responseData['paging'].keys():
                l_request = l_responseData['paging']['next']
                l_response = self.performRequest(l_request)

                l_responseData = json.loads(l_response)
            else:
                l_finished = True

        # end of while not l_finished:

        self.m_logger.info('End getPages()')

    def get_posts(self):
        """
        Gets the posts from all the pages in :any:`TB_PAGES`

        To transfer from :any:`TB_OBJ` to :any:`TB_PAGES`:
        .. code-block:: sql

            delete from "TB_PAGES";
            insert into "TB_PAGES"("ID","DT_CRE","ST_TYPE","ST_FB_TYPE","TX_NAME","ID_OBJ_INTERNAL")
            select "ID","DT_CRE","ST_TYPE","ST_FB_TYPE","TX_NAME","ID_INTERNAL"
            from "TB_OBJ"
            where "ST_TYPE" = 'Page';
        
        :return: Nothing
        """
        self.m_logger.info('Start get_posts()')

        l_conn = self.m_pool.getconn('BulkDownloader.get_posts()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                select "ID", "TX_NAME" from "TB_PAGES" order by "DT_CRE";
            """)
        except Exception as e:
            self.m_logger.warning('Error selecting from TB_PAGES: {0}/{1}'.format(repr(e), l_cursor.query))
            raise

        for l_id, l_name in l_cursor:
            self.m_logger.info('$$$$$$$$$ [{0}] $$$$$$$$$$'.format(l_name))
            self.m_page = l_name
            self.getPostsFromPage(l_id)

        l_cursor.close()
        self.m_pool.putconn(l_conn)
        self.m_logger.info('End get_posts()')

    def repeat_fetch_images(self):
        """
        Calls :any:`BulkDownloader.fetch_images()` repeatedly, with a 10 second delay between calls. Meant to be 
        the image fetching thread initiated in :any:`BulkDownloader.__init__()`. The loop stops (and the thread 
        terminates) when :any:`m_fetch_proceed` is set to `False`
        
        :return: Nothing 
        """
        self.m_logger.info('Start repeat_fetch_images()')
        while self.m_threads_proceed:
            self.fetch_images()
            time.sleep(10)

        self.m_logger.info('End repeat_fetch_images()')

    def fetch_images(self):
        """
        Image fetching. Take a block of 500 records in `TB_MEDIA` and attempts to download the pictures they reference
        (if any).
        
        :return: Nothing 
        """
        self.m_logger.info('Start fetch_images()')

        l_conn = self.m_pool.getconn('BulkDownloader.fetch_images()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                select "TX_MEDIA_SRC", "N_WIDTH", "N_HEIGHT", "ID_MEDIA_INTERNAL" 
                from "TB_MEDIA"
                where "TX_MEDIA_SRC" is not NULL and not "F_LOADED" and not "F_ERROR"
                limit 500;
            """)
        except Exception as e:
            self.m_logger.warning('Error selecting from TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor.query))

        for l_src, l_width, l_height, l_internal in l_cursor:
            self.m_logger.info('Src: {0}'.format(l_src))

            l_match = re.search(r'/([^/]+_[no]\.(png|jpg|jpeg|gif|svg|PNG|JPG|JPEG|GIF|SVG))', l_src)
            if l_match:
                l_img = l_match.group(1)
                l_fmt = l_match.group(2)
            else:
                l_match = re.search(r'url=([^&]+\.(png|jpg|jpeg|gif|svg|PNG|JPG|JPEG|GIF|SVG))[&%]', l_src)
                if l_match:
                    l_img = (urllib.parse.unquote(l_match.group(1))).split('/')[-1]
                    l_fmt = l_match.group(2)
                else:
                    self.m_logger.warning('Image not found in:' + l_src)
                    l_img = '__RBSFB_IMG__{0}'.format(l_internal)
                    l_fmt = ''

            l_fmt = l_fmt.lower()
            l_fmt = 'jpeg' if l_fmt == 'jpg' else l_fmt

            if len(l_img) > 200:
                l_img = l_img[-200:]

            l_attempts = 0
            l_error = False
            l_image_txt = None
            while True:
                l_attempts += 1
                if l_attempts > 10:
                    if self.m_background_task.internet_check():
                        l_msg = 'Cannot download image [{0}] Too many failed attempts'.format(l_img)
                        self.m_logger.warning(l_msg)
                        # raise BulkDownloaderException(l_msg)
                    else:
                        self.m_logger.info('Internet Down. Waiting ...')
                        time.sleep(5 * 60)
                        l_attempts = 0

                try:
                    l_img_content = Image.open(io.BytesIO(urllib.request.urlopen(l_src, timeout=20).read()))
                    if l_img_content.mode != 'RGB':
                        l_img_content = l_img_content.convert('RGB')

                    if len(l_fmt) == 0:
                        l_fmt = l_img_content.format
                        l_img += '.' + l_fmt

                    self.m_logger.info('   -->: [{0}] {1}'.format(l_fmt, l_img))

                    # l_img_content.save(os.path.join('./images_fb', l_img))

                    l_outputBuffer = io.BytesIO()
                    l_img_content.save(l_outputBuffer, format=l_fmt)
                    l_image_txt = base64.b64encode(l_outputBuffer.getvalue()).decode()
                    self.m_logger.info('[{0}] {1}'.format(len(l_image_txt), l_image_txt[:100]))
                    break
                except urllib.error.URLError as e:
                    if re.search(r'HTTPError 404', repr(e)):
                        self.m_logger.warning('Trapped urllib.error.URLError/HTTPError 404: ' + repr(e))
                        l_image_txt = repr(e)
                        l_error = True
                        break
                    else:
                        self.m_logger.info('Trapped urllib.error.URLError: ' + repr(e))
                        continue
                except socket.timeout as e:
                    self.m_logger.info('Trapped socket.timeout: ' + repr(e))
                    continue
                except TypeError as e:
                    self.m_logger.warning('Trapped TypeError (probably pillow  UserWarning: Couldn\'t ' +
                                          ' allocate palette entry for transparency): ' + repr(e))
                    l_image_txt = repr(e)
                    l_error = True
                    break
                except KeyError as e:
                    self.m_logger.warning('Error downloading image: {0}'.format(repr(e)))
                    l_error = True
                    break
                except Exception as e:
                    self.m_logger.warning('Error downloading image: {0}'.format(repr(e)))
                    raise

            l_conn_write = self.m_pool.getconn('BulkDownloader.fetch_images()')
            l_cursor_write = l_conn_write.cursor()

            try:
                l_cursor_write.execute("""
                    update "TB_MEDIA"
                    set "F_LOADED" = true, "TX_BASE64" = %s, "F_ERROR" = %s
                    where "ID_MEDIA_INTERNAL" = %s;
                """, (l_image_txt, l_error, l_internal))
                l_conn_write.commit()
            except Exception as e:
                self.m_logger.warning('Error updating TB_MEDIA: {0}'.format(repr(e)))
                l_conn_write.rollback()

            self.m_logger.info('Fetched image for internal ID: {0}'.format(l_internal))

            l_cursor_write.close()
            self.m_pool.putconn(l_conn_write)
            # end if l_img is not None:

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.info('End fetch_images()')

    def repeat_ocr_image(self):
        """
        Calls :any:`BulkDownloader.ocr_images()` repeatedly, with a 30 second delay between calls. Meant to be 
        the image OCR thread initiated in :any:`BulkDownloader.bulk_download()`. Forever loop. Thread dies with the 
        application.
        
        :return: Nothing
        """
        self.m_logger.info('Start repeat_ocr_image()')
        while self.m_threads_proceed:
            self.ocr_images()
            time.sleep(30)

    def ocr_images(self):
        """
        Takes a block of (normally 500) `TB_MEDIA` records with downloaded images and attempts OCR. Stores the results
        in appropriate fields in `TB_MEDIA`
        
        :return: 
        """
        self.m_logger.info('Start ocr_images()')

        # SQL Offset
        l_offset = 0
        l_max_img_count = 500
        l_threshold = 180
        l_order_range = 1
        v = [.75, 1.5]
        # v = [.75, 1.25, 1.5, 2.0]
        l_enlarge_factor = 2
        l_width = 6
        l_clip = 2
        l_img_path = './images_ocr'
        l_debug_messages = False

        l_conn = self.m_pool.getconn('BulkDownloader.fetch_images()')
        l_cursor = l_conn.cursor()
        try:
            l_cursor.execute("""
                select "ID_MEDIA_INTERNAL", "TX_BASE64" 
                from "TB_MEDIA"
                where "F_LOADED" and not "F_ERROR" and not "F_OCR"
                offset %s 
                limit %s;
            """, (l_offset, l_max_img_count))
        except Exception as e:
            self.m_logger.warning('Error selecting from TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor.query))

        l_img_count = 0
        l_suf_score = dict()
        for l_internal, l_base64 in l_cursor:
            if l_debug_messages:
                print('+++++++++++[{0}]++++++++++++++++++++++++++++++++++++++++++++++++++++++'.format(l_img_count))
            else:
                os.system('rm -f {0}/*'.format(l_img_path))

            l_fileList = []

            def add_image(p_image, p_suffix):
                l_file = os.path.join(l_img_path, 'img{0:03}_{1}.png'.format(l_img_count, p_suffix))
                p_image.save(l_file)
                l_fileList.append(l_file)
                return p_image

            #self.m_logger.info('Src: {0}'.format(l_src))
            l_raw = Image.open(io.BytesIO(base64.b64decode(l_base64)))
            if l_raw.mode != 'RGB':
                l_raw = l_raw.convert('RGB')

            l_base = add_image(
                l_raw.resize((int(l_raw.width*l_enlarge_factor), int(l_raw.height*l_enlarge_factor))), 'base')
            l_bw = add_image(ImageEnhance.Color(l_base).enhance(0.0), 'bw')

            for l_order in range(l_order_range):
                for c1 in range(len(v)):
                    for c2 in range(len(v)):
                        p1 = v[c1]
                        p2 = v[c2]

                        if p1 < 1.0 and p2 < 1.0:
                            continue

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

                        l_img_s3 = add_image(l_img_s2.filter(ImageFilter.MedianFilter()),
                                             'd{0}{1}{2}'.format(l_order, c1, c2))

                        add_image(l_img_s2.convert('L').point(lambda x: 0 if x<l_threshold else 255, '1'),
                                  'thr{0}{1}{2}'.format(l_order, c1, c2))
                        add_image(l_img_s2.convert('L').point(lambda x: 255 if x<l_threshold else 0, '1'),
                                  'inv{0}{1}{2}'.format(l_order, c1, c2))

                        add_image(l_img_s3.convert('L').point(lambda x: 0 if x < l_threshold else 255, '1'),
                                  'dthr{0}{1}{2}'.format(l_order, c1, c2))
                        add_image(l_img_s3.convert('L').point(lambda x: 255 if x < l_threshold else 0, '1'),
                                  'dinv{0}{1}{2}'.format(l_order, c1, c2))
            # end for l_order in range(l_order_range):

            def get_resultList(p_fileList, p_api, p_lang):
                if l_debug_messages:
                    print('get_resultList() p_lang: ' + p_lang)
                l_result_list = []
                l_max_avg = 0
                l_max_dict_ratio = 0
                for l_file in p_fileList:
                    if l_debug_messages:
                        print(l_file)
                    p_api.SetImageFile(l_file)

                    l_txt = re.sub(r'\s+', r' ', p_api.GetUTF8Text()).strip()
                    if len(l_txt) > 10:
                        ri = p_api.GetIterator()
                        l_more_3 = []
                        l_raw_list = []
                        l_list = []
                        while True:
                            try:
                                l_word = re.sub('\s+', ' ', ri.GetUTF8Text(RIL.WORD)).strip()
                                l_conf = ri.Confidence(RIL.WORD)
                                l_dict = ri.WordIsFromDictionary()

                                l_list_char = []
                                for c in list(l_word):
                                    try:
                                        l_list_char.append(self.m_lig_dict[c])
                                    except KeyError:
                                        l_list_char.append(c)
                                l_word = ''.join(l_list_char)

                                l_full_alpha = re.match(r'(^[a-zA-Z]+[\'’][a-zA-Z]+|[a-zA-Z]+)[\.,;:\?!]*$', l_word)
                                # l_full_alpha = False
                                # l_match = re.search(r'([a-zA-Z]+[\'’][a-zA-Z]+|[a-zA-Z]+)[\.,;:\?!]*', l_word)
                                # if l_match:
                                #     l_full_alpha = (l_match.group(0) == l_word)

                                l_full_num = re.match(r'(^[0-9]+[:,\.][0-9]+|[0-9]+)[\.,;:\?!]*$', l_word)
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
                                        l_word,
                                        p_lang))

                                l_raw_list.append((l_word, int(l_conf), l_dict))
                                if (l_full_num or l_full_alpha) and len(l_word) > 0:
                                    l_list.append((l_word, int(l_conf), l_dict))
                                    if len(l_word) > 2:
                                        l_more_3.append(l_dict)

                            except Exception as e:
                                if l_debug_messages:
                                    print(repr(e))
                                break
                            if not ri.Next(RIL.WORD):
                                break

                        if len(l_list) <= 3:
                            continue

                        l_conf_list = [l[1] for l in l_list]
                        l_avg = sum(l_conf_list) / float(len(l_conf_list))

                        if len(l_more_3) > 0:
                            l_dict_ratio = sum([1 if l else 0 for l in l_more_3])/float(len(l_more_3))
                        else:
                            l_dict_ratio = 0.0

                        if l_debug_messages:
                            print('Average Confidence : {0:.2f}'.format(l_avg))
                            print('Dictionary ratio   : {0:.2f}'.format(l_dict_ratio))
                        if l_avg < 75.0:
                            continue

                        l_txt = ' '.join([l[0] for l in l_list])
                        l_result_list.append((l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list))

                        if l_avg > l_max_avg:
                            l_max_avg = l_avg
                        if l_dict_ratio > l_max_dict_ratio:
                            l_max_dict_ratio = l_dict_ratio

                if len(l_result_list) > 0:
                    l_avg_dict_ratio = sum([l[1] for l in l_result_list])/float(len(l_result_list))
                else:
                    l_avg_dict_ratio = 0.0
                if l_debug_messages:
                    print(
                        ('[{3}] {0} results, max avg: {1:.2f}, max dict. ' +
                         'ratio {2:.2f}, avg. dict. ratio {4:.2f}').format(
                        len(l_result_list), l_max_avg, l_max_dict_ratio, p_lang, l_avg_dict_ratio))
                return l_result_list, l_max_avg, l_max_dict_ratio, l_avg_dict_ratio
            # end def get_resultList(p_fileList, p_api, p_lang):

            def display_results(p_result_list, p_lang):
                if l_debug_messages:
                    print('-----[{0} / {1}]----------------------------------------------'.format(l_img_count, p_lang))
                p_result_list.sort(key=lambda l_tuple: l_tuple[0])
                for l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list in p_result_list:
                    l_file = re.sub(r'{0}/img\d+_'.format(l_img_path), '', l_file)
                    l_file = re.sub(r'\.png', '', l_file)
                    if l_debug_messages:
                        print('{1:.2f} "{2}" [{0}]'.format(l_file, l_avg, l_txt))
                for l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list in p_result_list:
                    l_file = re.sub(r'{0}/img\d+_'.format(l_img_path), '', l_file)
                    l_file = re.sub(r'\.png', '', l_file)
                    if l_debug_messages:
                        print('{1:.2f} "{2}" [{0}]'.format(l_file, l_avg, l_txt))
                        print('     {0}'.format(l_list))
                        print('     {0}'.format(l_raw_list))

            # OCR - eng
            with PyTessBaseAPI(lang='eng') as l_api_eng:
                l_result_list_eng, l_max_avg_eng, l_max_dict_ratio_eng, l_avg_dict_ratio_eng = \
                    get_resultList(l_fileList, l_api_eng, 'eng')
                if len(l_result_list_eng) > 0:
                    display_results(l_result_list_eng, 'eng')

            # OCR - joh
            with PyTessBaseAPI(lang='joh') as l_api_joh:
                l_result_list_joh, l_max_avg_joh, l_max_dict_ratio_joh, l_avg_dict_ratio_joh = \
                    get_resultList(l_fileList, l_api_joh, 'joh')
                if len(l_result_list_joh) > 0:
                    display_results(l_result_list_joh, 'joh')

            def select_final_version(p_result_list):
                l_txt = ''
                l_vocabulary = []

                l_min_select = len(p_result_list) -1 -l_clip -l_width
                l_max_select = len(p_result_list) -1 -l_clip

                if l_min_select < 0:
                    l_min_select = 0
                if l_max_select < 0:
                    l_max_select = len(p_result_list) -1

                # calculate max length of result list (within selection bracket)
                l_max_len = 0
                for i in range(l_min_select, l_max_select+1):
                    l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list = p_result_list[i]
                    if len(l_list) > l_max_len:
                        l_max_len = len(l_list)

                    l_file = re.sub(r'images_ocr/img\d+_', '', l_file)
                    l_suf = re.sub(r'\.png', '', l_file)

                    if len(p_result_list) > l_clip + l_width:
                        if l_suf in l_suf_score.keys():
                            l_suf_score[l_suf] += 1
                        else:
                            l_suf_score[l_suf] = 1

                    for l_word, _, _ in l_list:
                        l_word = re.sub('[\.,;:\?!]*$', '', l_word)
                        if l_word not in l_vocabulary:
                            l_vocabulary.append(l_word)

                # select the longest (within selection bracket)
                for i in range(l_min_select, l_max_select+1):
                    l_avg, l_dict_ratio, l_txt, l_file, l_list, l_raw_list = p_result_list[i]
                    if len(l_list) == l_max_len:
                        break

                # print('l_vocabulary:', l_vocabulary, file=sys.stderr)
                return l_txt, l_vocabulary
            # end def select_final_version(p_result_list):

            l_text = ''
            if l_debug_messages:
                print('======[{0}]==================================================='.format(l_img_count))
            if l_max_avg_eng < l_max_avg_joh and l_avg_dict_ratio_eng < l_avg_dict_ratio_joh:
                l_text, l_vocabulary = select_final_version(l_result_list_joh)
                if l_debug_messages:
                    print('RESULT (joh):', l_text)
                    print('[{0}] RESULT (joh):'.format(l_img_count), l_text, file=sys.stderr)
            elif l_max_avg_joh < l_max_avg_eng and l_avg_dict_ratio_joh < l_avg_dict_ratio_eng:
                l_text, l_vocabulary = select_final_version(l_result_list_eng)
                if l_debug_messages:
                    print('RESULT (eng):', l_text)
                    print('[{0}] RESULT (eng):'.format(l_img_count), l_text, file=sys.stderr)
            else:
                l_txt_eng, l_vocabulary_eng = select_final_version(l_result_list_eng)
                l_txt_joh, l_vocabulary_joh = select_final_version(l_result_list_joh)

                # merge vocabularies
                l_vocabulary = l_vocabulary_eng
                for l_word in l_vocabulary_joh:
                    if l_word not in l_vocabulary:
                        l_vocabulary.append(l_word)

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

            if len(l_vocabulary) > 0:
                if l_debug_messages:
                    print('VOCABULARY:', ' '.join(l_vocabulary))
                    print('[{0}] VOCABULARY:'.format(l_img_count), ' '.join(l_vocabulary), file=sys.stderr)

                l_conn_write = self.m_pool.getconn('BulkDownloader.fetch_images() UPDATE')
                l_cursor_write = l_conn_write.cursor()

                self.m_logger.info('OCR complete on [{0}]: {1}'.format(l_internal, l_text))
                try:
                    l_cursor_write.execute("""
                        update "TB_MEDIA"
                        set 
                            "F_OCR" = true
                            , "TX_TEXT" = %s
                            , "TX_VOCABULARY" = %s
                        where "ID_MEDIA_INTERNAL" = %s
                    """, (l_text, ' '.join(l_vocabulary), l_internal))
                    l_conn_write.commit()
                except Exception as e:
                    l_conn_write.rollback()
                    self.m_logger.warning('Error updating TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor_write.query))

                l_cursor_write.close()
                self.m_pool.putconn(l_conn_write)
            else:
                l_conn_write = self.m_pool.getconn('BulkDownloader.fetch_images() UPDATE')
                l_cursor_write = l_conn_write.cursor()

                try:
                    l_cursor_write.execute("""
                        update "TB_MEDIA"
                        set 
                            "F_OCR" = true
                        where "ID_MEDIA_INTERNAL" = %s
                    """, (l_internal, ))
                    l_conn_write.commit()
                except Exception as e:
                    l_conn_write.rollback()
                    self.m_logger.warning('Error updating TB_MEDIA: {0}/{1}'.format(repr(e), l_cursor_write.query))

                l_cursor_write.close()
                self.m_pool.putconn(l_conn_write)

            l_img_count += 1
            if l_img_count == l_max_img_count:
                break
        # end for l_internal, l_base64 in l_cursor:

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        # final results
        if l_debug_messages:
            print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
            l_suf_list = list(l_suf_score.items())
            l_suf_list.sort(key=lambda l_tuple: l_tuple[1])
            print(l_suf_list)
            for l_suf, l_count in l_suf_list:
                print('{0:4} {1}'.format(l_count, l_suf))

        self.m_logger.info('End ocr_images()')

    def performRequest(self, p_request):
        """
        Calls Facebook's HTTP API and traps errors if any.
          
        :param p_request: The API request 
        :return: The response to the request from the FB API server.
        """
        self.m_logger.info('Start performRequest() Cycle: {0}'.format(
            self.m_FBRequestCount % EcAppParam.gcm_token_lifespan))

        l_request = p_request

        l_finished = False
        l_response = None

        # print('g_FBRequestCount:', g_FBRequestCount)

        # replace access token with the latest (this is necessary because
        # some old tokens may remain in the 'next' parameters kept from previous requests)
        # l_request = self.m_browserDriver.freshen_token(l_request)

        # request new token every G_TOKEN_LIFESPAN API requests, or when token is stale
        # if (self.m_FBRequestCount > 0 and self.m_FBRequestCount % EcAppParam.gcm_token_lifespan == 0)\
        #         or self.m_browserDriver.token_is_stale():
        #     l_request = self.m_browserDriver.renew_token_and_request(l_request)

        self.m_FBRequestCount += 1

        l_errCount = 0
        l_expiry_tries = 0
        while not l_finished:
            try:
                l_response = urllib.request.urlopen(l_request, timeout=20).read().decode('utf-8').strip()
                # self.m_logger.info('l_response: {0}'.format(l_response))
                l_finished = True

            except urllib.error.HTTPError as e:
                l_headersDict = dict(e.headers.items())

                self.m_logger.warning(
                    ('HTTPError (Non Critical)\n{0} {1}\n{2} {3}\n{4} {5}\n{6} {7}' +
                     '\n{8} {9}\n{10} {11}\n{12} {13}').format(
                        'l_errCount     :', l_errCount,
                        'Request Problem:', repr(e),
                        '   Code        :', e.code,
                        '   Errno       :', e.errno,
                        '   Headers     :', l_headersDict,
                        '   Message     :', e.msg,
                        'p_request      :', p_request
                    )
                )

                # Facebook error
                if 'WWW-Authenticate' in l_headersDict.keys():
                    l_FBMessage = l_headersDict['WWW-Authenticate']

                    # Request limit reached --> wait G_WAIT_FB s and retry
                    if re.search(r'\(#17\) User request limit reached', l_FBMessage):
                        l_wait = EcAppParam.gcm_wait_fb
                        self.m_logger.warning('FB request limit msg: {0} --> Waiting for {1} seconds'.format(
                            l_FBMessage, l_wait))

                        l_sleepPeriod = 5 * 60
                        for i in range(int(l_wait / l_sleepPeriod)):
                            time.sleep(l_sleepPeriod)
                            # l_request = self.m_browserDriver.renew_token_and_request(l_request)

                    # Unknown FB error --> wait 10 s and retry 3 times max then return empty result
                    if re.search(r'An unexpected error has occurred', l_FBMessage) \
                            or re.search(r'An unknown error has occurred', l_FBMessage):
                        if l_errCount < 3:
                            l_wait = 10
                            self.m_logger.warning('FB unknown error: {0} --> Waiting for {1} seconds'.format(
                                l_FBMessage,l_wait))

                            time.sleep(l_wait)
                            # l_request = self.m_browserDriver.renew_token_and_request(l_request)
                        else:
                            l_response = '{"data": []}'

                            self.m_logger.critical('FB unknown error: {0} --> Returned: {1}\n'.format(
                                l_FBMessage, l_response))

                            l_finished = True

                    # Session expired ---> nothing to do
                    elif re.search(r'Session has expired', l_FBMessage):
                        if l_expiry_tries < 3:
                            # l_request = self.m_browserDriver.renew_token_and_request(l_request)
                            l_expiry_tries += 1
                        else:
                            l_msg = 'FB session expiry msg: {0}'.format(l_FBMessage)
                            self.m_logger.critical(l_msg)
                            raise BulkDownloaderException(l_msg)

                    # Unsupported get request ---> return empty data and abandon request attempt
                    elif re.search(r'Unsupported get request', l_FBMessage):
                        l_response = '{"data": []}'

                        self.m_logger.warning('FB unsupported get msg: {0} --> Returned: {1}'.format(
                            l_FBMessage, l_response))

                        l_finished = True

                    # Other FB error
                    else:
                        self.m_logger.critical('FB msg: {0}'.format(l_FBMessage))
                        raise BulkDownloaderException('FB msg: {0}'.format(l_FBMessage))

                # Non FB HTTPError
                else:
                    l_wait = self.getWait(l_errCount)
                    self.m_logger.warning('Non FB HTTPError {0} --> Waiting for {1} seconds'.format(
                        repr(e), l_wait))

                    time.sleep(l_wait)
                    # if l_wait > 60 * 15:
                    #     l_request = self.m_browserDriver.renew_token_and_request(l_request)

                l_errCount += 1

            except urllib.error.URLError as e:
                self.m_logger.warning('URLError (Non Critical)\n{0} {1}\n{2} {3}\n{4} {5}\n{6} {7}\n{8} {9}'.format(
                    'l_errCount     :', l_errCount,
                    'Request Problem:', repr(e),
                    '   Errno       :', e.errno,
                    '   Message     :', e.reason,
                    'p_request      :', p_request
                ))

                time.sleep(1)
                l_errCount += 1

            except Exception as e:
                self.m_logger.warning('Unknown Error\n{0} {1}\n{2} {3}\n{4} {5}\n{6} {7}'.format(
                    'l_errCount     :', l_errCount,
                    'Request Problem:', repr(e),
                    '   Message     :', e.args,
                    'p_request      :', p_request
                ))

                time.sleep(1)
                l_errCount += 1

        self.m_logger.debug('End performRequest()  Cycle: {0}'.format(
            self.m_FBRequestCount % EcAppParam.gcm_token_lifespan))
        return l_response

    def getWait(self, p_errorCount):
        """
        Selects the appropriate wait-time depending on the number of accumulated errors 
        (for :any:`BulkDownloader.performRequest`)
        
        :param p_errorCount: Number of errors so far. 
        :return: The wait delay in seconds.
        """
        if p_errorCount < 3:
            return 5
        elif p_errorCount < 6:
            return 30
        elif p_errorCount < 9:
            return 60 * 2
        elif p_errorCount < 12:
            return 60 * 5
        elif p_errorCount < 15:
            return 60 * 15
        elif p_errorCount < 18:
            return 60 * 30
        elif p_errorCount < 21:
            return 60 * 60
        else:
            self.m_logger.critical('Too many errors: {0}'.format(p_errorCount))
            raise BulkDownloaderException('Too many errors: {0}'.format(p_errorCount))

    def storeObject(self,
                    p_padding,
                    p_type,
                    p_date_creation,
                    p_date_modification,
                    p_id,
                    p_parentId,
                    p_pageId,
                    p_postId,
                    p_fb_type,
                    p_fb_status_type,
                    p_shareCount,
                    p_likeCount,
                    p_permalink_url,
                    p_name='',
                    p_caption='',
                    p_desc='',
                    p_story='',
                    p_message='',
                    p_link='',
                    p_picture='',
                    p_place='',
                    p_source='',
                    p_userId='',
                    p_tags='',
                    p_with_tags='',
                    p_properties=''):

        """
        DB storage of a new object.
        
        :param p_padding: 
        :param p_type: 
        :param p_date_creation: 
        :param p_date_modification: 
        :param p_id: 
        :param p_parentId: 
        :param p_pageId: 
        :param p_postId: 
        :param p_fb_type: 
        :param p_fb_status_type: 
        :param p_shareCount: 
        :param p_likeCount: 
        :param p_permalink_url: 
        :param p_name: 
        :param p_caption: 
        :param p_desc: 
        :param p_story: 
        :param p_message: 
        :param p_link: 
        :param p_picture: 
        :param p_place: 
        :param p_source: 
        :param p_userId: 
        :param p_tags: 
        :param p_with_tags: 
        :param p_properties: 
        :return: `True` if insertion occurred
        """
        self.m_logger.debug('Start storeObject()')

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

        l_conn = self.m_pool.getconn('BulkDownloader.storeObject()')
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
                    ,"TX_PROPERTIES")
                VALUES(
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s, 
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s) 
            """, (
                p_id,
                p_parentId,
                p_pageId,
                p_postId,
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
                p_userId,
                p_likeCount,
                p_shareCount,
                p_place,
                p_tags,
                p_with_tags,
                p_properties
                )
            )

            self.m_objectStored += 1
            l_conn.commit()
            l_stored = True
        except psycopg2.IntegrityError as e:
            self.m_logger.info('{0}Object already in TB_OBJ [{1}]'.format(p_padding, repr(e)))
            l_conn.rollback()
        except Exception as e:
            self.m_logger.warning('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.info(
            '{0}Object counts: {1} attempts / {2} stored / {3} posts retrieved / {4} comments retrieved'.format(
            p_padding, self.m_objectStoreAttempts, self.m_objectStored, self.m_postRetrieved, self.m_commentRetrieved))

        self.m_logger.debug('End storeObject()')
        return l_stored

    def updateObject(self, p_id, p_shareCount, p_likeCount, p_name, p_caption, p_desc, p_story, p_message):
        """
        Update of an existing object.
        
        :param p_id: 
        :param p_shareCount: 
        :param p_likeCount: 
        :param p_name: 
        :param p_caption: 
        :param p_desc: 
        :param p_story: 
        :param p_message: 
        :return: `True` if update completed.
        """
        self.m_logger.debug('Start updateObject()')
        l_stored = False

        l_conn = self.m_pool.getconn('BulkDownloader.updateObject()')
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
            """, (p_likeCount, p_shareCount, p_name, p_caption, p_desc, p_story, p_message, p_id))
            l_conn.commit()
            l_stored = True
        except psycopg2.IntegrityError as e:
            self.m_logger.warning('Object Cannot be updated: {0}/{1}'.format(repr(e), l_cursor.query))
            l_conn.rollback()
        except Exception as e:
            self.m_logger.critical('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.debug('End updateObject()')
        return l_stored

    def storeUser(self, p_id, p_name, p_date, p_padding):
        """
        DB Storage of a new user. If user already in the BD, traps the integrity violation error and returns `False`.
        
        :param p_id: User ID (API App. specific)
        :param p_name: User Name
        :param p_date: Date of the object in which user first appeared.
        :param p_padding: Debug/Info massage left padding.
        :return: `True` if insertion occurred.
        """
        self.m_logger.debug('Start storeUser()')
        # date format: 2016-04-22T12:03:06+0000 ---> 2016-04-22 12:03:06
        l_date = re.sub('T', ' ', p_date)
        l_date = re.sub(r'\+\d+$', '', l_date)

        if len(l_date) == 0:
            l_date = datetime.datetime.now()
        else:
            l_date = datetime.datetime.strptime(l_date, '%Y-%m-%d %H:%M:%S')

        l_conn = self.m_pool.getconn('BulkDownloader.storeUser()')
        l_cursor = l_conn.cursor()

        l_inserted = False
        try:
            l_cursor.execute("""
                INSERT INTO "TB_USER"("ID", "ST_NAME", "DT_CRE", "DT_MSG")
                VALUES( %s, %s, %s, %s )
            """, (
                p_id,
                p_name,
                datetime.datetime.now(),
                l_date
            ))
            l_conn.commit()
            l_inserted = True
        except psycopg2.IntegrityError as e:
            self.m_logger.debug('{0}User already known: [{1}]'.format(p_padding, repr(e)))
            # print('{0}PostgreSQL: {1}'.format(p_padding, e))
            l_conn.rollback()
        except Exception as e:
            self.m_logger.critical('TB_USER Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_USER Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.debug('End storeUser()')
        return l_inserted

    def getUserInternalId(self, p_id):
        """
        Looks up the internal ID of an user based on its API App-specific ID.
        
        :param p_id: API App-specific ID.
        :return: Internal ID
        """
        self.m_logger.debug('Start getUserInternalId()')
        l_conn = self.m_pool.getconn('BulkDownloader.getUserInternalId()')
        l_cursor = l_conn.cursor()

        l_retId = None
        try:
            l_cursor.execute("""
                select "ID_INTERNAL"
                from "TB_USER"
                where "ID" = %s
            """, (p_id, ))

            for l_internalId, in l_cursor:
                l_retId = l_internalId

        except Exception as e:
            self.m_logger.critical('TB_USER Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_USER Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.debug('End getUserInternalId()')
        return l_retId

    def createLikeLink(self, p_userIdInternal, p_objIdInternal, p_date):
        """
        DB storage of a link between a liked object and the author of the like.
        
        :param p_userIdInternal: Internal ID of the user. 
        :param p_objIdInternal:  Internal ID of the object.
        :param p_date: Date the like was placed.
        :return: `True` if insertion occurred.
        """
        self.m_logger.debug('Start createLikeLink()')
        # date format: 2016-04-22T12:03:06+0000 ---> 2016-04-22 12:03:06
        l_date = re.sub('T', ' ', p_date)
        l_date = re.sub(r'\+\d+$', '', l_date)

        l_conn = self.m_pool.getconn('BulkDownloader.creatLikeLink()')
        l_cursor = l_conn.cursor()

        l_inserted = False
        try:
            l_cursor.execute("""
                INSERT INTO "TB_LIKE"("ID_USER_INTERNAL","ID_OBJ_INTERNAL","DT_CRE")
                VALUES( %s, %s, %s )
            """, (p_userIdInternal, p_objIdInternal, l_date))
            l_conn.commit()
            l_inserted = True
        except psycopg2.IntegrityError:
            l_conn.rollback()
            if EcAppParam.gcm_verboseModeOn:
                self.m_logger.info('Like link already exists')
        except Exception as e:
            self.m_logger.critical('TB_LIKE Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_LIKE Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.debug('End createLikeLink()')
        return l_inserted

    def setLikeFlag(self, p_id):
        """
        Sets a flag on an object to indicate that the like details have been fetched.
        
        :param p_id: API App-specific ID of the object.
        :return: Nothing 
        """
        self.m_logger.debug('Start setLikeFlag()')
        l_conn = self.m_pool.getconn('BulkDownloader.setLikeFlag()')
        l_cursor = l_conn.cursor()

        try:
            l_cursor.execute("""
                update "TB_OBJ"
                set "F_LIKE_DETAIL" = 'X'
                where "ID" = %s
            """, (p_id,))
            l_conn.commit()
        except Exception as e:
            l_conn.rollback()
            self.m_logger.critical('TB_OBJ Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_OBJ Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.debug('End setLikeFlag()')

    def store_media(
            self, p_id, p_fb_type, p_desc, p_title, p_tags, p_target, p_media, p_media_src, p_width, p_height):
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
        :return: Nothing (the insertion will always succeed except for technical malfunction)
        """
        self.m_logger.debug('Start store_media()')

        l_conn = self.m_pool.getconn('BulkDownloader.storeUser()')
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
                )
                VALUES( 
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s)
            """, (p_id, p_fb_type, p_desc, p_title, p_tags, p_target, p_media, p_media_src, p_width, p_height))
            l_conn.commit()
        except Exception as e:
            self.m_logger.warning('TB_MEDIA Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('TB_MEDIA Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.debug('Start store_media()')

    @classmethod
    def getOptionalField(self, p_json, p_field):
        """
        Macro to get a field from an API response that may or may not be present.
        
        :param p_json: The API response JSON fragment 
        :param p_field: The requested field
        :return: The field contents if present (full + shortened to 100 char). Empty strings otherwise.
        """
        l_value = ''
        l_valueShort = ''

        if p_field in p_json.keys():
            l_value = re.sub('\s+', ' ', p_json[p_field]).strip()
            if len(l_value) > 100:
                l_valueShort = l_value[0:100] + ' ... ({0})'.format(len(l_value))
            else:
                l_valueShort = l_value

        return l_value, l_valueShort

    def getPostsFromPage(self, p_id):
        """
        Gets all new posts from a page and store them in the DB.
        
        :param p_id: ID (API App-specific) of the page to get the posts from 
        :return: Nothing
        """
        self.m_logger.info('Start getPostsFromPage()')

        l_fieldList = 'id,caption,created_time,description,from,icon,link,message,message_tags,name,object_id,' + \
                      'permalink_url,picture,place,properties,shares,source,status_type,story,to,type,' + \
                      'updated_time,with_tags'

        l_request = ('https://graph.facebook.com/{0}/{1}/feed?limit={2}&' +
                     'access_token={3}&fields={4}').format(
            EcAppParam.gcm_api_version, p_id, EcAppParam.gcm_limit, self.m_long_token, l_fieldList)

        l_response = self.performRequest(l_request)
        l_responseData = json.loads(l_response)

        if len(l_responseData['data']) > 0:
            self.m_logger.info('   Latest date:' + l_responseData['data'][0]['created_time'])

        l_postCount = 0
        l_finished = False
        while not l_finished:
            for l_post in l_responseData['data']:
                self.m_postRetrieved += 1

                l_postId = l_post['id']
                l_postDate = l_post['created_time']
                l_type = l_post['type']
                l_shares = int(l_post['shares']['count']) if 'shares' in l_post.keys() else 0
                self.m_logger.info(
                    '   =====[ {0}/{1} ]================POST========================='.format(
                        l_postCount, self.m_page))
                self.m_logger.info('   id          : ' + l_postId)
                self.m_logger.info('   date        : ' + l_postDate)

                # 2016-04-22T12:03:06+0000
                l_msgDate = datetime.datetime.strptime(
                    re.sub(r'\+\d+$', '', l_postDate), '%Y-%m-%dT%H:%M:%S')
                self.m_logger.info('   date (P)    : {0}'.format(l_msgDate))

                # if message older than G_DAYS_DEPTH days ---> break loop
                l_days_old = (datetime.datetime.now() - l_msgDate).days
                self.m_logger.info('   Days old    : {0}'.format(l_days_old))
                if l_days_old > EcAppParam.gcm_days_depth:
                    self.m_logger.info('   ---> Too old, stop this page')
                    l_finished = True
                    break

                l_userId = ''
                if 'from' in l_post.keys():
                    l_userId, l_userIdShort = BulkDownloader.getOptionalField(l_post['from'], 'id')
                    l_userName, l_userNameShort = BulkDownloader.getOptionalField(l_post['from'], 'name')

                    if EcAppParam.gcm_verboseModeOn:
                        self.m_logger.info('   from        : {0} [{1}]'.format(l_userNameShort, l_userId))

                    # store user data
                    self.storeUser(l_userId, l_userName, l_postDate, '   ')

                l_name, l_nameShort = BulkDownloader.getOptionalField(l_post, 'name')
                l_caption, l_captionShort = BulkDownloader.getOptionalField(l_post, 'caption')
                l_description, l_descriptionSh = BulkDownloader.getOptionalField(l_post, 'description')
                l_story, l_storyShort = BulkDownloader.getOptionalField(l_post, 'story')
                l_message, l_messageShort = BulkDownloader.getOptionalField(l_post, 'message')

                l_object_id, x = BulkDownloader.getOptionalField(l_post, 'object_id')
                l_link, x = BulkDownloader.getOptionalField(l_post, 'link')
                l_picture, x = BulkDownloader.getOptionalField(l_post, 'picture')
                l_source, x = BulkDownloader.getOptionalField(l_post, 'source')

                l_icon, x = BulkDownloader.getOptionalField(l_post, 'icon')
                l_permalink_url, x = BulkDownloader.getOptionalField(l_post, 'permalink_url')
                l_status_type, x = BulkDownloader.getOptionalField(l_post, 'status_type')
                l_updated_time, x = BulkDownloader.getOptionalField(l_post, 'updated_time')

                l_place = ''
                if 'place' in l_post.keys():
                    l_place = json.dumps(l_post['place'])

                l_tags = ''
                if 'message_tags' in l_post.keys():
                    l_tags = json.dumps(l_post['message_tags'])

                l_with_tags = ''
                if 'with_tags' in l_post.keys():
                    l_with_tags = json.dumps(l_post['with_tags'])

                l_properties = ''
                if 'properties' in l_post.keys():
                    l_properties = json.dumps(l_post['properties'])

                self.m_logger.info('   name        : ' + l_nameShort)
                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info('   caption     : ' + l_captionShort)
                    self.m_logger.info('   description : ' + l_descriptionSh)
                    self.m_logger.info('   story       : ' + l_storyShort)
                    self.m_logger.info('   message     : ' + l_messageShort)
                    self.m_logger.info('   permalink   : ' + l_permalink_url)
                    self.m_logger.info('   icon        : ' + l_icon)
                    self.m_logger.info('   object_id   : ' + l_object_id)
                    self.m_logger.info('   shares      : {0}'.format(l_shares))
                    self.m_logger.info('   type        : ' + l_type)
                    self.m_logger.info('   updated time: ' + l_updated_time)
                    self.m_logger.info('   with        : {0}'.format(l_with_tags))
                    self.m_logger.info('   tags        : {0}'.format(l_tags))
                    self.m_logger.info('   place       : {0}'.format(l_place))

                # store post information
                if self.storeObject(
                        p_padding='   ',
                        p_type='Post',
                        p_date_creation=l_postDate,
                        p_date_modification=l_updated_time,
                        p_id=l_postId,
                        p_parentId=p_id,
                        p_pageId=p_id,
                        p_postId='',
                        p_fb_type=l_type,
                        p_fb_status_type=l_status_type,
                        p_shareCount=l_shares,
                        p_likeCount=0,
                        p_permalink_url=l_permalink_url,
                        p_name=l_name,
                        p_caption=l_caption,
                        p_desc=l_description,
                        p_story=l_story,
                        p_message=l_message,
                        p_link=l_link,
                        p_picture=l_picture,
                        p_place=l_place,
                        p_source=l_source,
                        p_userId=l_userId,
                        p_tags=l_tags,
                        p_with_tags=l_with_tags,
                        p_properties=l_properties):
                    # get comments
                    self.getPostAttachments(l_postId, l_status_type, l_source, l_link, l_picture, l_properties)
                    self.getComments(l_postId, l_postId, p_id, 0)
                    # time.sleep(5)
                else:
                    # if already in DB ---> break loop
                    self.m_logger.info(
                        '   ---> Post already in DB, stop this page')
                    l_finished = True
                    break

                l_postCount += 1
                if l_postCount > EcAppParam.gcm_max_post:
                    self.m_logger.info(
                        '   ---> Maximum number of posts ({0}) reached, stop this page'.format(l_postCount))
                    l_finished = True
                    break

                # End for l_post in l_responseData['data']:

            if 'paging' in l_responseData.keys() and 'next' in l_responseData['paging'].keys():
                self.m_logger.info('   *** Getting next post block ...')
                l_request = l_responseData['paging']['next']
                l_response = self.performRequest(l_request)

                l_responseData = json.loads(l_response)
            else:
                break
            # end while not l_finished:

        self.m_logger.info('End getPostsFromPage()')

    def getPostAttachments(self, p_id, p_status_type, p_source, p_link, p_picture, p_properties):
        """
        Gets all attachments from a post.
        
        :param p_id: API App-specific ID of the post. 
        :param p_status_type: 
        :param p_source: 
        :param p_link: 
        :param p_picture: 
        :param p_properties: 
        :return: Nothing
        """
        self.m_logger.debug('Start getPostAttachments()')

        # get list of attachments attached to this post
        l_fieldList = 'description,description_tags,media,target,title,type,url,attachments,subattachments'

        l_request = ('https://graph.facebook.com/{0}/{1}/attachments?limit={2}&' +
                     'access_token={3}&fields={4}').format(
            EcAppParam.gcm_api_version, p_id, EcAppParam.gcm_limit, self.m_long_token, l_fieldList)

        l_response = self.performRequest(l_request)
        l_responseData = json.loads(l_response)

        # self.m_logger.info(l_response)

        self.scan_attachments(l_responseData['data'],
                              p_id, p_status_type, p_source, p_link, p_picture, p_properties, 1)
        self.m_logger.info('debug getPostAttachments()')

    def scan_attachments(self, p_attachment_list,
                         p_id, p_status_type, p_source, p_link, p_picture, p_properties, p_depth):
        """
        Scans a JSON response fragment in order to get attachments and (through recursion) sub-attachments, if any.
        
        :param p_attachment_list: 
        :param p_id: 
        :param p_status_type: 
        :param p_source: 
        :param p_link: 
        :param p_picture: 
        :param p_properties: 
        :param p_depth: 
        :return: Nothing 
        """
        self.m_logger.debug('Start scan_attachments()')
        l_depthPadding = ' ' * (p_depth * 3)

        l_attachmentCount = 0
        for l_attachment in p_attachment_list:
            l_description, x = BulkDownloader.getOptionalField(l_attachment, 'description')
            l_title, x = BulkDownloader.getOptionalField(l_attachment, 'title')
            l_type, x = BulkDownloader.getOptionalField(l_attachment, 'type')
            l_url, x = BulkDownloader.getOptionalField(l_attachment, 'url')

            l_description_tags = None
            if 'description_tags' in l_attachment.keys():
                l_description_tags = json.dumps(l_attachment['description_tags'])

            l_src = None
            l_width = None
            l_height = None
            l_media = None
            if 'media' in l_attachment.keys():
                l_media = l_attachment['media']
                # self.m_logger.info('Keys: {0}'.format(list(l_media.keys())))
                if list(l_media.keys()) == ['image']:
                    try:
                        l_src = l_media['image']['src']
                        l_width = int(l_media['image']['width'])
                        l_height = int(l_media['image']['height'])
                    except ValueError:
                        self.m_logger.warning('Cannot convert [{0}] or [{1}]'.format(
                            l_media['image']['width'], l_media['image']['height']))
                    except KeyError:
                        self.m_logger.warning('Missing key in: {0}'.format(l_media['image']))
                l_media = json.dumps(l_attachment['media'])

            l_target = None
            if 'target' in l_attachment.keys():
                l_target = json.dumps(l_attachment['target'])

            self.m_logger.info(
                '{0}++++[ {1}/{2} ]++++++++++++{3}ATTACHMENT++++++++++++++++++++++++'.format(
                    l_depthPadding, l_attachmentCount, self.m_page, 'SUB' if p_depth >= 2 else ''))

            self.m_logger.info('{0}Type        : {1}'.format(l_depthPadding, l_type))
            self.m_logger.info('{0}status type : {1}'.format(l_depthPadding, p_status_type))
            self.m_logger.info('{0}Description : {1}'.format(l_depthPadding, l_description))
            self.m_logger.info('{0}Title       : {1}'.format(l_depthPadding, l_title))
            self.m_logger.info('{0}Tags        : {1}'.format(l_depthPadding, l_description_tags))
            self.m_logger.info('{0}Target      : {1}'.format(l_depthPadding, l_target))
            self.m_logger.info('{0}Url         : {1}'.format(l_depthPadding, l_url))
            self.m_logger.info('{0}link        : {1}'.format(l_depthPadding, p_link))
            self.m_logger.info('{0}Media       : {1}'.format(l_depthPadding, l_media))
            self.m_logger.info('{0}Media/src   : {1}'.format(l_depthPadding, l_src))
            self.m_logger.info('{0}Media/width : {1}'.format(l_depthPadding, l_width))
            self.m_logger.info('{0}Media/height: {1}'.format(l_depthPadding, l_height))
            self.m_logger.info('{0}source      : {1}'.format(l_depthPadding, p_source))
            self.m_logger.info('{0}picture     : {1}'.format(l_depthPadding, p_picture))
            self.m_logger.info('{0}properties  : {1}'.format(l_depthPadding, p_properties))

            self.store_media(p_id, l_type, l_description, l_title, l_description_tags,
                             l_target, l_media, l_src, l_width, l_height)

            if 'subattachments' in l_attachment.keys():
                self.scan_attachments(l_attachment['subattachments']['data'],
                                      p_id, p_status_type, p_source, p_link, p_picture, p_properties, p_depth+1)

            l_attachmentCount += 1
        # end of for l_attachment in p_attachment_list:

        self.m_logger.debug('End scan_attachments()')

    def getComments(self, p_id, p_postId, p_pageId, p_depth):
        """
        Gets comments from a post or another comment.
        
        :param p_id: API App-specific ID of the object or comment.
        :param p_postId: 
        :param p_pageId: 
        :param p_depth: 
        :return: Nothing. 
        """
        self.m_logger.debug('Start scan_attachments()')
        l_depthPadding = ' ' * ((p_depth + 2) * 3)

        # get list of comments attached to this post (or this comment)
        l_fieldList = 'id,attachment,created_time,comment_count,from,like_count,message,'+ \
                      'message_tags,user_likes'

        l_request = ('https://graph.facebook.com/{0}/{1}/comments?limit={2}&' +
                     'access_token={3}&fields={4}').format(
            EcAppParam.gcm_api_version, p_id, EcAppParam.gcm_limit, self.m_long_token, l_fieldList)

        l_response = self.performRequest(l_request)
        l_responseData = json.loads(l_response)

        if len(l_responseData['data']) > 0:
            self.m_logger.info('{0}Latest date: '.format(l_depthPadding) + l_responseData['data'][0]['created_time'])

        l_commCount = 0
        while True:
            for l_comment in l_responseData['data']:
                self.m_commentRetrieved += 1

                l_commentId = l_comment['id']
                l_commentDate = l_comment['created_time']
                l_commentLikes = int(l_comment['like_count'])
                l_commentCCount = int(l_comment['comment_count'])
                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info(
                        '{0}----[{1}]-----------COMMENT------------------------------'.format(
                            l_depthPadding, self.m_page))
                    self.m_logger.info('{0}id      : '.format(l_depthPadding) + l_commentId)
                    self.m_logger.info('{0}date    : '.format(l_depthPadding) + l_commentDate)
                    self.m_logger.info('{0}likes   : {1}'.format(l_depthPadding, l_commentLikes))
                    self.m_logger.info('{0}sub com.: {1}'.format(l_depthPadding, l_commentCCount))

                l_userId = ''
                if 'from' in l_comment.keys():
                    l_userId, l_userIdShort = BulkDownloader.getOptionalField(l_comment['from'], 'id')
                    l_userName, l_userNameShort = BulkDownloader.getOptionalField(l_comment['from'], 'name')

                    if EcAppParam.gcm_verboseModeOn:
                        self.m_logger.info('{0}from    : {1} [{2}]'.format(l_depthPadding, l_userNameShort, l_userId))

                    # store user data
                    self.storeUser(l_userId, l_userName, l_commentDate, l_depthPadding)

                l_message, l_messageShort = BulkDownloader.getOptionalField(l_comment, 'message')

                l_tags = ''
                if 'message_tags' in l_comment.keys():
                    l_tags = json.dumps(l_comment['message_tags'])

                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info('{0}message : '.format(l_depthPadding) + l_messageShort)
                    self.m_logger.info('{0}tags    : '.format(l_depthPadding) + l_tags)

                # store comment information
                if self.storeObject(
                    p_padding=l_depthPadding,
                    p_type='Comm',
                    p_date_creation=l_commentDate,
                    p_date_modification='',
                    p_id=l_commentId,
                    p_parentId=p_id,
                    p_pageId=p_pageId,
                    p_postId=p_postId,
                    p_fb_type='Comment',
                    p_fb_status_type='',
                    p_shareCount=0,
                    p_likeCount=l_commentLikes,
                    p_permalink_url='',
                    p_message=l_message,
                    p_userId=l_userId,
                    p_tags=l_tags,
                ):
                    l_commCount += 1
                    if 'attachment' in l_comment.keys():
                        self.scan_attachments(
                            [l_comment['attachment']], l_commentId, '', '', '', '', '', p_depth + 2)

                # get comments
                if l_commentCCount > 0:
                    self.getComments(l_commentId, p_postId, p_pageId, p_depth + 1)

            if 'paging' in l_responseData.keys() and 'next' in l_responseData['paging'].keys():
                self.m_logger.info('{0}[{1}] *** Getting next comment block ...'.format(l_depthPadding, l_commCount))
                l_request = l_responseData['paging']['next']
                l_response = self.performRequest(l_request)

                l_responseData = json.loads(l_response)
            else:
                break

        self.m_logger.info('[End scan_attachments()] {0}comment download count --> {1}'.format(
            l_depthPadding[:-3], l_commCount))

    def updatePosts(self):
        """
        Update existing posts: Text modifications, new comments, likes count.
        
        :return: Nothing 
        """
        self.m_logger.info('Start updatePosts()')
        l_conn = self.m_pool.getconn('BulkDownloader.updatePosts()')
        l_cursor = l_conn.cursor()

        # All posts not older than G_DAYS_DEPTH days and not already updated in the last day
        # Among these, comments to be downloaded only for those which were not created today
        # ("DT_LAST_UPDATE" not null)
        try:
            l_cursor.execute("""
                select
                    "ID"
                    , "ID_PAGE"
                    , case when "DT_LAST_UPDATE" is null then '' else 'X' end "COMMENT_DOWNLOAD"
                from "TB_OBJ"
                where
                    "ST_TYPE" = 'Post'
                    and DATE_PART('day', now()::date - "DT_CRE") <= %s
                    and (
                        "DT_LAST_UPDATE" is null
                        or DATE_PART('day', now()::date - "DT_LAST_UPDATE") >= 2
                    )
            """, (EcAppParam.gcm_days_depth, ))

            for l_postId, l_pageId, l_commentFlag in l_cursor:
                self.m_postRetrieved += 1
                # get post data
                l_fieldList = 'id,created_time,from,story,message,' + \
                              'caption,description,icon,link,name,object_id,picture,place,shares,source,type'

                l_request = ('https://graph.facebook.com/{0}/{1}?limit={2}&' +
                             'access_token={3}&fields={4}').format(
                    EcAppParam.gcm_api_version,
                    l_postId,
                    EcAppParam.gcm_limit,
                    self.m_long_token,
                    l_fieldList)

                # print('   l_request:', l_request)
                l_response = self.performRequest(l_request)

                l_responseData = json.loads(l_response)
                l_shares = int(l_responseData['shares']['count']) if 'shares' in l_responseData.keys() else 0
                self.m_logger.info('============= UPDATE ==============================================')
                self.m_logger.info('Post ID     : {0}'.format(l_postId))
                if 'created_time' in l_responseData.keys():
                    self.m_logger.info('Post date   : {0}'.format(l_responseData['created_time']))
                    self.m_logger.info('Comm. dnl ? : {0}'.format(l_commentFlag))

                l_name, l_nameShort = BulkDownloader.getOptionalField(l_responseData, 'name')
                l_caption, l_captionShort = BulkDownloader.getOptionalField(l_responseData, 'caption')
                l_description, l_descriptionSh = BulkDownloader.getOptionalField(l_responseData, 'description')
                l_story, l_storyShort = BulkDownloader.getOptionalField(l_responseData, 'story')
                l_message, l_messageShort = BulkDownloader.getOptionalField(l_responseData, 'message')

                self.m_logger.info('name        : {0}'.format(l_nameShort))
                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info('caption     : {0}'.format(l_captionShort))
                    self.m_logger.info('description : {0}'.format(l_descriptionSh))
                    self.m_logger.info('story       : {0}'.format(l_storyShort))
                    self.m_logger.info('message     : {0}'.format(l_messageShort))
                    self.m_logger.info('shares      : {0}'.format(l_shares))

                # get post likes
                l_request = ('https://graph.facebook.com/{0}/{1}/likes?limit={2}&' +
                             'access_token={3}&summary=true').format(
                    EcAppParam.gcm_api_version, l_postId, 25, self.m_long_token, l_fieldList)
                # print('   l_request:', l_request)
                l_response = self.performRequest(l_request)

                l_responseData = json.loads(l_response)
                l_likeCount = 0
                if 'summary' in l_responseData.keys():
                    l_likeCount = int(l_responseData['summary']['total_count'])
                if EcAppParam.gcm_verboseModeOn:
                    self.m_logger.info('likes       : {0}'.format(l_likeCount))

                if self.updateObject(
                        l_postId, l_shares, l_likeCount, l_name, l_caption, l_description, l_story, l_message) \
                        and l_commentFlag == 'X':
                    self.getComments(l_postId, l_postId, l_pageId, 0)

        except Exception as e:
            self.m_logger.critical('Post Update Unknown Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('Post Update Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)
        self.m_logger.info('End updatePosts()')

    def getLikesDetail(self):
        """
        Get the likes details of sufficiently old posts.
        
        :return: Nothing
        """
        self.m_logger.info('Start getLikesDetail()')
        l_conn = self.m_pool.getconn('BulkDownloader.getLikesDetail()')
        l_cursor = l_conn.cursor()

        l_totalCount = 0
        try:
            l_cursor.execute("""
                SELECT
                    count(1) AS "LCOUNT"
                FROM
                    "TB_OBJ"
                WHERE
                    "ST_TYPE" != 'Page'
                    AND DATE_PART('day', now()::date - "DT_CRE") >= %s
                    AND "F_LIKE_DETAIL" is null
            """, (EcAppParam.gcm_likes_depth, ))

            for l_count, in l_cursor:
                l_totalCount = l_count
        except Exception as e:
            self.m_logger.critical('Likes detail download (count) Unknown Exception: {0}/{1}'.format(
                repr(e), l_cursor.query))
            raise BulkDownloaderException('Likes detail download (count) Unknown Exception: {0}'.format(repr(e)))

        l_cursor.close()
        l_cursor = l_conn.cursor()

        # all non page objects older than G_LIKES_DEPTH days and not already processed
        l_objCount = 0
        try:
            l_cursor.execute("""
                SELECT
                    "ID", "ID_INTERNAL", "DT_CRE"
                FROM
                    "TB_OBJ"
                WHERE
                    "ST_TYPE" != 'Page'
                    AND DATE_PART('day', now()::date - "DT_CRE") >= %s
                    AND "F_LIKE_DETAIL" is null
            """, (EcAppParam.gcm_likes_depth, ))

            for l_id, l_internalId, l_dtMsg in l_cursor:
                print('{0}/{1}'.format(l_objCount, l_totalCount), l_id, '--->')
                # get likes data

                l_request = ('https://graph.facebook.com/{0}/{1}/likes?limit={2}&' +
                             'access_token={3}').format(
                    EcAppParam.gcm_api_version, l_id, EcAppParam.gcm_limit, self.m_long_token)
                l_response = self.performRequest(l_request)

                l_responseData = json.loads(l_response)
                l_likeCount = 0
                while True:
                    for l_liker in l_responseData['data']:
                        try:
                            l_likerId = l_liker['id']
                        except KeyError:
                            self.m_logger.warning('No Id found in Liker: {0}'.format(l_liker))
                            continue

                        try:
                            l_likerName = l_liker['name']
                        except KeyError:
                            self.m_logger.warning('No name found in Liker: {0}'.format(l_liker))
                            continue

                        l_dtMsgStr = l_dtMsg.strftime('%Y-%m-%dT%H:%M:%S+000')

                        self.storeUser(l_likerId, l_likerName, l_dtMsgStr, '')

                        l_likerInternalId = self.getUserInternalId(l_likerId)

                        self.createLikeLink(l_likerInternalId, l_internalId, l_dtMsgStr)

                        if EcAppParam.gcm_verboseModeOn:
                            self.m_logger.debug('   {0}/{1} [{2} | {3}] {4}'.format(
                                l_objCount, l_totalCount, l_likerId, l_likerInternalId, l_likerName))

                        l_likeCount += 1

                    if 'paging' in l_responseData.keys() and 'next' in l_responseData['paging'].keys():
                        self.m_logger.info('   *** {0}/{1} Getting next likes block ...'.format(
                            l_objCount, l_totalCount))
                        l_request = l_responseData['paging']['next']
                        l_response = self.performRequest(l_request)

                        l_responseData = json.loads(l_response)
                    else:
                        break

                self.setLikeFlag(l_id)
                self.m_logger.info('   {0}/{1} --> {2} Likes:'.format(l_objCount, l_totalCount, l_likeCount))
                l_objCount += 1

        except Exception as e:
            self.m_logger.critical('Likes detail download Exception: {0}/{1}'.format(repr(e), l_cursor.query))
            raise BulkDownloaderException('Likes detail download Exception: {0}'.format(repr(e)))

        l_cursor.close()
        self.m_pool.putconn(l_conn)

        self.m_logger.info('End getLikesDetail()')

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

    # mailer init
    EcMailer.init_mailer()

    # test connection to PostgresQL and wait if unavailable
    gcm_maxTries = 20
    l_iter = 0
    while True:
        if l_iter >= gcm_maxTries:
            EcMailer.send_mail('WAITING: No PostgreSQL yet ...', 'l_iter = {0}'.format(l_iter))
            sys.exit(0)

        l_iter += 1

        try:
            l_connect0 = psycopg2.connect(
                host=EcAppParam.gcm_dbServer,
                database=EcAppParam.gcm_dbDatabase,
                user=EcAppParam.gcm_dbUser,
                password=EcAppParam.gcm_dbPassword
            )

            l_connect0.close()
            break
        except psycopg2.Error as e0:
            EcMailer.send_mail('WAITING: No PostgreSQL yet ...', repr(e0))
            time.sleep(1)
            continue

    # logging system init
    try:
        EcLogger.log_init()
    except Exception as e0:
        EcMailer.send_mail('Failed to initialize EcLogger', repr(e0))

    l_phantomId0, l_phantomPwd0, l_vpn0 = 'nicolas.reimen@gmail.com', 'murugan!', None
    # l_vpn = 'India.Maharashtra.Mumbai.TCP.ovpn'

    l_driver = BrowserDriver()
    l_pool = EcConnectionPool.get_new()

    l_driver.m_user_api = l_phantomId0
    l_driver.m_pass_api = l_phantomPwd0

    l_downloader = BulkDownloader(l_driver, l_pool, l_phantomId0, l_phantomPwd0)
    l_downloader.bulk_download()

    if EcAppParam.gcm_headless:
        l_driver.close()
