#!/usr/bin/python3
# -*- coding: utf-8 -*-

import lxml.html as html
import io
import subprocess
import json
import base64
import copy
from PIL import Image

from selenium.webdriver.common.keys import Keys as K

from rbs_fb_connect import *


class ProfileDownloader:
    """
    Bogus class isolating the user profile downloading features
    """

    def __init__(self, p_browserDriver):
        # Local copy of the browser Driver
        self.m_driver = p_browserDriver.m_driver
        self.m_browserDriver = p_browserDriver

        # instantiates class logger
        self.m_logger = logging.getLogger('ProfileDownloader')

    def get_fb_profile(self, p_feedType='User', p_obfuscate=True):
        """
        Downloads the profile of a user. Before calling this method, the user's page must already have been loaded.

        :param p_feedType: 'User' (Ordinary user feed), 'Own' (Logged-in user's own feed) or 'Page' (Page feed).
        :param p_obfuscate: True --> random mouse moves and right clicks while waiting. Otherwise, simple `os.sleep()`.
        :return: Nothing
        """
        self.m_logger.info("get_fb_profile()")

        # erase all images and xml files in the directory
        if EcAppParam.gcm_verboseModeOn:
            for l_path in [EcAppParam.gcm_appRoot + '*.png', EcAppParam.gcm_appRoot + '*.xml']:
                l_result = subprocess.run(
                    'rm -f ' + l_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                self.m_logger.info('Erasing files : ' + repr(l_result))

        # determination of the xpath to find the stories in the feed
        if p_feedType == 'Page':
            l_storyXpath = '//div[@class="_427x"]/div[contains(@class, "_4-u2")]'
            l_storyPrefix = None
        else:
            if p_feedType == 'Own':
                # Logged-in user's own feed
                l_storyPrefix = 'hyperfeed_story_id_'
            else:
                # Ordinary user feed
                l_storyPrefix = 'tl_unit_'

            l_storyXpath = '//div[contains(@id, "{0}")]'.format(l_storyPrefix)

        # wait for the presence of at least one such block
        try:
            WebDriverWait(self.m_driver, 15).until(
                EC.presence_of_element_located((By.XPATH, l_storyXpath)))
        except EX.TimeoutException as e:
            self.m_logger.warning('could not find story xpath: {0}/{1}'.format(l_storyXpath, repr(e)))
            raise

        self.m_logger.info('presence of {0}'.format(l_storyXpath))

        # repeat calls to analyze_story until no more stories can be obtained
        # list of story ids to avoid processing them twice
        l_storyList = []
        # a fruitless try occurs when no new stories could be found while scanning all stories on the page
        l_fruitlessTries = 0
        # current y position of the top of the viewport
        l_curY = 0
        # total number of stories retrieved so far
        l_storyCount = 0

        # just in case
        self.m_driver.execute_script('window.scrollTo(0, 0);')

        l_selectorCount = 0
        l_unfilteredCount = 0
        while l_storyCount <= EcAppParam.gcm_max_story_count and l_fruitlessTries < 3:
            # main loop will continue until the desired number of stories is reached or 3 fruitless
            # tries have occurred
            l_fruitlessTry = True
            self.m_logger.info(
                '### main loop. l_storyCount={0} l_fruitlessTries={1}'.format(l_storyCount, l_fruitlessTries))
            for l_story in self.m_driver.find_elements_by_xpath(l_storyXpath):
                if l_storyCount > EcAppParam.gcm_max_story_count:
                    break
                # inner loop: scans all stories present on the page
                try:
                    # get the stories id attribute
                    l_id = l_story.get_attribute('id')
                    if l_id not in l_storyList:
                        # if the story's id is not in the list --> it is a new one

                        # so this cycle is not a fruitless try
                        l_fruitlessTry = False
                        l_fruitlessTries = 0

                        # add the story's id to the list to avoid processing it again
                        l_storyList.append(l_id)

                        # analyze the story (includes scrolling past the story block to trigger the loading
                        # of new ones, if any). This call affects the current y position.
                        l_curY, l_retStory = \
                            self.analyze_story(l_story, l_storyCount, l_curY, p_feedType,
                                               p_storyPrefix=l_storyPrefix, p_obfuscate=p_obfuscate)

                        if l_retStory is not None:
                            l_storyDate = l_retStory['date']

                            if EcAppParam.gcm_debugModeOn:
                                l_retStory1 = copy.deepcopy(l_retStory)
                                l_retStory1['images'] = [i[:100] + '...' for i in l_retStory1['images']]
                                l_retStory1['html'] = l_retStory1['html'][:100] + '...'
                                self.m_logger.debug('repr: {0}'.format(repr(l_retStory1)))

                            if l_retStory['date'] is not None:
                                l_retStory['date'] = l_retStory['date'].strftime('%Y%m%d %H:%M')
                            l_retStory['date_quoted'] = [d.strftime('%Y%m%d %H:%M') for d in l_retStory['date_quoted']]
                            if 'comments' in l_retStory.keys():
                                l_retStory['comments'] = [{
                                    'authors': l_com['authors'],
                                    'text': l_com['text'],
                                    'date': l_com['date'].strftime('%Y%m%d %H:%M'),
                                } for l_com in l_retStory['comments']
                                ]
                            l_jsonStore = json.dumps(l_retStory)

                            l_likes = 0
                            if 'total' in l_retStory['likes'].keys():
                                l_likes = l_retStory['likes']['total']

                            l_commentsCount = l_retStory['comments_count']
                            l_shareCount = l_retStory['shares']

                            if EcAppParam.gcm_debugModeOn:
                                l_retStory['images'] = [i[:100] + '...' for i in l_retStory['images']]
                                l_retStory['html'] = l_retStory['html'][:100] + '...'
                                self.m_logger.info('json [{0}]: {1}'.format(len(l_jsonStore), json.dumps(l_retStory)))

                            if l_retStory['comment_selector']:
                                l_selectorCount += 1
                            if l_retStory['comment_unfiltered']:
                                l_unfilteredCount += 1

                            # open database connection
                            l_connect1 = psycopg2.connect(
                                host=EcAppParam.gcm_dbServer,
                                database=EcAppParam.gcm_dbDatabase,
                                user=EcAppParam.gcm_dbUser,
                                password=EcAppParam.gcm_dbPassword
                            )

                            l_cursor = l_connect1.cursor()
                            l_cursor.execute(
                                """
                                    insert into
                                        "TB_STORY" (
                                            "ST_SESSION_ID"
                                            , "DT_CRE"
                                            , "DT_STORY"
                                            , "ST_TYPE"
                                            , "TX_JSON"
                                            , "N_LIKES"
                                            , "N_COMMENTS"
                                            , "N_SHARES"
                                        )
                                    values (%s, %s, %s, %s, %s, %s, %s, %s)
                                ;""", (
                                    self.m_browserDriver.m_dnl_ses_id,
                                    datetime.datetime.now(),
                                    l_storyDate,
                                    l_retStory['type'],
                                    l_jsonStore,
                                    l_likes, l_commentsCount, l_shareCount
                                )
                            )
                            l_connect1.commit()

                            # close database connection
                            l_connect1.close()

                        l_storyCount += 1
                        self.m_logger.info(
                            'Stories with comment selector / unfiltered selected: {0}/{1}'.format(l_selectorCount,
                                                                                                  l_unfilteredCount))
                        if p_obfuscate:
                            l_wait = random.randint(
                                EcAppParam.gcm_storiesMinDelay, EcAppParam.gcm_storiesMaxDelay)
                            self.m_logger.info('[{0}] Waiting for {1} seconds'.format(
                                self.m_browserDriver.m_phantomID, l_wait))
                            self.m_browserDriver.mouse_obfuscate(l_wait)
                    else:
                        self.m_logger.debug('--- Story already analyzed: ' + l_id)
                except EX.StaleElementReferenceException as e:
                    self.m_logger.warning('Stale element: {0}'.format(repr(e)))
                    continue

            if l_fruitlessTry:
                l_fruitlessTries += 1

            if l_storyCount > EcAppParam.gcm_max_story_count or l_fruitlessTries > 3:
                break

        # mark the session as complete
        self.m_logger.info('Session [{0}] Complete'.format(self.m_browserDriver.m_dnl_ses_id))
        self.m_browserDriver.m_dnl_ses_id = None

        self.m_logger.info(
            'Stories with comment selector / unfiltered selected: {0}/{1}'.format(l_selectorCount, l_unfilteredCount))

    def date_from_text(self, p_txtDt):
        """
        Convert a date string in FB format into a Python `datetime`.

        :param p_txtDt: Date string in FB format 
        :return:  Corresponding `datetime`
        """
        l_txtDt0 = p_txtDt
        for l_from, l_to in [(r'\,', r''), (r'\sat\s', r' '),
                             (r'(\d+)am', r'\1AM'),
                             (r'(\d+)pm', r'\1PM'),
                             (r'\s(\d)\s(\d\d\d\d)', r' 0\1 \2'),
                             (r'\s(\d):', r' 0\1:'),
                             (r':(\d)(AM|PM|$)', r':0\1\2')]:
            p_txtDt = re.sub(l_from, l_to, p_txtDt)

        try:
            l_dt = datetime.datetime.strptime(p_txtDt, '%A %d %B %Y %H:%M')
        except ValueError as e1:
            if EcAppParam.gcm_debugModeOn:
                print(repr(e1))
            try:
                l_dt = datetime.datetime.strptime(p_txtDt, '%A %B %d %Y %I:%M%p')
            except ValueError as e2:
                if EcAppParam.gcm_debugModeOn:
                    self.m_logger.warning('Date conversion Error: using now() instead' + repr(e2))
                l_dt = datetime.datetime.now()

        if EcAppParam.gcm_debugModeOn:
            self.m_logger.info('l_txtDt: [{0}] {1} --> {2}'.format(
                l_txtDt0, p_txtDt, l_dt.strftime('%A %B %d/%m/%Y %I:%M%p')))

        return l_dt

    def value_from_text(self, p_txt):
        """
        Tries to convert a string into a numeric value according to Facebook conventions:
        * plain integer
        * string of the form 'xx.yy K'

        :param p_txt: The text to convert
        :return: The resulting numeric value or -1 if failed
        """
        l_value = -1
        l_txt = re.sub(',', '.', p_txt.strip())
        try:
            l_value = int(l_txt)
        except ValueError:
            l_txt = re.sub('([kK])$', '', l_txt).strip()
            try:
                l_value = float(l_txt) * 1000
            except ValueError as e:
                self.m_logger.warning('Could not convert string: "{0}" to numeric [{1}]'.format(p_txt, repr(e)))

        return l_value

    def analyze_story(self, p_story, p_iter, p_curY, p_feedType,
                      p_storyPrefix='tl_unit_', p_obfuscate=True):
        """
        Story analysis method.
        :param p_story: WebDriver element positioned on the story
        :param p_iter: Story number (starting at 0)
        :param p_curY: Current scrolling Y position in the browser
        :param p_feedType: 'User', 'Own' or 'Page'. Same as in :any:`BrowserDriver.get_fb_profile()`
        :param p_storyPrefix: Prefix of the `id` attribute of the story's outermost `<div>`
            (for 'User' and 'Own' types only)
        :return: the new y scroll value
        """
        # timing counter
        t0 = time.perf_counter()

        # raw html for this story
        l_html = p_story.get_attribute('outerHTML')

        if EcAppParam.gcm_debugModeOn:
            l_htmlShort = l_html[:500]
            if len(l_html) != len(l_htmlShort):
                l_htmlShort += '...'
            print("-------- {0} --------\n{1}".format(p_iter, l_htmlShort))

        # determination of post ID
        if p_feedType == 'Page':
            l_data_ft = p_story.get_attribute('data-ft')
            if l_data_ft is not None:
                self.m_logger.info('l_data_ft: {0}'.format(l_data_ft))
                l_data_ft_dict = json.loads(l_data_ft)
                self.m_logger.info('l_data_ft_dict: {0}'.format(l_data_ft_dict))
                l_id = l_data_ft_dict['tl_objid']
            else:
                l_post_link = p_story.find_element_by_xpath('.//div[contains(@class, "_5u5j")]//a[@class="_5pcq"]')
                l_post_link_href = l_post_link.get_attribute('href')
                self.m_logger.info('l_post_link_href: {0}'.format(l_post_link_href))
                l_match = re.search('([^/]+)($|/$)', l_post_link_href)
                if l_match:
                    l_id = l_match.group(1)
                else:
                    l_id = None
        else:
            # story ID without prefix
            l_id = p_story.get_attribute('id')
            l_id = re.sub(p_storyPrefix, '', l_id).strip()

        self.m_logger.info('l_id: {0}'.format(l_id))

        # location and size
        l_location = p_story.location
        l_size = p_story.size

        # detection of abnormal location parameters --> skip story
        if l_location['x'] == 0 or l_location['y'] == p_curY:
            self.m_logger.info('Location anomaly: {0}'.format(l_location))
            return p_curY, None

        # calculation of scroll parameters
        l_yTop = l_location['y'] - 100 if l_location['y'] > 100 else 0
        l_deltaY = l_yTop - p_curY
        # l_curY = l_yTop
        l_overshoot = l_location['y'] + l_size['height'] + 50

        # scroll past story (overshoot) and then to top of story.
        if p_obfuscate:
            self.m_browserDriver.scroll_obfuscate(l_overshoot)
            self.m_browserDriver.scroll_obfuscate(l_yTop)
        else:
            # first scroll to the bottom of the story (overshoot) to trigger loading of next stories, if any
            self.m_driver.execute_script('window.scrollTo(0, {0});'.format(l_overshoot))
            # then scroll to the top of the story
            self.m_driver.execute_script('window.scrollTo(0, {0});'.format(l_yTop))

        # determine actual scroll position in case scroll was hampered by top or bottom of page
        l_curY = self.m_driver.execute_script('return window.scrollY;')

        # previous method with delta, kept for now in reserve
        # self.m_driver.execute_script('window.scrollBy(0, {0});'.format(l_deltaY))

        # waiting for story availability
        # first wait for visibility DOM attribute
        try:
            WebDriverWait(self.m_driver, 15).until(EC.visibility_of(p_story))
        except EX.TimeoutException as e:
            self.m_logger.warning('Story [{0}] failed to become visible: {1}'.format(l_id, repr(e)))

        # l_wait_cycles = 0
        # while not self.m_driver.execute_script('return arguments[0].complete', p_story):
        #    time.sleep(.05)
        #    l_wait_cycles += 1
        #    if l_wait_cycles % 10 == 0:
        #        self.m_logger.info('Wait for complete: {0}'.format(l_wait_cycles))

        #    if l_wait_cycles > 100:
        #        break

        # for safety's sake, wait for the absence of data-ft attribute
        l_dataWait = 0
        while l_dataWait < 100:
            l_data_ft = p_story.get_attribute('data-ft')
            if l_data_ft is None:
                break
            else:
                l_dataWait += 1
                if l_dataWait % 10 == 0 and EcAppParam.gcm_debugModeOn:
                    print('l_data_ft [{0}]: {1}'.format(l_dataWait, l_data_ft[:50]))

            time.sleep(.05)

        # build an lxml analyser out of the html
        l_tree = html.fromstring(l_html)

        # DATA: (1) detection of sponsored content
        l_sponsored = (len(l_tree.xpath('.//a[text()="Sponsored"]')) > 0)

        # raw text from the main header and of the sub-header (quoted content), if any
        l_fromHeader = BrowserDriver.get_unique(l_tree, '//h5[contains(@class, "_5vra")]')
        l_fromHeaderSub = BrowserDriver.get_unique(l_tree, '//h6[contains(@class, "_5vra")]')

        # DATA: (2) determines if the list of authors contains a 'with' marker
        l_hasWith = False
        if re.search('with', l_fromHeader) or re.search('with', l_fromHeaderSub):
            l_hasWith = True

        # types of shared documents (may be found in the same form as from authors and must thus be eliminated)
        l_shareTypes = ['photo', 'post', 'link', 'event', 'video', 'live video', 'page']

        # DATA: (3) shared object list (post, photo, video ...) + life events + locations
        # _5vra --> part of the post header containing the names of the post authors
        l_sharedList = []
        for l_type in l_shareTypes:
            for l in l_tree.xpath('.//h5[contains(@class, "_5vra")]//a[text()="{0}"]'.format(l_type)):
                l_sharedList.append((l_type, l.get('href')))

        # DATA: (4) determining story type
        # by default, it is a post
        l_type = 'post'
        if l_sponsored:
            # sponsored type
            l_type = 'sponsored'
        else:
            # special cases of 'share': memory
            if re.search('shared a memory', l_fromHeader):
                l_type = 'memory'
            # other cases cases of share or like of previously existing content
            elif re.search('shared|liked', l_fromHeader):
                if re.search('shared', l_fromHeader):
                    l_type = 'share'
                else:
                    l_type = 'like'

                # subtype of share/like given by the type of the first element of the quoted content list
                if len(l_sharedList) > 0:
                    l_type += '/{0}'.format(l_sharedList[0][0])

            # other types of posts
            elif re.search('commented on this', l_fromHeader):
                l_type = 'comment'
            elif re.search('updated (his|her) profile picture', l_fromHeader):
                l_type = 'narcissistic/PP'
            elif re.search('updated (his|her) cover photo', l_fromHeader):
                l_type = 'narcissistic/CP'
            elif re.search('updated (his|her) profile video', l_fromHeader):
                l_type = 'narcissistic/PV'
            elif re.search('(friends|friend) ​posted​ on (\S+) Timeline', l_fromHeader):
                l_type = 'wall'
            else:
                l_lifeEvent = BrowserDriver.get_unique(l_tree, '//a[@class="_39g6"]')
                if len(l_lifeEvent) > 0:
                    l_type = 'life_event'
                    for l_eventLink in l_tree.xpath('//a[@class="_39g6"]'):
                        l_sharedList.append((l_type, l_eventLink.text_content()))
                else:
                    # case of 'people you may know' stories (found only in user's own feed)
                    l_fromHeader2 = BrowserDriver.get_unique(l_tree, '//div[contains(@class, "fwn fcg")]')
                    # <div class="fwn fcg"><span class="fwb fcb">People you may know</span></div>
                    if re.search('People you may know', l_fromHeader2):
                        l_type = 'FB/PYMK'

        # DATA: (5) Date(s)
        l_datePost = None
        l_dateQuoted = []
        for l_dateContainer in p_story.find_elements_by_xpath('.//abbr[contains(@class, "_5ptz")]'):
            l_dt = self.date_from_text(l_dateContainer.get_attribute('title'))

            if l_datePost is None:
                # first date found considered as main story date
                l_datePost = l_dt
            else:
                # other dates considered as coming from quoted parts (shared post, ...)
                l_dateQuoted.append(l_dt)

        # DATA: (6) main text
        l_postText = ''
        # text found in the 'userContent' marked section. But 'userContentWrapper' must be avoided
        for l_pt in p_story.find_elements_by_xpath(
                './/div[contains(@class, "userContent") and not(contains(@class, "userContentWrapper"))]'):
            l_ptHtml = l_pt.get_attribute('innerHTML')

            # removes html tags and replaces them with spaces
            if l_ptHtml is not None:
                l_postText = re.sub('<[^>]+>', ' ', l_ptHtml)
                l_postText = re.sub('\s+', ' ', l_postText).strip()

            if l_postText is not None and len(l_postText) > 0:
                break

        # DATA: (7) quoted text
        l_quotedText = ''
        # _5r69 --> Quoted portion (normally) otherwise, class containing 'mtm'
        for l_xpath in ['.//div[@class="_5r69"]', './/div[contains(@class, "mtm")]']:
            for l_quote in p_story.find_elements_by_xpath(l_xpath):
                l_quoteHtml = l_quote.get_attribute('innerHTML')

                # removes html tags and replaces them with spaces
                if l_quoteHtml is not None:
                    l_quotedText = re.sub('<[^>]+>', ' ', l_quoteHtml)
                    l_quotedText = re.sub('\s+', ' ', l_quotedText).strip()

                if l_quotedText is not None and len(l_quotedText) > 0:
                    break

            # if _5r69 yielded tex, 'mtm' must not be used
            if l_quotedText is not None and len(l_quotedText) > 0:
                break

        # DATA: (8) determining from
        # list of author (no deduplication but order preserved)
        l_from = []
        # dictionary of authors (deduplication but order lost)
        l_fromDict = dict()
        # _5x46 --> whole post header
        # _5vra --> part of the post header containing the names of the post authors

        # first case below corresponds to multiple authors and the second to single authors
        # the third one corresponds to wall postings by others (type = 'wall')
        for l_xpath in ['//div[contains(@class, "_5x46")]//a[contains(@class, "profileLink")]',
                        '//h5[contains(@class, "_5vra")]//a',
                        '//h6[contains(@class, "_5vra")]//a']:

            for l_profLink in l_tree.xpath(l_xpath):
                l_previous = l_profLink.getprevious()

                # identification of location links --> list of shared objects and not 'from'
                if l_previous is not None \
                        and l_previous.tag == 'i' \
                        and l_previous.get('class') == '_51mq img sp_ok0d9_HV2Xz sx_66a6bc':
                    # _51mq img sp_ok0d9_HV2Xz sx_66a6bc

                    l_sharedList.append(('location', l_profLink.get('href')))
                else:
                    # real from links (not locations)

                    # a. full name --> text content of the link
                    l_fromName = l_profLink.text_content()

                    # b. FB user name
                    l_fromUser = l_profLink.get('href')
                    if l_fromUser is not None and len(l_fromUser) > 0:
                        # 'https\:\/\/www\.facebook\.com\/Sergei1970sk'
                        # https://www.facebook.com/solanki.jagdish.75098?hc_ref=NEWSFEED
                        if re.search('\.php\?id=', l_fromUser):
                            # human users
                            l_match = re.search('\.php\?id=(\d+)(&|$)', l_fromUser)
                            if l_match:
                                l_fromUser = l_match.group(1)
                        else:
                            # page users
                            l_match = re.search('\.com/([^?]+)(\?|$)', l_fromUser)
                            if l_match:
                                l_fromUser = l_match.group(1)

                    # c. FB user ID
                    l_fromId = l_profLink.get('data-hovercard')
                    if l_fromId is not None and len(l_fromId) > 0:
                        l_match = re.search('\.php\?id=(\d+)(&|$)', l_fromId)
                        if l_match:
                            l_fromId = l_match.group(1)

                    # only record user data if it is not a 'false' user corresponding to a share or like
                    # or to a wall posting link (type = 'wall')
                    if l_fromName not in l_shareTypes and l_fromName != 'posted':
                        # store both in a list and dict
                        l_from += [(l_fromName, l_fromId, l_fromUser)]
                        l_fromDict[l_fromId] = (l_fromName, l_fromUser)

            if len(l_from) > 0:
                break

        # DATA: (9) image extraction
        l_imageList = []
        if EcAppParam.gcm_getImages:
            # gets a full screenshot of the browser viewport
            l_imgStory = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
            x = l_location['x']
            y = l_location['y'] - l_yTop

            l_img = l_imgStory.crop((x, y, x + l_size['width'], y + l_size['height']))

            l_baseName = '{0:03}-'.format(p_iter) + l_id
            if EcAppParam.gcm_debugModeOn:
                # store html source and image of complete story
                l_img.save(l_baseName + '.png')
                with open(l_baseName + '.xml', "w") as l_xml_file:
                    l_xml_file.write(l_html)

            l_outputBuffer = io.BytesIO()
            l_img.save(l_outputBuffer, format='PNG')
            l_imageList.append(base64.b64encode(l_outputBuffer.getvalue()).decode())

            # extract actual images from the story, if any
            l_imgCount = 0
            # use, if possible, the div above the <img> tag itself because its size is more often correct
            for l_xpath in ['.//div[./img[contains(@class, "img")]]', './/img[contains(@class, "img")]']:
                for l_image in p_story.find_elements_by_xpath(l_xpath):
                    l_height = l_image.size['height']
                    l_width = l_image.size['width']
                    if l_width >= EcAppParam.gcm_minImageSize and l_height >= EcAppParam.gcm_minImageSize:
                        l_htmlHeight = l_image.get_attribute('height')
                        l_htmlWidth = l_image.get_attribute('width')

                        if EcAppParam.gcm_debugModeOn:
                            print(
                                '*** IMG [{0}] S:({1} {2}) H:({3} {4}): '.format(
                                    l_imgCount, l_width, l_height, l_htmlWidth, l_htmlHeight) +
                                l_image.get_attribute('outerHTML')
                            )

                        # for i in range(0,10):
                        #    print('Loaded: ' + repr(self.m_driver.execute_script('return arguments[0].complete', l_image)))

                        l_img_location = l_image.location
                        xi = l_img_location['x']
                        yi = l_img_location['y'] - l_yTop

                        # do the crop of the whole screen image in order to keep only the image
                        l_imgInStory = l_imgStory.crop((xi, yi, xi + l_width, yi + l_height))
                        if EcAppParam.gcm_debugModeOn:
                            l_imgInStory.save(l_baseName + '_{0:02}.png'.format(l_imgCount))
                        l_imgCount += 1

                        l_outputBuffer = io.BytesIO()
                        l_imgInStory.save(l_outputBuffer, format='PNG')
                        l_imageList.append(base64.b64encode(l_outputBuffer.getvalue()).decode())

                # if first xpath worked --> no nee to try the second
                if l_imgCount > 0:
                    break

        # DATA: (10) likes
        l_likesValues = dict()
        for l_likeElement in l_tree.xpath('//a[@class="_2x4v"]/span[@class="_4arz"]'):
            l_likeTxt = l_likeElement.text_content()
            l_likeCount = self.value_from_text(l_likeTxt)
            if l_likeCount == -1:
                self.m_logger.warning('Found likes total but could not convert to num: {0}'.format(l_likeTxt))
            else:
                if 'total' in l_likesValues.keys():
                    l_likesValues['total'] += l_likeCount
                else:
                    l_likesValues['total'] = l_likeCount

        for l_likeElement in l_tree.xpath('//span[@class="_3t54"]/a[@class="_3emk"]'):
            l_likeTxt = l_likeElement.get('aria-label')
            l_found = re.search('(\d+)\s(.+)', l_likeTxt)
            if l_found:
                l_likeCountTxt = l_found.group(1)
                l_likeType = l_found.group(2)
                l_likeCount = self.value_from_text(l_likeCountTxt)
                if l_likeCount == -1:
                    self.m_logger.warning('Found likes value but could not convert to num: {0}'.format(
                        l_likeCountTxt))
                else:
                    if l_likeType in l_likesValues.keys():
                        l_likesValues[l_likeType] += l_likeCount
                    else:
                        l_likesValues[l_likeType] = l_likeCount
            else:
                self.m_logger.warning('Unrecognized like pattern: {0}'.format(l_likeTxt))

        # DATA: (11) comments
        # Expanding comments
        l_modeSelector = False
        l_foundItem = False
        if EcAppParam.gcm_expandComments:
            # try to select unfiltered mode if possible
            l_modeLink = None
            try:
                # UFIRow UFILikeSentence _4204 _4_dr _3scp _3scs uiPopover _6a _6b _54nh uiContextualLayer
                l_modeLink = p_story.find_element_by_xpath(
                    './/div[contains(@class, "UFILikeSentence")]//div[@class="_3scp"]' +
                    '//div[contains(@class, "uiPopover")]/a')

                self.m_logger.info('Mode selection link BEFORE: ' + l_modeLink.text)
                l_modeSelector = self.m_browserDriver.make_visible_and_click(l_modeLink)
            except EX.NoSuchElementException:
                self.m_logger.info('No mode selector')
                l_modeSelector = False

            if l_modeSelector:
                try:
                    self.m_logger.info('Waiting for Popup menu')
                    WebDriverWait(self.m_driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//div[@class="_3scn"]')))
                    self.m_logger.info('Popup menu found')

                    l_candidateNumber = 0
                    for l_menuCandidate in self.m_driver.find_elements_by_xpath('//ul[@class="_54nf"]'):
                        self.m_logger.info('Popup menu candidate: {0}'.format(l_candidateNumber))
                        for l_item in l_menuCandidate.find_elements_by_xpath('.//div[@class="_3scn"]'):
                            self.m_logger.info('div[@class="_3scn"] text --> ' + l_item.text)
                            self.m_logger.info(
                                'div[@class="_3scn"] inner HTML --> ' + l_item.get_attribute('outerHTML'))
                            self.m_logger.info('div[@class="_3scn"] visibility --> {0}'.format(
                                l_item.is_displayed()
                            ))

                            if l_item.is_displayed() and re.search('\(unfiltered\)', l_item.text):
                                if self.m_browserDriver.make_visible_and_click(l_item):
                                    self.m_logger.info('Menu Item clicked')
                                    l_foundItem = True
                                    time.sleep(.01)
                                    while True:
                                        try:
                                            p_story.find_element_by_xpath(
                                                './/div[contains(@class, "UFICommentContentBlock")]')
                                            break
                                        except EX.NoSuchElementException:
                                            time.sleep(.01)
                                else:
                                    # Escape -> make the pop-up disappear
                                    self.m_driver.send_keys(K.ESCAPE)
                                    l_modeSelector = False
                                    l_foundItem = False
                                    break
                        l_candidateNumber += 1

                except EX.TimeoutException:
                    self.m_logger.info('Popup menu NOT found')

            if l_modeSelector:
                self.m_logger.info('Mode selection link AFTER: ' + l_modeLink.text)

                if re.search('\(unfiltered\)', l_modeLink.text):
                    l_foundItem = True
                else:
                    l_foundItem = False

            l_newCommentsFound = False
            l_expansionOccurred = True
            while l_expansionOccurred:
                l_expansionOccurred = False

                # count Comments
                l_commentCountBefore = len(p_story.find_elements_by_xpath(
                    './/div[contains(@class, "UFICommentContentBlock")]'))
                self.m_logger.info('l_commentCountBefore: {0}'.format(l_commentCountBefore))

                l_additionalComments = 0
                # list all "more comments" and "replies" links
                for l_commentLink in p_story.find_elements_by_xpath(
                        './/a[@class="UFIPagerLink" or @class="UFICommentLink"]'):

                    # do not activate the "Hide xxx replies" links
                    if re.search('Hide.*Replies', l_commentLink.text) or \
                            re.search('Write\sa', l_commentLink.text):
                        continue

                    self.m_logger.info('+++ Link Text: [{0}] +++'.format(l_commentLink.text))
                    l_increment = 1
                    for l_word in l_commentLink.text.split(' '):
                        try:
                            l_increment = int(l_word)
                            self.m_logger.info('l_increment: {0}'.format(l_increment))
                            break
                        except Exception:
                            continue
                    l_additionalComments += l_increment

                    # make sure the link is in view and click it
                    if self.m_browserDriver.make_visible_and_click(l_commentLink):
                        l_expansionOccurred = True
                        l_newCommentsFound = True

                self.m_logger.info('l_additionalComments: {0}'.format(l_additionalComments))

                l_finished = False
                l_loopCount = 0
                while not l_finished:
                    # count Comments again
                    l_commentCountAfter = len(p_story.find_elements_by_xpath(
                        './/div[contains(@class, "UFICommentContentBlock")]'))
                    self.m_logger.info('l_commentCountAfter: {0}'.format(l_commentCountAfter))

                    l_loopCount += 1
                    l_finished = (l_commentCountAfter >= l_commentCountBefore + l_additionalComments) \
                                 or l_loopCount >= 12

                    time.sleep(.25)

            if l_newCommentsFound:
                # update lxml tree if new comments were retrieved
                l_tree = html.fromstring(p_story.get_attribute('outerHTML'))

        # Recording comments
        l_comments = []
        for l_commentBlock in l_tree.xpath('//div[contains(@class, "UFICommentContentBlock")]'):
            l_dateTxt = BrowserDriver.get_unique_attr(
                l_commentBlock, './/abbr[contains(@class, "livetimestamp")]', 'title')
            l_text = BrowserDriver.get_unique(l_commentBlock, './/span[@class="UFICommentBody"]')

            l_authList = []
            for l_authorTag in l_commentBlock.xpath('.//a[contains(@class, "UFICommentActorName")]'):
                l_name = l_authorTag.text_content()
                l_userId = l_authorTag.get('href')
                l_userId = re.sub('https://www.facebook.com/', '', l_userId)
                l_userId = re.sub('\?.*$', '', l_userId).strip()
                l_authList.append({'name': l_name, 'user_id': l_userId})

            l_comments.append({
                'date': self.date_from_text(l_dateTxt),
                'text': l_text,
                'authors': l_authList})

        l_commentCount = len(l_comments)
        for l_additionalTag in l_tree.xpath('//a[contains(@class, "UFIPagerLink")]'):
            l_txt = l_additionalTag.text_content()
            l_txt = re.sub('^View\s', '', l_txt)
            l_txt = re.sub('\smore (comments|comment)$', '', l_txt)

            l_additional = self.value_from_text(l_txt)
            if l_additional > 0:
                l_commentCount += l_additional

        # DATA: (12) shares
        l_shares = 0
        for l_shareTag in l_tree.xpath('//a[contains(@class, "UFIShareLink")]'):
            l_txt = l_shareTag.text_content()
            l_txt = re.sub('\s+(shares|share)$', '', l_txt)
            l_shares = self.value_from_text(l_txt)

        l_retStory = dict()
        l_retStory['id'] = l_id
        l_retStory['date'] = l_datePost
        l_retStory['with'] = l_hasWith
        l_retStory['sponsored'] = l_sponsored
        l_retStory['type'] = l_type
        l_retStory['shared'] = l_sharedList
        l_retStory['from_list'] = l_from
        l_retStory['from_dict'] = l_fromDict
        l_retStory['text'] = l_postText
        l_retStory['text_quoted'] = l_quotedText
        l_retStory['date_quoted'] = l_dateQuoted
        l_retStory['images'] = l_imageList
        l_retStory['html'] = l_html
        l_retStory['likes'] = l_likesValues
        l_retStory['comments'] = l_comments
        l_retStory['comments_count'] = l_commentCount
        l_retStory['shares'] = l_shares
        l_retStory['comment_selector'] = l_modeSelector
        l_retStory['comment_unfiltered'] = l_foundItem

        if EcAppParam.gcm_verboseModeOn:
            print('l_fromHeader   : ' + l_fromHeader)
            print('Id             : ' + l_id)
            print('Sponsored      : ' + repr(l_sponsored))
            print('Type           : ' + l_type)
            print('Shared objects : ' + repr(l_sharedList))
            print('Has with       : ' + repr(l_hasWith))
            print('Date           : ' + repr(l_datePost))
            print('Dates quoted   : ' + repr(l_dateQuoted))
            print('From           : ' + repr(l_from))
            print('From (dict)    : ' + repr(l_fromDict))
            print('Text           : ' + l_postText)
            print('Quoted Text    : ' + l_quotedText)
            print('Likes          : {0}'.format(repr(l_likesValues)))
            print('shares         : {0}'.format(l_shares))
            print('Comments       : {0}'.format(repr(l_comments)))
            print('Comments count : {0}'.format(l_commentCount))
            print('Size           : {0}'.format(l_size))
            print('Location       : {0}'.format(l_location))
            print('l_curY         : {0}'.format(p_curY))
            print('l_yTop         : {0}'.format(l_yTop))
            print('l_deltaY       : {0}'.format(l_deltaY))
            print('l_overshoot    : {0}'.format(l_overshoot))

        self.m_logger.info(
            'Processing story {0} complete. Elapsed time: {1:.3}'.format(p_iter, time.perf_counter() - t0))
        return l_curY, l_retStory


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

    # g_browser = 'Firefox'
    g_browser = 'Chrome'

    l_phantomId0 = 'nicolas.reimen@gmail.com'
    l_phantomPwd0 = 'murugan!'
    # l_vpn = 'India.Maharashtra.Mumbai.TCP.ovpn'
    l_vpn0 = None

    l_driver = BrowserDriver()
    l_driver.login_as_scrape(l_phantomId0, l_phantomPwd0, l_vpn0)
    #l_driver.go_random()
    l_driver.go_to_id(None, 'steve.stanzione')
    l_downloader = ProfileDownloader(l_driver)
    l_downloader.get_fb_profile()
    l_driver.log_out()

    if EcAppParam.gcm_headless:
        l_driver.close()