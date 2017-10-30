#!/usr/bin/python3
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common import exceptions as EX
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options

from pyvirtualdisplay import Display
from subprocess import run, PIPE

import random
import sys
import io
from PIL import Image

from ec_utilities import *
import wrapvpn

__author__ = 'Pavan Mahalingam'


class BrowserDriverException(Exception):
    def __init__(self, p_msg):
        self.m_msg = p_msg


class BrowserDriver:
    """
    The class driving the data extraction process.

    At instantiation, creates a Selenium Chrome driver and a headless (or non-headless) instance of chrome.
    After that, all operations are handled by this class.
    """

    @staticmethod
    def get_unique_attr(p_frag, p_xpath, p_attribute):
        """
        get a unique element attribute through lxml, with a warning mark ('¤')
        inside the string if more than one was found

        :param p_frag: lxml fragment from which to extract the value
        :param p_xpath: XPath pointing to the element
        :param p_attribute: Attribute name
        :return: Attribute value with possible warning mark ('¤') if more than one found
        """
        return '¤'.join([str(l_span.get(p_attribute)) for l_span in p_frag.xpath(p_xpath)]).strip()

    # get a unique text element through lxml, with a warning mark ('¤')
    # inside the string if more than one was found
    @staticmethod
    def get_unique(p_frag, p_xpath):
        """
        get a unique text element through lxml, with a warning mark ('¤')
        inside the string if more than one was found

        :param p_frag: lxml fragment from which to extract the value
        :param p_xpath: XPath pointing to the element
        :return: Text content of the element with possible warning mark ('¤') if more than one found
        """
        return '¤'.join([str(l_span.text_content()) for l_span in p_frag.xpath(p_xpath)]).strip()

    def __init__(self):
        """
        Instantiates Selenium WebDriver and Chrome instance. Chrome is headless or not
        depending on the value of :any:`EcAppParam.gcm_headless` (boolean)
        """
        # instantiates class logger
        self.m_logger = logging.getLogger('BrowserDriver')

        # create members so that they exist in __init__. In fact their real instantiation is in login_as_scrape()
        self.m_creationDate = datetime.datetime.now(tz=pytz.utc)
        self.m_expirationDate = datetime.datetime.now(tz=pytz.utc) + datetime.timedelta(days=3650)
        self.m_vpn_handle = None
        # FB User ID for progress messages
        self.m_phantomID = ''
        # FB User ID and password for API access
        self.m_user_api = ''
        self.m_pass_api = ''
        # FB token for API access + expiry date
        self.m_token_api = ''
        self.m_token_expiry = datetime.datetime.now(tz=pytz.utc) + datetime.timedelta(hours=1)

        if EcAppParam.gcm_headless:
            # if headless mode requested, starts the pyvirtualdisplay xvfb driver
            self.m_logger.info("Launching xvfb")
            self.m_display = Display(visible=0, size=(EcAppParam.gcm_headlessWidth, EcAppParam.gcm_headlessHeight))
            self.m_display.start()
        else:
            self.m_display = None

        # Launch Chrome (or Firefox) Webdriver
        if EcAppParam.gcm_browser == 'Chrome':
            # option object to be passed to chrome
            l_option = Options()

            # notification disabling option to be passed to Chrome
            l_option.add_argument('disable-notifications')
            if not EcAppParam.gcm_headless:
                l_option.add_argument('start-maximized')
            else:
                l_option.add_argument('start-fullscreen')

            # Create a new instance of the Chrome driver
            self.m_logger.info("Launching Chrome")
            self.m_driver = webdriver.Chrome(chrome_options=l_option)

            if not EcAppParam.gcm_headless:
                # Move the window to position x/y
                self.m_driver.set_window_position(700, 0)
                # Resize the window to the screen width/height
                self.m_driver.set_window_size(EcAppParam.gcm_width, EcAppParam.gcm_height)

                self.m_browserWidth, self.m_browserHeight = EcAppParam.gcm_width, EcAppParam.gcm_height
            else:
                self.m_browserWidth, self.m_browserHeight = \
                    EcAppParam.gcm_headlessWidth, EcAppParam.gcm_headlessHeight

        elif EcAppParam.gcm_browser == 'Firefox':
            # Create a new instance of the Firefox driver
            self.m_logger.info("Launching Firefox")
            self.m_driver = webdriver.Firefox()

            if not EcAppParam.gcm_headless:
                # Resize the window to the screen width/height
                self.m_driver.set_window_size(EcAppParam.gcm_width, EcAppParam.gcm_height)
                # Move the window to position x/y
                self.m_driver.set_window_position(800, 0)

                self.m_browserWidth, self.m_browserHeight = EcAppParam.gcm_width, EcAppParam.gcm_height
            else:
                self.m_browserWidth, self.m_browserHeight = \
                    EcAppParam.gcm_headlessWidth, EcAppParam.gcm_headlessHeight
        else:
            l_message = '[BrowserDriver] Browser type not supported: {0}'.format(EcAppParam.gcm_browser)
            self.m_logger.critical(l_message)
            raise BrowserDriverException(l_message)

        self.m_dnl_ses_id = None
        self.m_loggedIn = False

    def isStale(self):
        """
        Past expiration date flag.
        
        :return: Boolean. True if past expiration date.
        """
        return self.m_expirationDate < datetime.datetime.now(tz=pytz.utc)

    def token_is_stale(self):
        """
        API token past expiry date flag.

        :return: Boolean. True if past expiry date.
        """
        return self.m_token_expiry < datetime.datetime.now(tz=pytz.utc)

    def internet_check(self):
        """
        Presence of internet connection verification. Uses :any:`OpenvpnWrapper.getOwnIp`

        :return: `True` if internet can be reached. `False` otherwise. 
        """
        try:
            l_ip = wrapvpn.OpenvpnWrapper.getOwnIp()

            # if this point is reached --> the internet connection is probably ok
            self.m_logger.info('Own IP: {0}'.format(l_ip))
            l_internetOk = True
        except wrapvpn.OpenVpnFailure as e1:
            self.m_logger.warning('Own IP failure: ' + repr(e1))
            l_internetOk = False

        return l_internetOk

    def close(self):
        """
        Closing method. Closes Chrome and the associated WebDriver. If headless then also close xvfb.
        THIS METHOD DOES NOT WORK. Only logout must be used when a BrowserDriver is stale.
        
        :return: Nothing
        """
        self.m_driver.close()
        self.m_driver = None

        if self.m_display is not None:
            self.m_display.stop()
            self.m_display = None

        if self.m_vpn_handle is not None:
            self.m_vpn_handle.close()
            self.m_vpn_handle = None

        l_result = run(['sudo', 'killall', '-9', 'chromedriver'], stdout=PIPE, stderr=PIPE)
        self.m_logger.info('Killing chromedriver : ' + repr(l_result))
        l_result = run(['sudo', 'killall', '-9', 'chromium-browser'], stdout=PIPE, stderr=PIPE)
        self.m_logger.info('Killing Chromium : ' + repr(l_result))

    def login_as_scrape(self, p_user, p_passwd, p_vpn):
        """
        Logging-in method.

        :param p_user: FB user ID
        :param p_passwd: FB password
        :return: nothing (if there were problems --> raise errors)
        """

        # open vpn
        if p_vpn is not None and len(p_vpn) > 0:
            try:
                self.m_vpn_handle = wrapvpn.OpenvpnWrapper(p_vpn)
            except Exception as e:
                self.m_logger.warning('Unable to launch VPN [{0}]: {1}'.format(p_vpn, repr(e)))
        else:
            self.m_vpn_handle = None

        # load logging page
        self.m_logger.info('Loading Facebook log-in page')
        self.m_driver.get('http://www.facebook.com')

        l_stage = -1
        try:
            l_stage = 0
            # wait for the presence of the user ID (or e-mail) text input field.
            l_userInput = WebDriverWait(self.m_driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//td/input[@id="email"]')))

            l_stage = 1
            # sends the user ID string to it
            l_userInput.clear()
            # l_userInput.send_keys(p_user)
            self.key_slow(l_userInput, p_user)
            self.m_logger.info('User ID entered: {0}'.format(p_user))

            l_stage = 2
            # wait for the presence of the user password (or e-mail) text input field.
            l_pwdInput = WebDriverWait(self.m_driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//td/input[@id="pass"]')))

            # sends the password string to it
            l_pwdInput.clear()
            # l_pwdInput.send_keys(p_passwd)
            self.key_slow(l_pwdInput, p_passwd)
            self.m_logger.info('Password entered: {0}'.format(p_passwd))

            l_stage = 3
            # finds the log-in button and clicks it
            l_button = WebDriverWait(self.m_driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//label[@id="loginbutton"]/input')))
            l_button.click()
            self.m_logger.info('Login button clicked')

            # time.sleep(3600)

            l_stage = 4
            l_outcome = WebDriverWait(self.m_driver, 60).until(
                lambda p_driver: p_driver.find_elements(By.XPATH, '//div[@id="mainContainer"]') or
                                 p_driver.find_elements(By.XPATH, '//label/input[@name="clicked_not_now[1]"]') or
                                 p_driver.find_elements(By.XPATH, '//div[@class="loggedout_menubar_container"]'))
            l_outcome = l_outcome[0]

            if l_outcome.get_attribute('id') == 'mainContainer':
                # the user is logged-in and his home pages is starting to appear
                self.m_logger.info('User page display started')
            elif l_outcome.get_attribute('name') == 'clicked_not_now[1]':
                # a security warning page has been displayed
                self.m_logger.info('FB Security warning')
                l_outcome.click()

                l_stage = 9
                WebDriverWait(self.m_driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@id="mainContainer"]')))

                self.m_logger.info('[Skipped FB security warning] User page display started')
            else:
                # second login window
                # wait for the presence of the user ID (or e-mail) text input field.
                l_stage = 5
                l_userInput = WebDriverWait(self.m_driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//div/input[@id="email"]')))

                # sends the user ID string to it
                l_userInput.clear()
                # l_userInput.send_keys(p_user)
                self.key_slow(l_userInput, p_user)
                self.m_logger.info('User ID entered: {0}'.format(p_user))

                # wait for the presence of the user password (or e-mail) text input field.
                l_stage = 6
                l_pwdInput = WebDriverWait(self.m_driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//div/input[@id="pass"]')))

                # sends the password string to it
                l_pwdInput.clear()
                # l_pwdInput.send_keys(p_passwd)
                self.key_slow(l_pwdInput, p_passwd)
                self.m_logger.info('Password entered: {0}'.format(p_passwd))

                # finds the log-in button and clicks it
                l_stage = 7
                l_button = WebDriverWait(self.m_driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//div/button[@id="loginbutton"]')))
                l_button.click()
                self.m_logger.info('Login button clicked')

                # wait for the presence of the `mainContainer` element, indicating post login page load
                l_stage = 8
                WebDriverWait(self.m_driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@id="mainContainer"]')))

                self.m_logger.info('User page display started')

        except (EX.TimeoutException, EX.NoSuchElementException) as e:
            if type(e) == EX.TimeoutException:
                self.m_logger.critical(
                    'Timeout during a WebDriverWait. l_stage : {0}'.format(l_stage))
            else:
                self.m_logger.critical(
                    'NoSuchElementException. l_stage : {0}'.format(l_stage))

            self.m_logger.info('Current URL: {0}'.format(self.m_driver.current_url))
            self.dump_html()
            l_img = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
            l_img.save('./images_error/login_as_scrape_failure_{0}_{1}.png'.format(p_user,
                datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))

            # "Current URL: https://www.facebook.com/mobileprotection?source=mobile_mirror_nux"
            if re.search(r'mobileprotection', self.m_driver.current_url):
                self.log_out()

            if self.m_vpn_handle is not None:
                self.m_vpn_handle.close()
                self.m_vpn_handle = None

            raise

        # creation date/time (for staleness)
        self.m_creationDate = datetime.datetime.now(tz=pytz.utc)
        l_lifespan = EcAppParam.gcm_bdLifeAverage + \
                     (EcAppParam.gcm_bdLifeDiameter / 2.0 - random.random() * EcAppParam.gcm_bdLifeDiameter)
        self.m_expirationDate = datetime.datetime.now(tz=pytz.utc) + datetime.timedelta(hours=l_lifespan)
        self.m_logger.info('lifespan: {0} hours'.format(l_lifespan))

        self.m_loggedIn = True
        self.m_phantomID = p_user

    def key_slow(self, p_element, p_text):
        for l in p_text:
            p_element.send_keys(l)
            self.m_logger.info(l)
            time.sleep(random.randint(30, 70)/100.0)

    def log_out(self):
        """
        Logs out of facebook when logged-in as a scraper.
        
        :return: Nothing. 
        """
        try:
            # wait for the presence of the settings arrow down button
            l_settings = WebDriverWait(self.m_driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//a[contains(@class, "_5lxs")]')))

            l_settings.click()
        except EX.TimeoutException:
            self.m_logger.critical('Did not find settings arrow down button')
            self.m_logger.info('Current URL: {0}'.format(self.m_driver.current_url))
            self.dump_html()
            l_img = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
            l_img.save('./images_error/login_as_scrape_failure_{0}_{1}.png'.format(p_user,
                datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))

            raise

        try:
            # wait for the presence of the log-out button
            l_logoutButton = WebDriverWait(self.m_driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//a[@class="_54nc" and contains(@data-gt, "menu_logout")]')))

            l_logoutButton.click()
        except EX.TimeoutException:
            self.m_logger.critical('Did not find logout button')
            self.m_logger.info('Current URL: {0}'.format(self.m_driver.current_url))
            self.dump_html()
            l_img = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
            l_img.save('./images_error/login_as_scrape_failure_{0}_{1}.png'.format(p_user,
                datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
            raise

        # close vpn if present
        if self.m_vpn_handle is not None:
            self.m_vpn_handle.close()
            self.m_vpn_handle = None

        self.m_loggedIn = False
        self.m_phantomID = ''

    def refresh_page(self):
        """
        Re-loads the current page. To be used when the Internet connection is recovered after an interruption.
        
        :return: Nothing 
        """
        self.m_driver.refresh()
        time.sleep(30)

    def isLoggedIn(self):
        """
        Self explanatory.
        
        :return: True if logged-in as a scraper. 
        """
        return self.m_loggedIn

    def dump_html(self):
        """
        Retrieves the whole HTML of the currently displayed page.
        
        :return: The HTML. 
        """
        l_html = self.m_driver.find_element_by_xpath('//html').get_attribute('outerHTML')
        with open(datetime.datetime.now().strftime('%Y%m%d_%H%M%S.html'), 'w') as f:
            f.write(l_html)

    def go_random(self):
        """
        Selects a user ID from TB_USER and display its FB profile
        :return: The chosen user ID
        """
        # connects to the database
        l_connect1 = psycopg2.connect(
            host=EcAppParam.gcm_dbServer,
            database=EcAppParam.gcm_dbDatabase,
            user=EcAppParam.gcm_dbUser,
            password=EcAppParam.gcm_dbPassword
        )

        # TB_USER total row count
        l_count = 0
        l_query = """
                    select count(1)
                    from
                        "TB_USER"
                    ;"""

        l_cursor = l_connect1.cursor()
        l_cursor.execute(l_query)
        for l_count, in l_cursor:
            pass

        # choose random row
        l_row_choice = random.randrange(l_count)

        self.m_logger.info('User count: {0}'.format(l_count))
        self.m_logger.info('Row choice: {0}'.format(l_row_choice))

        # retrieve ID of chosen row
        l_id = ''
        l_query = """
                    select
                        "ID"
                        , "ID_INTERNAL"
                        , "ST_USER_ID"
                    from
                        "TB_USER"
                    order by
                        "ID_INTERNAL"
                    offset {0};""".format(l_row_choice)

        l_cursor = l_connect1.cursor()

        # timing counter
        t0 = time.perf_counter()
        l_cursor.execute(l_query)
        self.m_logger.info('execute(): {0:.3}'.format(time.perf_counter() - t0))
        l_id_internal = None
        l_userId = None
        for l_id, l_id_internal, l_userId in l_cursor:
            break

        # close database connection
        l_connect1.close()

        self.m_logger.info('FB ID: {0}'.format(l_id))
        self.m_logger.info('FB UID: {0}'.format(l_userId))

        # go to the user's page and wait for the page to load
        self.go_to_id(l_id, l_userId, l_id_internal)

        return l_id

    def freshen_token(self, p_request):
        """
        Replace the API access token present in the given request by the one stored in :any:`self.m_token_api`.
         
        :param p_request: The request to be freshened up. 
        :return: The freshened up request.
        """
        l_request = re.sub(r'access_token=[^&]+($|&)', r'___ACCESS_TOKEN___\1', p_request)

        if not re.search(r'___ACCESS_TOKEN___', l_request):
            self.m_logger.critical('Unable to locate token within request: ' + l_request)
            raise BrowserDriverException('Unable to locate token within request: ' + l_request)

        l_request = re.sub(r'___ACCESS_TOKEN___',
                      r'access_token={0}'.format(self.m_token_api), l_request)
        return l_request

    def renew_token_and_request(self, p_request):
        """
        Get a new token (and store it) and then update the given request, to make sure its embedded token 
        is also updated.
        
        :param p_request: API request to be freshened up. 
        :return: The freshened up request.
        """
        self.get_fb_token()
        return self.freshen_token(p_request)

    def get_fb_token(self):
        """
        Get an FB API access token by logging into a bogus application and retrieving its token.
        
        :return: The token (also stored in :any:`self.m_token_api` 
        """
        # tries 10 times to log out
        if self.m_token_api != '':
            l_error_list = []
            l_loop_count = 0
            while True:
                try:
                    self.logout_api()
                    break
                except Exception as e:
                    if self.internet_check():
                        l_error_list.append(repr(e))
                        l_loop_count += 1
                        self.m_logger.debug('[logout attempt] l_loop_count: {0}'.format(l_loop_count))
                        if l_loop_count >= 10:
                            raise BrowserDriverException('Cannot Log out: {0}'.format(l_error_list))
                        else:
                            time.sleep(60)
                    else:
                        time.sleep(60 * 5)
                        self.refresh_page()

        # tries 10 times to log in and get a new token
        l_error_list = []
        l_loop_count = 0
        l_accessToken = None
        while True:
            try:
                self.m_logger.info('User/pass: {0}/{1}'.format(self.m_user_api, self.m_pass_api))
                l_accessToken = self.loginAsAPI(self.m_user_api, self.m_pass_api)
                break
            except Exception as e:
                if self.internet_check():
                    l_error_list.append(repr(e))
                    l_loop_count += 1
                    self.m_logger.debug('[login attempt] l_loop_count: {0}'.format(l_loop_count))
                    if l_loop_count >= 10:
                        self.m_logger.critical('Cannot Log in: {0}'.format(l_error_list))
                        break
                    else:
                        time.sleep(60)
                else:
                    time.sleep(60 * 5)
                    self.refresh_page()

        if l_accessToken is not None:
            self.m_logger.info('g_FBToken before: {0}'.format(self.m_token_api))
            self.m_token_api = l_accessToken
            self.m_token_expiry = datetime.datetime.now(tz=pytz.utc) + datetime.timedelta(hours=1)
            self.m_logger.info('g_FBToken new   : {0}'.format(self.m_token_api))
        else:
            self.m_logger.warning('Cannot obtain FB Token for:' + self.m_user_api)
            raise BrowserDriverException('Cannot obtain FB Token for:' + self.m_user_api)

        return l_accessToken

    def logout_api(self):
        l_iframe = WebDriverWait(self.m_driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//span/iframe')))
        self.m_logger.info('iframe name:' + l_iframe.get_attribute('name'))
        self.m_driver.switch_to.frame(l_iframe.get_attribute('name'))

        l_button = self.m_driver.find_element_by_xpath('//td/span[@class="_4z_9"]/span')
        l_buttonText = l_button.text
        self.m_logger.info('l_buttonText:' + l_buttonText[0:50])

        if re.match('Log Out', l_buttonText):
            l_button.click()
        else:
            self.m_logger.warning('Cannot log out; button text mismatch: ' + l_buttonText)
            raise BrowserDriverException('Cannot log out; button text mismatch: ' + l_buttonText)

        self.m_logger.info('Log out successful')

    def loginAsAPI(self, p_user, p_passwd):
        self.m_logger.info('start loginAsAPI()')
        self.m_driver.get(EcAppParam.gcm_api_login_url)

        try:
            l_status = WebDriverWait(self.m_driver, 15).until(EC.presence_of_element_located((By.ID, 'status')))

            # l_status = l_driver.find_element_by_id('status')
            while l_status.text != 'Please log into Facebook.':
                self.m_logger.info('loginAsAPI: Waiting for correct status')
                time.sleep(1)

            l_mainWindowHandle = None
            for l_handle in self.m_driver.window_handles:
                self.m_driver.switch_to.window(l_handle)
                self.m_logger.info('Window: {0} {1}'.format(l_handle, self.m_driver.title))

                l_mainWindowHandle = l_handle

            l_iframe = self.m_driver.find_element_by_xpath('//span/iframe')
            self.m_logger.info('iframe name:' + l_iframe.get_attribute('name'))
            self.m_driver.switch_to.frame(l_iframe.get_attribute('name'))

            l_button = self.m_driver.find_element_by_xpath('//td/span[@class="_4z_9"]/span')
            l_buttonText = l_button.text
            self.m_logger.info('l_buttonText:' + l_buttonText[0:50])

            if re.match('Log In', l_buttonText):
                time.sleep(2)
                l_button.click()
            else:
                self.m_logger.critical('Cannot log in; button text mismatch: ' + l_buttonText)
                l_body = self.m_driver.find_element_by_xpath('//body').get_attribute('innerHTML')
                self.m_logger.info('Whole HTML of <body>: ' + l_body)
                raise BrowserDriverException('Cannot log in; button text mismatch: ' + l_buttonText)

            # Handle log-in pop-up
            l_finished = False
            while not l_finished:
                for l_handle in self.m_driver.window_handles:
                    self.m_driver.switch_to.window(l_handle)
                    self.m_logger.info('Window: {0} {1}'.format(l_handle, self.m_driver.title))

                    if self.m_driver.title == 'Facebook':
                        self.m_logger.info('Found Login window')
                        l_finished = True

                        try:
                            # locate the user name (email) input box and enter the user name
                            l_user = WebDriverWait(self.m_driver, 10).until(EC.presence_of_element_located(
                                (By.ID, 'email')))
                            l_user.clear()
                            l_user.send_keys(p_user)
                            self.m_logger.info('p_user: ' + p_user)

                            # locate the password input box and enter the apssword
                            l_pwd = self.m_driver.find_element_by_id('pass')
                            l_pwd.clear()
                            l_pwd.send_keys(p_passwd)
                            self.m_logger.info('p_passwd: ' + p_passwd)

                            # time.sleep(5)
                            # submit the form
                            self.m_driver.find_element_by_id('loginbutton').click()
                        except EX.NoSuchElementException:
                            self.m_logger.critical('[01] Something is badly wrong (Element not found) ...')
                            raise BrowserDriverException('[01] Something is badly wrong (Element not found) ...')
                        except EX.TimeoutException:
                            self.m_logger.critical('[02] Something is badly wrong (Timeout) ...')
                            raise BrowserDriverException('[02] Something is badly wrong (Timeout) ...')
                        except Exception as e:
                            self.m_logger.critical('[03] Something is badly wrong (Unknown): {0}'.format(repr(e)))
                            self.dump_html()
                            l_img = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
                            l_img.save(
                                './images_error/login_as_API_failure_{0}.png'.format(
                                    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
                            raise BrowserDriverException(
                                '[03] Something is badly wrong (Unknown): {0}'.format(repr(e)))

                        break

            # Handle permission pop-up if any or moves on after 10 s
            time.sleep(1)
            l_finished = False
            l_count = 0
            while not l_finished and l_count < 5:
                for l_handle in self.m_driver.window_handles:
                    self.m_driver.switch_to.window(l_handle)
                    self.m_logger.info('Window: {0} {1}'.format(l_handle, self.m_driver.title))

                    if self.m_driver.title == 'Log in with Facebook':
                        self.m_logger.info('Found Permissions Window')

                        # Approve
                        self.m_driver.find_element_by_name('__CONFIRM__').click()

                        l_finished = True

                time.sleep(1)
                l_count += 1

            self.m_driver.switch_to.window(l_mainWindowHandle)

            # retrieve token value from the status line
            l_count = 0
            while len(l_status.text.split('|')) < 2:
                self.m_logger.info('Waiting for status update in login page: {0}'.format(l_count))
                time.sleep(.1)
                l_count += 1
                if l_count >= 25:
                    self.m_logger.critical('Could not retrieve token in status line')
                    self.dump_html()
                    l_img = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
                    l_img.save(
                        './images_error/login_as_API_failure_{0}.png'.format(
                            datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
                    raise BrowserDriverException('Could not retrieve token in status line')

            l_accessToken = l_status.text.split('|')[1]

        except EX.TimeoutException:
            self.m_logger.critical('Did not find status line')
            self.dump_html()
            l_img = Image.open(io.BytesIO(self.m_driver.get_screenshot_as_png()))
            l_img.save('./images_error/login_as_API_failure_{0}.png'.format(
                datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
            raise BrowserDriverException('Did not find status line')

        self.m_logger.info('Successfully logged in as [{0}]'.format(p_user))
        self.m_logger.info('End loginAsAPI()')
        return l_accessToken

    def go_to_id(self, p_id, p_userId, p_idInternal=None, p_name='<Unknown>', p_type='Page'):
        """
        Go to the specified user page. Both `p_id` and `p_userId` cannot be `None`.

        :param p_id: Can be either a numeric ID or `None`.
        :param p_userId: Can be either a string user ID or `None`.
        :param p_idInternal: Internal ID in `TB_USER`.
        :param p_name: Name of the user (used only if the record is not found in `TB_USER` and must be created).
        :param p_type: 'User' or 'Page' (used only if the record is not found in `TB_USER` and must be created).
        :return: Nothing.
        """

        if p_userId is None:
            self.m_driver.get('http://www.facebook.com/{0}'.format(p_id))
        else:
            self.m_driver.get('http://www.facebook.com/{0}'.format(p_userId))

        # connects to the database
        l_connect1 = psycopg2.connect(
            host=EcAppParam.gcm_dbServer,
            database=EcAppParam.gcm_dbDatabase,
            user=EcAppParam.gcm_dbUser,
            password=EcAppParam.gcm_dbPassword
        )

        l_id_internal = None
        if p_idInternal is None:
            # find ID_INTERNAL in TB_USER
            l_cursor = l_connect1.cursor()
            l_query = """
                select
                    "ID_INTERNAL"
                from
                    "TB_USER"
                where
                    "{0}" = '{1}'
                ;""".format(
                    'ID' if p_userId is None else 'ST_USER_ID',
                    p_id if p_userId is None else p_userId,
                )

            l_cursor.execute(l_query)
            for l_id_internal, in l_cursor:
                break

            # If still not found, create the record in TB_USER
            if l_id_internal is None:
                l_id = p_id if p_id is not None else '_-RBSFB-ID-_' + p_userId
                l_now = datetime.datetime.now()
                l_cursor = l_connect1.cursor()
                l_cursor.execute("""
                    insert into "TB_USER" ("ID", "ST_NAME", "DT_CRE", "DT_MSG", "ST_USER_ID", "ST_TYPE")
                    values (%s, %s, %s, %s, %s, %s)
                    ;""", (l_id, p_name, l_now, l_now, p_userId, p_type))
                l_connect1.commit()

                l_cursor = l_connect1.cursor()
                l_query = """
                    select
                        "ID_INTERNAL"
                    from
                        "TB_USER"
                    where
                        "ID" = '{0}'
                    ;""".format(l_id)
                l_cursor.execute(l_query)
                for l_id_internal, in l_cursor:
                    break
        else:
            l_id_internal = p_idInternal

        # retrieve user ID from url if not already known
        if p_userId is None:
            l_url = self.m_driver.current_url
            self.m_logger.info('Current url [{0}]'.format(l_url))

            l_userId = re.sub('^https://www\.facebook\.com/', '', l_url)
            l_userId = re.sub('^profile\.php\?', '', l_userId)
            self.m_logger.info('FB user ID [{0}]'.format(l_userId))

            if l_id_internal is not None:
                l_cursor = l_connect1.cursor()
                l_cursor.execute("""
                            update
                                "TB_USER"
                            set
                                "ST_USER_ID" = %s
                            where
                                "ID_INTERNAL" = %s
                            ;""", (l_userId, l_id_internal))
                l_connect1.commit()
        else:
            l_userId = p_userId

        # initiate download session
        l_now = datetime.datetime.now()
        self.m_dnl_ses_id = l_userId + '_' + l_now.strftime('%Y%m%d-%H%M%S.%f')

        l_cursor = l_connect1.cursor()
        l_cursor.execute("""
                    insert into
                        "TB_SESSION" ("ID_INTERNAL", "ST_SESSION_ID", "DT_CRE")
                    values (%s, %s, %s)
                    ;""", (l_id_internal, self.m_dnl_ses_id, l_now))
        l_connect1.commit()

        # close database connection
        l_connect1.close()

        try:
            # wait for mainContainer
            WebDriverWait(self.m_driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//div[@id="mainContainer"]')))
        except EX.TimeoutException:
            self.m_logger.critical('Did not find user\'s page mainContainer. Id: {0}'.format(p_id))
            raise

    def scroll_obfuscate(self, y):
        """
        Perform a window scroll in several random steps emulating human behavior.

        :param y: Target scroll position.
        :return: Nothing 
        """
        l_stepCount = random.randint(5, 15)
        self.m_logger.info('Steps: {0}'.format(l_stepCount))

        for i in range(l_stepCount, 0, -1):
            d = l_stepCount * 10
            l_yTarget = y + random.randint(-d / 2, d / 2)
            self.m_driver.execute_script('window.scrollTo(0, {0});'.format(l_yTarget))
            time.sleep(.01)

        self.m_driver.execute_script('window.scrollTo(0, {0});'.format(y))

    def mouse_obfuscate(self, p_delay):
        """
        Wait for a specified delay while performing mouse moves and right-clicks.

        :param p_delay: Delay in seconds. 
        :return: Nothing
        """
        l_max_steps = 5
        if p_delay > 5:
            l_max_steps = int(p_delay)
        if l_max_steps > 15:
            l_max_steps = 15

        l_delay_steps = random.randint(5, l_max_steps)
        for i in range(l_delay_steps):
            l_body = self.m_driver.find_element_by_xpath('//body')
            l_action = ActionChains(self.m_driver)
            l_action.move_to_element(l_body)
            l_action.perform()

            l_mouse_steps = random.randint(2, 10)
            # l_mouse_steps = 0
            for j in range(l_mouse_steps):
                l_action = ActionChains(self.m_driver)
                l_offsetX = random.randint(-self.m_browserWidth / 10, self.m_browserWidth / 10)
                l_offsetY = random.randint(-self.m_browserHeight / 10, self.m_browserHeight / 10)

                # self.m_logger.info('{0} Offset: [{1}, {2}]'.format(j, l_offsetX, l_offsetY))

                l_action.move_by_offset(l_offsetX, l_offsetY)
                l_action.perform()
                time.sleep(.01)

            l_action = ActionChains(self.m_driver)
            l_action.context_click()
            l_action.perform()
            time.sleep(p_delay / l_delay_steps)

    def make_visible_and_click(self, p_object):
        """
        Make sure an element is visible before clicking it.

        :param p_object: WebDriver element to be clicked. 
        :return: Nothing
        """
        l_scrollDone = False
        l_loopCount = 0
        while True:
            l_yTop1 = self.m_driver.execute_script('return window.pageYOffset;')
            l_yTop2 = self.m_driver.execute_script('return window.scrollY;')

            if l_yTop1 == l_yTop2:
                l_yTop = l_yTop1
            else:
                self.m_logger.warning('l_yTop1/l_yTop2: {0}/{1}'.format(l_yTop1, l_yTop2))
                l_yTop = l_yTop2

            # getBoundingClientRect
            l_delta_y_js = self.m_driver.execute_script(
                'return arguments[0].getBoundingClientRect().top;', p_object)

            l_yComment = p_object.location['y']
            l_delta_y = l_yComment - l_yTop
            l_yTarget = l_yComment - 300
            if l_delta_y != l_delta_y_js:
                self.m_logger.warning('l_delta_y_js/l_delta_y: {0}/{1}'.format(l_delta_y_js, l_delta_y))

            self.m_logger.info(
                '[{0}] l_yTop/l_yComment/l_yTarget/l_delta_y/l_delta_y_js: {1}/{2}/{3}/{4}/{5}'.format(
                    l_loopCount, l_yTop, l_yComment, l_yTarget, l_delta_y, l_delta_y_js))

            # perform click if object is in visibility range
            if (l_delta_y > 250) and (l_delta_y < self.m_browserHeight - 200):
                try:
                    # click the link
                    WebDriverWait(self.m_driver, 10).until(EC.visibility_of(p_object))
                    p_object.click()
                    break
                except EX.WebDriverException as e:
                    self.m_logger.info('Error: ' + repr(e))

            # execute the scroll commands only once
            if not l_scrollDone:
                # self.m_driver.execute_script("arguments[0].scrollIntoView();", l_commentLink)
                # self.m_driver.execute_script('window.scrollBy(0, {0});'.format(-200))

                self.m_driver.execute_script('window.scrollTo(0, {0});'.format(l_yTarget))
                self.m_logger.info('ScrollTo: {0} Done'.format(l_yTarget))
                l_scrollDone = True
            else:
                l_scrollValue = self.m_browserHeight - 500
                if l_delta_y < 0:
                    l_scrollValue = - l_scrollValue
                self.m_driver.execute_script('window.scrollBy(0, {0});'.format(l_scrollValue))
                self.m_logger.info('ScrollBy: {0} Done'.format(l_scrollValue))

            time.sleep(.1)
            l_loopCount += 1
            if l_loopCount >= 100:
                self.m_logger.warning('Unable to do make_visible_and_click()')
                return False

        # end while True:
        return True

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

    # l_phantomId0 = 'nicolas.reimen@gmail.com'
    # l_phantomPwd0 = 'murugan!'
    l_phantomId0, l_phantomPwd0 = 'kabir.abdulhami@gmail.com', '12Alhamdulillah',
    # l_vpn = 'India.Maharashtra.Mumbai.TCP.ovpn'
    l_vpn0 = None

    l_driver = BrowserDriver()
    # l_driver.login_as_scrape(l_phantomId0, l_phantomPwd0, l_vpn0)
    # print('Token:', l_driver.loginAsAPI(l_phantomId0, l_phantomPwd0))

    # time.sleep(3)

    # l_driver.logout_api()
    #l_driver.go_random()
    #l_driver.log_out()

    l_driver.m_user_api = l_phantomId0
    l_driver.m_pass_api = l_phantomPwd0
    print(l_driver.get_fb_token())

    if EcAppParam.gcm_headless:
        l_driver.close()