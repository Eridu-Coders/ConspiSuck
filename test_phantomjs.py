#!/usr/bin/python3
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common import exceptions as EX

__author__ = 'Pavan Mahalingam'

# opens a Selenium driven Firefox window
def getDriver():
    if g_browser == 'Firefox':
        # Create a new instance of the Firefox driver
        l_driver = webdriver.Firefox()
    elif g_browser == 'HtmlUnit':
        # Create a new instance of the Firefox driver
        l_driver = webdriver.Remote("http://localhost:4444/wd/hub", webdriver.DesiredCapabilities.HTMLUNIT.copy())
    else:
        # Create a new instance of the PhantomJS driver
        l_driver = webdriver.PhantomJS()

    # Resize the window to the screen width/height
    l_driver.set_window_size(1200, 1100)

    # Move the window to position x/y
    l_driver.set_window_position(800, 0)

    return l_driver

def loginAsScrape(p_user, p_passwd):
    l_driver = getDriver()

    l_driver.get('http://www.facebook.com')

    try:
        l_userInput = WebDriverWait(l_driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//td/input[@id="email"]')))

        l_userInput.send_keys(p_user)

        l_pwdInput = WebDriverWait(l_driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//td/input[@id="pass"]')))

        l_pwdInput.send_keys(p_passwd)

        # loginbutton
        l_driver.find_element_by_xpath('//label[@id="loginbutton"]/input').click()

        # wait for mainContainer
        WebDriverWait(l_driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//div[@id="mainContainer"]')))
    except EX.TimeoutException:
        print('Did not find user ID input or post-login mainContainer')
        return None

    return l_driver

def doScrapeTest(p_id, p_pwd):
    print('***** {0} ******'.format(g_browser))
    print('Log in')

    l_driver = loginAsScrape(p_id, p_pwd)

    print('Finding posts')
    try:
        for l_postText in l_driver.find_elements_by_xpath('//div[@class="_5pbx userContent"]'):
            print('>>> {0}\n--------------'.format(l_postText.text))

    except EX.NoSuchElementException:
        print('Nothing found')

# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| FB scraping web service for ROAD B SCORE                   |')
    print('|                                                            |')
    print('| PhantomJS / HtmlUnit test                                  |')
    print('|                                                            |')
    print('| v. 1.0 - 22/02/2017                                        |')
    print('+------------------------------------------------------------+')

    l_phantomId = 'kabir.eridu@gmail.com'
    l_phantomPwd = '12Alhamdulillah'

    g_browser = 'PhantomJS'
    doScrapeTest(l_phantomId, l_phantomPwd)


