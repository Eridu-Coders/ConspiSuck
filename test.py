#!/usr/bin/python3
# -*- coding: utf-8 -*-

__author__ = 'Pavan Mahalingam'

import requests
import json
import random

def send(p_AT, p_pageID):
    l_pageId = p_pageID
    l_accessToken = p_AT

    print('------------ SEND -----------------')
    # page info (not needed, just for checking)
    l_request = ('https://graph.facebook.com/{0}/{1}?access_token={2}' +
                 '&fields=id,name,access_token').format('v2.8', l_pageId, l_accessToken)
    print('-->' + l_request)

    r = requests.get(l_request)
    print('   Status Code: {0}'.format(r.status_code))

    l_response = r.text
    print('   >> ' + l_response)

    # posting message
    l_request = ('https://graph.facebook.com/{0}/{1}/feed?access_token={2}').format('v2.8', l_pageId, l_accessToken)
    print('-->' + l_request)

    l_message = 'Message number: {0}'.format(random.randint(1,1000))
    print('   Message: ' + l_message)
    r = requests.post(l_request, {'message': l_message})
    print('   Status Code: {0}'.format(r.status_code))

    l_response = r.text
    print('   >> ' + l_response)


def receive(p_AT, p_pageID):
    l_pageId = p_pageID
    l_accessToken = p_AT

    print('------------ RECEIVE -----------------')

    # page info (in this case it is needed, in order to retrieve the page access token)
    l_request = ('https://graph.facebook.com/{0}/{1}?access_token={2}' +
                 '&fields=id,name,access_token').format('v2.8', l_pageId, l_accessToken)
    print('-->' + l_request)

    r = requests.get(l_request)
    print('   Status Code: {0}'.format(r.status_code))

    l_response = r.text
    print('   >> ' + l_response)

    # page access token
    l_pageAT = None
    if r.status_code == 200:
        l_responseData = json.loads(l_response)
        l_pageAT = l_responseData['access_token']
        print('Page Access Token : {0}'.format(l_pageAT))

    # page feed
    l_request = ('https://graph.facebook.com/{0}/{1}/feed?access_token={2}').format('v2.8', l_pageId, l_accessToken)
    print('-->' + l_request)

    r = requests.get(l_request)
    print('   Status Code: {0}'.format(r.status_code))

    l_response = r.text
    print('   >> ' + l_response)

    # retrieve post ID
    if r.status_code == 200:
        l_responseData = json.loads(l_response)
        # here I assume that the post is the first in the feed. In practice, it may be a little more complicated
        l_postId = l_responseData['data'][0]['id']
        print('Post ID : {0}'.format(l_postId))

        l_request = ('https://graph.facebook.com/{0}/{1}?access_token={2}' +
                     '&fields=id,message,from').format('v2.8', l_postId, l_accessToken)
        print('-->' + l_request)

        r = requests.get(l_request)
        print('   Status Code: {0}'.format(r.status_code))

        l_response = r.text
        print('   >> ' + l_response)

        # retrieve author ID
        if r.status_code == 200:
            l_responseData = json.loads(l_response)
            l_personId = l_responseData['from']['id']

            print('Person ID : {0}'.format(l_personId))

            l_request = ('https://graph.facebook.com/{0}/{1}?fields={3}&access_token={2}').format(
                'v2.8', l_personId, l_accessToken, 'id,name,about,age_range,birthday')
            print('-->' + l_request)

            r = requests.get(l_request)
            print('   Status Code: {0}'.format(r.status_code))

            l_response = r.text
            print('   >> ' + l_response)

            l_request = ('https://graph.facebook.com/{0}/{1}/feed?access_token={2}').format(
                'v2.8', l_personId, l_accessToken)
            print('-->' + l_request)

            r = requests.get(l_request)
            print('   Status Code: {0}'.format(r.status_code))

            l_response = r.text
            print('   >> ' + l_response)

        # post deletion (optional. If not done, then page access token not needed)
        if l_pageAT is not None:
            l_request = ('https://graph.facebook.com/{0}/{1}?access_token={2}').format('v2.8', l_postId, l_pageAT)
            print('-->' + l_request)

            r = requests.get(l_request)
            print('   Status Code: {0}'.format(r.status_code))

            l_response = r.text
            print('   >> ' + l_response)


# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| FB scraping web service for ROAD B SCORE                   |')
    print('|                                                            |')
    print('| User ID transmission test script                           |')
    print('|                                                            |')
    print('| v. 1.1 - 19/02/2017                                        |')
    print('+------------------------------------------------------------+')

    random.seed()

    l_pageID = '1644087245606719'

    # token associated with the User ID to be transmitted
    l_senderToken = 'EAACEdEose0cBAApZA9CWRefnCzpZCJj4gD1xKUnWvLsrZBBURuHE1ZBIo95XdxoQtZCxkawXXSH2ZBfuv7SBF7LWQA773mYgZBSh48ECCpeNijAzGPZASGwEV4wGoC7Bj4T6iN4FHbLw6ZB4d2v7ZB1iaSMlm8iVfSZAee4WKDCpD61hzZAcy1k1kWJ7nluLrdqcZCg0ZD'

    # token associated with a user who has admin privileges for the page (required for post deletion)
    l_receiverToken = 'EAACEdEose0cBAHZCZA6GUrbVURTwquwZCSavoVrNAC0RTWjLvuyjESVo9LO4ia7Vb9wTfb3DYcxgzIqdIcfZBCMcZC76ZCZBTznsZAv0b9uUnlHADPdTKRXs3j6gcMcgrcQc8ulFwnpnZBoKCbuhVQAuGriTvxYHKoQCvRWV0WOTlffvh9NQtCik1GTsgF5Pmql4ZD'

    send(l_senderToken, l_pageID)
    receive(l_receiverToken, l_pageID)

    # retrieves sender ID
    l_request = ('https://graph.facebook.com/{0}/me?fields={2}&access_token={1}').format(
        'v2.8', l_senderToken, 'id,name,about,age_range,birthday')
    print('-->' + l_request)

    r = requests.get(l_request)
    print('   Status Code: {0}'.format(r.status_code))

    l_response = r.text
    print('   >> ' + l_response)

    if r.status_code == 200:
        l_responseData = json.loads(l_response)
        l_personId = l_responseData['id']

        l_request = ('https://graph.facebook.com/{0}/{1}?fields={3}&access_token={2}').format(
            'v2.8', l_personId, l_senderToken, 'id,name,about,age_range,birthday')
        print('-->' + l_request)

        r = requests.get(l_request)
        print('   Status Code: {0}'.format(r.status_code))

        l_response = r.text
        print('   >> ' + l_response)

        l_request = ('https://graph.facebook.com/{0}/{1}/feed?access_token={2}').format(
            'v2.8', l_personId, l_senderToken)
        print('-->' + l_request)

        r = requests.get(l_request)
        print('   Status Code: {0}'.format(r.status_code))

        l_response = r.text
        print('   >> ' + l_response)

