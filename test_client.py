#!/usr/bin/python3
# -*- coding: utf-8 -*-

import requests

__author__ = 'Pavan Mahalingam'

# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| FB scraping web service for ROAD B SCORE                   |')
    print('|                                                            |')
    print('| POST request sending test client                           |')
    print('|                                                            |')
    print('| v. 1.0 - 21/02/2017                                        |')
    print('+------------------------------------------------------------+')

    r = requests.post('http://192.168.0.51:9080/', {'toto': 'tutu'})
    print('   Status Code: {0}'.format(r.status_code))

    l_response = r.text
    print('   >> ' + l_response)
