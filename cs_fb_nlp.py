#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unicodedata
from ec_utilities import *

__author__ = 'Pavan Mahalingam'


class Dummy:
    @staticmethod
    def get_categories(p_string, p_separator=' '):
        l_retval = ''
        for c in p_string:
            l_category = unicodedata.category(c)
            if l_category != 'Zs':
                l_retval += '{0}/{1}/{2}{3}'.format(
                    c, l_category, UnicodeBlockInfo.get_block_for_codepoint(c), p_separator)

        return l_retval.strip()

# ---------------------------------------------------- Main section ---------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| FB scraping web service for ROAD B SCORE                   |')
    print('|                                                            |')
    print('| ec_utilities module test                                   |')
    print('|                                                            |')
    print('| v. 1.0 - 20/02/2017                                        |')
    print('+------------------------------------------------------------+')

    UnicodeBlockInfo.class_init()

    l_string = '&}]@^\`|[{#~²£$¤µ§®©«¶¼°±ª💵🤣👋🏼😉😂🚫🙌😍🙈💁🏻‍♂️'
    print(l_string + ' ---> \n' + Dummy.get_categories(l_string, p_separator='\n'))
