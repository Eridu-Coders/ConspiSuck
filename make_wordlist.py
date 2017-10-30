#!/usr/bin/python3
# -*- coding: utf-8 -*-

from rbs_fb_connect import *
from wrapvpn import *

__author__ = 'Pavan Mahalingam'

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
                database='FBWatch',
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

    try:
        l_connect0 = psycopg2.connect(
            host=EcAppParam.gcm_dbServer,
            database='FBWatch',
            user=EcAppParam.gcm_dbUser,
            password=EcAppParam.gcm_dbPassword
        )

        l_cursor = l_connect0.cursor()
        l_cursor.execute("""
            select "A"."ST_WORD", count(1) as "WCOUNT" 
            from(
                select
                    regexp_replace("ST_WORD", '[\.,;:\?!]+$'::text, ''::text, 'g'::text) AS "ST_WORD"
                from "FBWatch"."TB_CONCORD"
            ) "A"
            group by "A"."ST_WORD"
            order by "A"."ST_WORD";
        """)

        for l_word, l_count in l_cursor:
            if l_count < 2:

                continue
            l_match = re.search(r'([a-zA-Z]+[\'’][a-zA-Z]+|[a-zA-Z]+)[\.,;:\?!]*', l_word)
            if l_match and (l_match.group(0) == l_word):
                #EcLogger.cm_logger.info('{0:6} {1}'.format(l_count, l_word))
                print(l_word)

        l_cursor.close()
        l_connect0.close()
    except psycopg2.Error as e0:
        EcLogger.cm_logger.warning('PostgreSQL Error: {0}'.format(repr(e0)))

