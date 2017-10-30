#!/usr/bin/python3
# -*- coding: utf-8 -*-

import http.server
import logging

__author__ = 'Pavan Mahalingam'


# ----------------------------------------- New Request Handler --------------------------------------------------------
class EcRequestHandler(http.server.SimpleHTTPRequestHandler):
    """
    HTTP request handler. Subclass of :any:`http.server.SimpleHTTPRequestHandler` from python std. lib.
    """

    # the application that the handler must call to build the response
    cm_app = None

    # counter, for creating handler instance IDs
    cm_handlerCount = 0

    @classmethod
    def init_class(cls, p_app):
        l_logger = logging.getLogger('EcRequestHandler_Init')

        l_logger.info("Initializing EcRequestHandler class")

        # link to app
        cls.cm_app = p_app

        l_logger.info("EcRequestHandler class Initialization complete")

    def __init__(self, p_request, p_client_address, p_server):
        # instance ID
        self.m_handlerID = EcRequestHandler.cm_handlerCount
        EcRequestHandler.cm_handlerCount += 1

        # logger
        self.m_logger = logging.getLogger('EcRequestHandler #{0}'.format(self.m_handlerID))

        # final message
        self.m_logger.info('------------ request handler #{0} created ----------------------'.format(self.m_handlerID))

        super().__init__(p_request, p_client_address, p_server)

    # disable logging to std output
    def log_message(self, *args):
        pass

    # nothing to do here, except maybe logging
    def do_HEAD(self):
        self.m_logger.info('Received HEAD request')
        super().do_HEAD()

    # GET HTTP request
    def do_GET(self):
        self.m_logger.info('Received GET request')
        # super().do_GET()

        # response code and MIME type
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        # call the rest of the app to get the appropriate response
        l_response = EcRequestHandler.cm_app.get_responseGet(self)

        # and send it
        self.wfile.write(bytes(l_response, 'utf-8'))

    # POST HTTP request
    def do_POST(self):
        self.m_logger.info('Received POST request')

        # retrieves POSTed data
        l_dataLength = int(self.headers['content-length'])
        self.m_logger.debug('POST data length : {0}'.format(l_dataLength))
        l_data = self.rfile.read(l_dataLength)
        self.m_logger.debug('POST data: {0}'.format(l_data))

        # response code and MIME type
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # call the rest of the app to get the appropriate response
        l_response = EcRequestHandler.cm_app.get_responsePost(self, l_data)

        # and send it
        self.wfile.write(bytes(l_response, 'utf-8'))


