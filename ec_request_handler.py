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

    #: The application that the handler must call to build the response
    cm_app = None

    #: Counter, for creating handler instance IDs
    cm_handlerCount = 0

    @classmethod
    def init_class(cls, p_app):
        """
        Initialize the class.

        :param p_app: Handle on the application.
        :return: Nothing
        """
        l_logger = logging.getLogger('EcRequestHandler_Init')

        l_logger.info("Initializing EcRequestHandler class")

        # link to app
        cls.cm_app = p_app

        l_logger.info("EcRequestHandler class Initialization complete")

    def __init__(self, p_request, p_client_address, p_server):
        """
        Reimplementation of the :any:`http.server.SimpleHTTPRequestHandler` constructor.

        :param p_request: The request (:any:`http.server.SimpleHTTPRequestHandler` parameter)
        :param p_client_address: The caller's IP address (:any:`http.server.SimpleHTTPRequestHandler` parameter)
        :param p_server: The server instance (:any:`http.server.SimpleHTTPRequestHandler` parameter)
        """
        #: instance ID
        self.m_handlerID = EcRequestHandler.cm_handlerCount
        EcRequestHandler.cm_handlerCount += 1

        #: logger
        self.m_logger = logging.getLogger('EcRequestHandler #{0}'.format(self.m_handlerID))

        # final message
        self.m_logger.info('------------ request handler #{0} created ----------------------'.format(self.m_handlerID))

        super().__init__(p_request, p_client_address, p_server)

    def log_message(self, *args):
        """
        Reimplementation of a :any:`http.server.SimpleHTTPRequestHandler` method which takes care of logging
        messages to the console. Since it does nothing --> no messages.

        :param args: The :any:`http.server.SimpleHTTPRequestHandler` list of arguments for this method.
        :return: Nothing.
        """
        pass

    # nothing to do here, except maybe logging
    def do_HEAD(self):
        """
        Reimplementation the :any:`http.server.SimpleHTTPRequestHandler` taking care of `HEAD` requests. Does nothing
        except log the request.

        :return: Nothing
        """
        self.m_logger.info('Received HEAD request')
        super().do_HEAD()

    # GET HTTP request
    def do_GET(self):
        """
        Reimplementation the :any:`http.server.SimpleHTTPRequestHandler` taking care of `GET` requests. The important
        stuff happens here. Most importantly, calls the :any:`EcAppCore.get_responseGet()` method (in fact, its
        reimplementation by the actual app, if any) which builds the `HTML` response to the request.

        :return: Nothing.
        """
        self.m_logger.info('Received GET request')
        # super().do_GET()

        # response code and MIME type
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        # call the rest of the app to get the appropriate response
        l_response = EcRequestHandler.cm_app.get_response_get(self)

        # and send it
        self.wfile.write(bytes(l_response, 'utf-8'))

    # POST HTTP request
    # noinspection PyPep8Naming
    def do_POST(self):
        """
        Reimplementation the :any:`http.server.SimpleHTTPRequestHandler` taking care of `GET` requests. The important
        stuff happens here. Most importantly, calls the :any:`EcAppCore.get_responsePost()` method (in fact, its
        reimplementation by the actual app, if any) which builds the `HTML` response to the request.

        :return: Nothing.
        """
        self.m_logger.info('Received POST request')

        # retrieves POSTed data
        l_data_length = int(self.headers['content-length'])
        self.m_logger.debug('POST data length : {0}'.format(l_data_length))
        l_data = self.rfile.read(l_data_length)
        self.m_logger.debug('POST data: {0}'.format(l_data))

        # response code and MIME type
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # call the rest of the app to get the appropriate response
        l_response = EcRequestHandler.cm_app.get_responsePost(self, l_data)

        # and send it
        self.wfile.write(bytes(l_response, 'utf-8'))
