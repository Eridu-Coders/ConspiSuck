Database
========

* RDBMS used: `PostgreSQL <https://www.postgresql.org/>`_
* Database python interface: (`psycopg2 <http://initd.org/psycopg/>`_)

DB Connexion
------------

.. automodule:: ec_utilities

.. autoclass:: EcConnectionPool
   :members:

.. autoclass:: EcConnection
   :members:

Common Tables
-------------

* ``TB_EC_LOG``::

   CREATE TABLE public."TB_EC_LOG"
   (
     "LOG_ID" integer NOT NULL DEFAULT nextval('"TB_EC_LOG_LOG_ID_seq"'::regclass),
     "TERMINAL_ID" character varying(32),
     "DT_LOG" timestamp without time zone DEFAULT timezone('utc'::text, now()),
     "ST_IP" character varying(45),
     "N_PORT" integer,
     "F_BAD" character varying(1) NOT NULL DEFAULT 'N'::character varying,
     "TX_USER_AGENT" text,
     "ST_BROWSCAP_DATA" text,
     "TX_PATH" text,
     "TX_CONTEXT" text,
     CONSTRAINT "TB_EC_LOG_pkey" PRIMARY KEY ("LOG_ID")
   )

**NB**: the SQL code above is what Postgres shows after the table is created but it does not permit the creation of
the table from scratch because of the sequence in the first line. To create the table at first, replace the first line
by ``"LOG_ID" serial,``

* ``TB_EC_MSG``::

   CREATE TABLE public."TB_EC_MSG"
   (
     "MSG_ID" integer NOT NULL DEFAULT nextval('"TB_EC_LOG_LOG_ID_seq"'::regclass),
     "DT_MSG" timestamp without time zone DEFAULT timezone('utc'::text, now()),
     "ST_NAME" character varying(40),
     "ST_LEVEL" character varying(10),
     "ST_MODULE" character varying(40),
     "ST_FILENAME" text,
     "ST_FUNCTION" character varying(50),
     "N_LINE" integer,
     "TX_MSG" text,
     CONSTRAINT "TB_EC_MSG_pkey" PRIMARY KEY ("MSG_ID")
   )

**NB**: ``TB_EC_LOG`` and ``TB_EC_MSG`` share the same sequence for their IDs. As a result, they can be treated as
a single sequence of messages with 2 different formats.

* ``TB_EC_TERMINAL``::

   CREATE TABLE public."TB_EC_TERMINAL"
   (
     "TERMINAL_ID" character varying(32) NOT NULL,
     "DT_CREATION" timestamp without time zone,
     "DT_LAST_UPDATE" timestamp without time zone,
     "TX_CONTEXT" text NOT NULL,
     "F_VALIDATED" character varying(3) NOT NULL DEFAULT 'NO'::character varying,
     CONSTRAINT "TB_EC_TERMINAL_pkey" PRIMARY KEY ("TERMINAL_ID")
   )
