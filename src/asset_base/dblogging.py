#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Logging handlers module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""

from sqlalchemy import Column
from sqlalchemy.types import DateTime, Integer, String, Boolean
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

import traceback
import logging

from asset_base import get_log_path

_db_url = "sqlite:///asset_base.log.db"
_db_engine = create_engine(_db_url)
_db_session = Session(_db_engine)


# Get the base for ORM objects.
Base = declarative_base()

class FileHandler(logging.handlers.TimedRotatingFileHandler):
    """Solves the log file path problem for file logging.

    This class is a child of TimedRotatingFileHandler.

    The log file path must no longer be specified in the log configuration file
    ``logconf.yaml`` as it is set by this class. This class is a child of one of
    the standard login file handlers such as
    ``logging.TimedRotatingFileHandler``.

    Parameters
    ----------
    kwargs : dict
        The key word arguments for ``logging.TimedRotatingFileHandler``. These
        shall come from the configuration file ``logconf.yaml``

    Note
    ----
    The log file path must no longer be specified in the log configuration file
    ``logconf.yaml``

    See also
    --------
    logging.TimedRotatingFileHandler

    """

    _log_file = "asset_base.log"

    def __init__(self, **kwargs):
        """Initialization."""
        log_file_name = get_log_path(self._log_file)
        super(FileHandler, self).__init__(log_file_name, **kwargs)


class FundLog(Base):
    """SQLAlchemy ORM class to write a single Fund's pass/fail log to a table.

    Requires a Fund UUID if passed in the logger `extra` dict argument. Else a
    KeyError Exception is raised.

    Attributes
    ----------
    platform : str
        The platform name unique identifier string.
    uuid : str
        The unique fund identifier string.
    is_pass : bool
        The fund flag which indicates that the fund has passed.

    See also
    --------
    ``funds.Fund``

    """

    __tablename__ = "fundlog"

    id = Column(Integer, primary_key=True)  # auto incrementing
    created_at = Column(DateTime, default=func.now())  # the current timestamp
    platform = Column(String(45))
    uuid = Column(String(45))
    is_pass = Column(Boolean)

    def __init__(self, uuid=None, platform=None, is_pass=None):
        """Initialization."""
        if uuid is None or is_pass is None:
            raise ValueError("Expected non-None parameter values.")
        self.platform = platform
        self.uuid = uuid
        self.is_pass = is_pass


class SQLFundLogHandler(logging.Handler):
    """Handler to write fund pass/fail logs to a SQL table."""

    def test(self, platform=None, uuid=None, is_pass=None):
        """For test purposes only."""
        uuid = uuid
        is_pass = is_pass
        log = FundLog(platform=platform, uuid=uuid, is_pass=is_pass)
        _db_session.add(log)
        _db_session.commit()

    def emit(self, record):
        """Overloaded method."""
        platform = record.__dict__["platform"]
        uuid = record.__dict__["uuid"]
        is_pass = record.__dict__["is_pass"]
        log = FundLog(platform=platform, uuid=uuid, is_pass=is_pass)
        _db_session.add(log)
        _db_session.commit()


class Log(Base):
    """SQLAlchemy ORM class to write a general log to a table."""

    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)  # auto incrementing
    # the current timestamp
    created_at = Column(DateTime, default=func.now())
    # Text time when the LogRecord was created.
    asctime = Column(String(45))
    # the name of the logger. (e.g. myapp.views)
    name = Column(String(45))
    # The name of the Fund's platform object. An `extra` logging item.
    platform = Column(String(45))
    # The UUID of the Fund object. An `extra` logging item.
    uuid = Column(String(45))
    # Numeric logging level  (DEBUG, ..., CRITICAL).
    levelno = Column(Integer)
    # Text logging level ('DEBUG', ...,'CRITICAL').
    levelname = Column(String(45))
    # Module (name portion of filename).
    module = Column(String(45))
    # Filename portion of pathname.
    filename = Column(String(45))
    # Source line number where  logging call was issued.
    lineno = Column(Integer)
    # The logged message, computed as msg % args.
    message = Column(String(2048))
    # The full traceback printout
    trace = Column(String(16384))

    def __init__(
        self,
        asctime=None,
        name=None,
        levelno=None,
        levelname=None,
        module=None,
        filename=None,
        lineno=None,
        message=None,
        trace=None,
        uuid=None,
        platform=None,
    ):
        """Initialization."""
        self.asctime = asctime
        self.name = name
        self.levelno = levelno
        self.levelname = levelname
        self.module = module
        self.filename = filename
        self.lineno = lineno
        self.message = message
        self.trace = trace
        self.uuid = uuid
        self.platform = platform

    def __repr__(self):
        """Return the official string output."""
        return (
            "Log("
            "asctime=%r, "
            "name=%r, "
            "levelno=%r, "
            "levelname=%r, "
            "module=%r, "
            "filename=%r, "
            "lineno=%r, "
            "message=%r, "
            "trace=%r, "
            "uuid=%r, "
            "platform=%r)"
        ) % (
            self.asctime,
            self.name,
            self.levelno,
            self.levelname,
            self.module,
            self.filename,
            self.lineno,
            self.message,
            self.trace,
            self.uuid,
            self.platform,
        )


class SQLLogHandler(logging.Handler):
    """Handle to write a general log to a SQL table."""

    # A very basic logger that commits a LogRecord to the SQL Db
    def test(
        self,
        asctime=None,
        name=None,
        levelno=None,
        levelname=None,
        module=None,
        filename=None,
        lineno=None,
        message=None,
        trace=None,
    ):
        """For test purposes only."""
        log = Log(
            asctime=asctime,
            name=name,
            levelno=levelno,
            levelname=levelname,
            module=module,
            filename=filename,
            lineno=lineno,
            message=message,
            trace=trace,
        )
        _db_session.add(log)
        _db_session.commit()

    # A very basic logger that commits a LogRecord to the SQL Db
    def emit(self, record):
        """Overloaded method.

        Includes the ability to log a Fund UUID and platform name if passed in
        the logger `extra` dict argument.
        """
        # Check for a trace back from an exception.
        exc = record.__dict__["exc_info"]
        if exc:
            trace = traceback.format_exc(exc)
        else:
            trace = None
        # Check for extra Fund object's in the record.
        uuid = None
        platform = None
        if "uuid" in list(record.__dict__.keys()):
            uuid = record.__dict__["uuid"]
        if "platform" in list(record.__dict__.keys()):
            platform = record.__dict__["platform"]
        # Create a log entry.
        log = Log(
            trace=trace,
            asctime=record.__dict__["asctime"],
            name=record.__dict__["name"],
            levelno=record.__dict__["levelno"],
            levelname=record.__dict__["levelname"],
            module=record.__dict__["module"],
            filename=record.__dict__["filename"],
            lineno=record.__dict__["lineno"],
            message=record.__dict__["message"],
            uuid=uuid,
            platform=platform,
        )
        _db_session.add(log)
        _db_session.commit()

# Configure the logging database.
LOG_NAME = "asset_base.log.db"
logfile_name = get_log_path(LOG_NAME)
_db_url = "sqlite:///" + logfile_name
_db_engine = create_engine(_db_url)
_db_session = Session(_db_engine)

# Check for 1st time and create and set up the schema if needed.
table_exist = _db_engine.dialect.has_table(_db_engine.connect(), Log.__tablename__)
if not table_exist:
    # Create all the needed tables.
    Base.metadata.create_all(_db_engine)
