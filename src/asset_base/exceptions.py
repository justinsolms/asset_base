""" Package exception definitions.

"""


class _BaseException(BaseException):
    """Fund exception.

    Parameters
    ----------
    msg : str
        The exception message.
    action: str (Optional)
        What action has already been taken.

    """

    action = None

    def __init__(self, msg, **kwargs):
        """Initialization."""
        self.message = msg  # In python3 there is no Exception.message
        if "action" in kwargs:
            self.action = kwargs["action"]
        super().__init__(msg)

    def __str__(self):
        """Return the informal string output."""
        if self.action is not None:
            fmt = "%(except_name)s : %(action)s : %(message)s"
        else:
            fmt = "%(except_name)s : %(message)s"
        data = {
            "message": self.message,
            "except_name": self.__class__.__name__,
            "action": self.action,
        }
        return fmt % data

    def set_action(self, action):
        """Set the action taken description string.

        Parameters
        ----------
        action : str
            A string describing the action taken, usually by the exception
            handler.
        """
        self.action = action

    @property
    def logger_extra(self):
        """Extra dictionary for the logger `extra` argument.

        The dictionary elements are:
            'except_name':
                The class name of the exception.
            'action':
                What action has already been taken.
        """
        return {"except_name": self.__class__.__name__, "action": self.action}


class FactoryError(_BaseException):
    """Entity not found in database or could not be created."""


class HoldingsError(_BaseException):
    """Any model specific error."""


class TimeSeriesNoData(_BaseException):
    """Time series data returns empty."""


class EODSeriesNoData(TimeSeriesNoData):
    """EOD data returns empty."""


class DividendSeriesNoData(TimeSeriesNoData):
    """Dividends data returns empty."""


class BadISIN(_BaseException):
    """A Listed ISIN number exception.

    Parameters
    ----------
    isin : str
        The problematic ISIN number as a string.
    action: str (Optional)
        What action has already been taken.

    Note
    ----
    There is no message parameter as is the usual with most exception classes.
    In this exception the message is hard coded.
    """

    def __init__(self, isin, **kwargs):
        """See the class docstring."""
        self.isin = isin
        msg = "ISIN=%s checksum fails" % isin
        super().__init__(msg, **kwargs)


class ReconcileError(_BaseException):
    """Raised when parameters and object attributes do not reconcile.

    Parameters
    ----------
    obj
        The object that did not reconcile.
    param : str
        A string parameter that is passed by the exception. This may be a method
        parameter name name that did not reconcile. It may also be a code of
        some kind that is useful in resolving the exception.
    action: str (Optional)
        What action has already been taken.

    Attributes
    ----------
    obj
        The object that did not reconcile.
    param : str
        A string parameter that is passed by the exception. This may be a method
        parameter name name that did not reconcile. It may also be a code of
        some kind that is useful in resolving the exception.
    action: str
        What action has already been taken.

    """

    def __init__(self, obj, param, **kwargs):
        """Instance initialization."""
        self.param = param
        self.obj = obj
        msg = (
            "The existing %r does not reconcile "
            'with the factory method argument "%s".'
        ) % (obj, param)
        super().__init__(msg, **kwargs)


class _EntityError(Exception):
    """Entity exception.

    Parameters
    ----------
    msg : string
        A text message for the exception.
    entity : a Entity object
        An (in)complete object. Must have at least it's `platform` and
        `uuid` attribute.

    Note
    ----
    Do not use directly but use the child classes.

    """

    def __init__(self, msg, entity):
        """See the class docstring."""
        self.entity = entity
        super().__init__(msg)

    def __str__(self):
        """The class display string."""
        entity = self.entity
        name = entity.name
        domicile_code = entity.domicile.code
        discriminator = entity._discriminator
        msg = super().__str__()
        return "%s:%s:%s:%s" % (name, domicile_code, discriminator, msg)

    @property
    def logger_extra(self):
        """Extra dictionary for the logger `extra` argument.

        The dictionary elements are:
            :platform: The platform name (str).
            :uuid: The entity unique identifier (str)

        """
        entity = self.entity
        name = entity.name
        domicile_code = entity.domicile.code
        discriminator = entity._discriminator
        return {
            "name": name,
            "domicile_code": domicile_code,
            "discriminator": discriminator,
        }


class ListedError(_EntityError):
    """A Listed exception."""
