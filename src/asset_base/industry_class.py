#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Support for Industry Classification Benchmark (ICB)

    Used by stock exchanges such as the London FTSE and FTSE/JSE.

"""
# Allows  in type hints to use class names instead of class name strings
from __future__ import annotations

# Used to avoid ImportError (most likely due to a circular import)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from .common import Base
from .exceptions import ReconcileError


class IndustryClassICB(Base):
    """The Industry Classification Benchmark (ICB) implementation.

    `The Industry Classification Benchmark (ICB)`_ is an `industry
    classification`_ taxonomy launched by Dow Jones and FTSE in 2005 and now
    owned solely by FTSE International. It is used to segregate markets into
    sectors within the macroeconomy. The ICB uses a system of 10 industries,
    partitioned into 19 super sectors which are further divided into 41 sectors,
    which then contain 114 sub sectors.

    See also the official `Industry Classification Benchmark`_ website.

    .. _`Industry Classification Benchmark`:
        http://www.icbenchmark.com/

    The ICB is used globally (though not universally) to divide the market into
    increasingly specific categories, allowing investors to compare industry
    trends between well-defined sub sectors. The ICB replaced the legacy FTSE
    and Dow Jones classification systems on 3 January 2006, and is used today by
    the NASDAQ, NYSE and several other markets around the globe. All ICB sectors
    are represented on the New York Stock Exchange except Equity Investment
    Instruments (8980) and Non-equity Investment Instruments (8990).

    The ICB structure for sector and industry analysis enables the comparison of
    companies across four levels of classification and national boundaries. It
    offers a balance between levels of aggregation, for those who look at
    markets from the top down, and granularity, for those who look at markets
    from the bottom up.

        * 114 sub sectors allow detailed analysis.
        * 41 sectors provide a broad benchmark for investment managers.
        * 19 super sectors can be used for trading.
        * 10 industries help investors monitor broad industry trends

    Each company is allocated to the sub sector that most closely represents the
    nature of its business, which is determined by its source of revenue or
    where it constitutes the majority of revenue.

    .. _`The Industry Classification Benchmark (ICB)`:
        https://en.wikipedia.org/wiki/Industry_Classification_Benchmark

    .. _`industry classification`:
        https://en.wikipedia.org/wiki/Industry_classification

    Parameters
    ----------
    industry_name : str
        Industry name.
    super_sector_name : str
        Super sector name.
    sector_name : str
        Sector name,
    sub_sector_name : str
        Sub sector name.
    industry_code : str
        Industry code.
    super_sector_code : str
        Super sector code.
    sector_code : str
        Sector code.
    sub_sector_code : str
        Sub sector code.

    See also
    --------
    .ListedEquity

    """

    # FIXME: Currently a new entry is created for each new ListedEquity instance, see below TODO.
    # TODO: Make the ICB table unique by all its codes, pre-populate it, and link to ListedEquity instances when they are created.

    __tablename__ = "industry_class_icb"

    id = Column(Integer, primary_key=True)
    """ Primary key."""

    # Collection of ListedEquity instances having this instance of
    # ICB classification
    _listed_equities = relationship("ListedEquity", backref="_industry_class_icb")

    # Industry classification names.
    industry_name = Column(String(64), nullable=False)
    super_sector_name = Column(String(64), nullable=False)
    sector_name = Column(String(64), nullable=False)
    sub_sector_name = Column(String(64), nullable=False)

    # Industry classification codes.
    industry_code = Column(String(4), nullable=False)
    super_sector_code = Column(String(4), nullable=False)
    sector_code = Column(String(4), nullable=False)
    sub_sector_code = Column(String(4), nullable=False)

    def __init__(
        self,
        industry_name,
        super_sector_name,
        sector_name,
        sub_sector_name,
        industry_code,
        super_sector_code,
        sector_code,
        sub_sector_code,
    ):
        """Instance initialization."""
        self.industry_name = industry_name
        self.super_sector_name = super_sector_name
        self.sector_name = sector_name
        self.sub_sector_name = sub_sector_name
        self.industry_code = industry_code
        self.super_sector_code = super_sector_code
        self.sector_code = sector_code
        self.sub_sector_code = sub_sector_code

    def _reconcile(self, **kwargs):
        """Reconcile specified parameters with class instance attributes.

        The parameters are the same as those for the `factory` method with the
        exception of the `session` argument. This method is always used by the
        `factory` method to reconcile instances retrieved from the session.

        Raises
        ------
        ReconcileError
            The specified parameters do not reconcile with class instance
            attributes.
        """
        # Local attributes to reconcile.
        try:
            if kwargs["industry_name"] != self.industry_name:
                raise ReconcileError(self, "industry_name")
            if kwargs["super_sector_name"] != self.super_sector_name:
                raise ReconcileError(self, "super_sector_name")
            if kwargs["sector_name"] != self.sector_name:
                raise ReconcileError(self, "sector_name")
            if kwargs["sub_sector_name"] != self.sub_sector_name:
                raise ReconcileError(self, "sub_sector_name")
            if kwargs["industry_code"] != self.industry_code:
                raise ReconcileError(self, "industry_code")
            if kwargs["super_sector_code"] != self.super_sector_code:
                raise ReconcileError(self, "super_sector_code")
            if kwargs["sector_code"] != self.sector_code:
                raise ReconcileError(self, "sector_code")
            if kwargs["sub_sector_code"] != self.sub_sector_code:
                raise ReconcileError(self, "sub_sector_code")
        except KeyError as ex:
            raise ValueError("Expected argument: %s" % ex)
