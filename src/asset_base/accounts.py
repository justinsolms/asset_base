#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Classes to describe accounts."""


from sqlalchemy import Integer
from sqlalchemy import Column, ForeignKey

from .asset import Cash


class CashAccount(Cash):
    """The cash in a fund is held in a cash account.

    Use this to differentiate the cash account from cash.
    """
    __tablename__ = "cash_account"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    id = Column(Integer, ForeignKey("cash.id"), primary_key=True)


class SettlementAccount(Cash):
    """The settlement cash in a fund is held in a cash settlement account.

    Use this to differentiate the settlement cash account from cash.
    """
    __tablename__ = "settlement_account"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    id = Column(Integer, ForeignKey("cash.id"), primary_key=True)

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return super().identity_code + "-S"  # Mark as special settlement account


