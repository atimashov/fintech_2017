# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 05:39:16 2017

@author: aleks
"""
import pandas as pd
import numpy as np
import sqlalchemy
import math
import psycopg2
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import CHAR
from sqlalchemy.types import ARRAY
import time
#***************************
dfAppl=getTableFromDB(typeConnection='myDB', db='scorChina',showLogs=True)
del dfAppl
infoJSON=getTree(dfAppl,True,True)

"""QUANTGROUP"""
dfQG=getTableFromDB(typeConnection='myDB', db='scorChina',tableName='quantgroup_all', showLogs=True)
infoQG=getTree(dfQG,True,True)

"""TONGDUN"""
dfTongdun=getTableFromDB(typeConnection='myDB', db='scorChina',req='id, tongdun_report_raw_response, tongdun_load_application_raw_response',showLogs=True)
InfoTongdun=getTree(dfTongdun,True,True)


#ВАЖНО! Тут id - id запроса, id_application - id в applications
dfQG_error=getTableFromMyDB('quantgroup_all',req='id, error',showTime=True)
dfQG_error=getDFfromLists(dfQG_error.copy(),True)
