# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 12:14:14 2017

@author: Alex
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
import datetime
from termcolor import colored

info = pd.DataFrame({
    'dbMy':[
        'scorVietnam', 'scorVietnam', 'scorVietnam', 'scorMalaysia', 'scorMalaysia',
        'scorMalaysia', 'scorIndonesia', 'scorIndonesia', 'scorPhilippiness'
    ],
    'schemaMy':[
        'doctordong','doctordong_info','vaynhanhtrongngay','drringgit',
        'doctorringgit_info','malaysiamoney','drrupiah','drrupiah_cc','doctorcash'
    ],
    'dbInfinto':[
        'doctordong_production','doctordong_info_production','vaynhanhtrongngay_production',
        'drringgit_production','doctorringgit_info_production','malaysiamoney_production',
        'drrupiah_production','drrupiah_cc_production','doctorcash_production'
    ]
})
updateDataFromInfinto(info)