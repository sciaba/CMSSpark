#! /usr/bin/env python

"""
Script to analyse file accesses and simulate a cache

Things to calculate:
- cache hit rate (weighted by file or size) vs. time and cache size
- cache miss rate (weighted by file or size) vs. time and cache size
- no. of files in cache vs. time and cache size
- cache used space vs. time and cache size
- no. of expunged files from cache vs. time and cache size
- average number of file accesses from cache vs. cache size

Further on:
- by file type
"""

import os
import sys
import re
import time
import datetime
import calendar

import pandas as pd

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
from pyspark.sql.types import ByteType, StructField, StructType, LongType, \
    StringType
from pyspark.sql.functions import asc, avg, count, col, expr, sum, lag, lit, \
    row_number, when, max

from CMSSpark.spark_utils import spark_context, cmssw_tables, jm_tables, \
    dbs_tables, unionAll, spark_session
from CMSSpark.conf import OptionParser

# global patterns
PAT_YYYYMMDD = re.compile(r'^20[0-9][0-9][0-1][0-9][0-3][0-9]$')
PAT_YYYY = re.compile(r'^20[0-9][0-9]$')

def tsprint (t):
    print datetime.datetime.now().strftime('[%H:%M:%S] ') + t

def toScalar(df):
    return df.collect()[0].asDict().values()[0]

def toScalar2(df):
    return df.head()[0]

def jm_date(date):
    "Convert given date into JobMonitoring date format"
    if  not date:
        date = time.strftime("year=%Y/month=%-m/day=%-d",
                             time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)


def dateformat(value):
    """
    Return seconds since epoch for provided YYYYMMDD or number with
    suffix 'd' for days
    """
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported format is YYYYMMDD or number with suffix 'd' for days"
    value = str(value).lower()
    if  PAT_YYYYMMDD.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            year = value[0:4]
            if  not PAT_YYYY.match(year):
                raise Exception(msg
                                + ', fail to parse the year part, %s' % year)
            month = value[4:6]
            date = value[6:8]
            ddd = datetime.date(int(year), int(month), int(date))
        else:
            raise Exception(msg)
        return calendar.timegm((ddd.timetuple()))
    elif value.endswith('d'):
        try:
            days = int(value[:-1])
        except ValueError:
            raise Exception(msg)
        return time.time()-days*24*60*60
    else:
        raise Exception(msg)


def range_dates(trange):
    out = [jm_date(str(trange[0]))]
    if  trange[0] == trange[1]:
        return out
    tst = dateformat(trange[0])
    while True:
        tst += 24*60*60
        tdate = time.strftime("%Y%m%d", time.gmtime(tst))
        out.append(jm_date(tdate))
        if  str(tdate) == str(trange[1]):
            break
    return out


def jm_dates(dateinput):
    dates = dateinput.split('-')
    if  len(dates) == 2:
        return range_dates(dates)
    dates = dateinput.split(',')
    if  len(dates) > 1:
        return [jm_date(d) for d in dates]
    return [jm_date(dateinput)]


def run(site, date, fout, yarn=None, verbose=None):

    spark = spark_session('cms', yarn, verbose)
    ctx = spark.sparkContext
    sqlContext = HiveContext(ctx)
    if not date:
        dates = [time.strftime("year=%Y/month=%-m/day=%-d",
                               time.gmtime(time.time()-60*60*24))]
    else:
        dates = jm_dates(date)

    tsprint("Reading DBS data from my DBS dump...")
    myfdf = spark.read.load("/cms/users/asciaba/myfdf")
    myfdf.cache()
    myfdf.createOrReplaceTempView('myfdf')

    i = 0
    for date in dates:
        tsprint("Day %s"% date)
        i += 1

        # Read file access data for the day
        tsprint("Reading file access information...")
        cmssw_df = cmssw_tables(ctx, sqlContext, date=date, verbose=verbose)
        if site:
            cmssw_df['cmssw_df'] = cmssw_df['cmssw_df'].\
            where(col("SITE_NAME") == site)
        cmssw_df['cmssw_df'].createOrReplaceTempView('cmssw_df')

        tsprint("Joining with DBS information...")
        # Join file access data with DBS file data
        cols = ['FILE_LFN', 'FILE_SIZE', 'SITE_NAME', 'APP_INFO', 'START_TIME',
                'f_file_size', 'd_dataset', 'data_tier_name']

        # An inner join selects only accesses to files registered in DBS, which
        # leaves out e.g. files in /store/unmerged/..., which are almost always
        # read locally and therefore do not need to be cached

        stmt = "SELECT %s FROM cmssw_df INNER JOIN myfdf ON cmssw_df.FILE_LFN = myfdf.f_logical_file_name" % ','.join(cols)
        joins = spark.sql(stmt)
    
        fjoin = joins.withColumnRenamed('FILE_LFN', 'Filename')\
                     .withColumnRenamed('FILE_SIZE', 'Filesize')\
                     .withColumnRenamed('f_file_size', 'DBS_Filesize')\
                     .withColumnRenamed('START_TIME', 'AccessTime')\
                     .withColumnRenamed('SITE_NAME', 'SiteName')\
                     .withColumnRenamed('d_dataset', 'Dataset')\
                     .withColumnRenamed('data_tier_name', 'Datatier')\
                     .withColumn("JobType",
                        when(col("APP_INFO").contains('crab'),
                        'analysis').otherwise('production'))\
                     .drop("APP_INFO").orderBy(col("AccessTime"))

        fmt = 'csv'
        if fout:
            schema = StructType([
                StructField("Filename", StringType(), False),
                StructField("Filesize", LongType(), False),
                StructField("SiteName", StringType(), False),
                StructField("AccessTime", LongType(), False),
                StructField("DBS_Filesize", LongType(), False),
                StructField("Dataset", StringType(), False),
                StructField("Datatier", StringType(), False),
                StructField("JobType", StringType(), False),
            ])
            tsprint("Saving day information...")
            outfile = fout + '/access_' + date
            if fmt == 'csv':
                fjoin.write.format("csv")\
                    .schema(schema)\
                    .option("header", "true").mode("overwrite").save(outfile)
            elif fmt == 'parquet':
                fjoin.write.format("parquet")\
                    .schema(schema)\
                    .option("codec", "snappy").mode("overwrite").save(outfile)
    tsprint("Job finished!")


def main():
    optmgr = OptionParser('cache_data')
    optmgr.parser.add_argument("--site", action="store",
            dest="site", default="", help='Select CMS site')
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    run(opts.site, opts.date, opts.fout, opts.yarn, opts.verbose)    

if __name__ == '__main__':
    main()
