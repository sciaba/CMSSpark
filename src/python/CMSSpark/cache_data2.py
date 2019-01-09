#! /usr/bin/env python

"""
Script to extract data useful for cache simulation
"""

import os
import sys
import re
import time
import datetime
import calendar

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
from pyspark.sql.types import ByteType
from pyspark.sql.functions import asc, count, col, expr, sum, lag, lit, \
    row_number, when

from CMSSpark.spark_utils import spark_context, cmssw_tables, jm_tables, \
    dbs_tables, unionAll
from CMSSpark.conf import OptionParser

# global patterns
PAT_YYYYMMDD = re.compile(r'^20[0-9][0-9][0-1][0-9][0-3][0-9]$')
PAT_YYYY = re.compile(r'^20[0-9][0-9]$')


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

    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)
    if not date:
        dates = [time.strftime("year=%Y/month=%-m/day=%-d",
                               time.gmtime(time.time()-60*60*24))]
    else:
        dates = jm_dates(date)

    tables = {}
    dfs = []
    for date in dates:
        cmssw_df = cmssw_tables(ctx, sqlContext, date=date, verbose=verbose)
        if site:
            cmssw_df['cmssw_df'] = cmssw_df['cmssw_df'].\
            where(col("SITE_NAME") == site)
        dfs.append(cmssw_df['cmssw_df'])
    df = unionAll(dfs)
    df.registerTempTable('cmssw_df')
    cmssw_df = {'cmssw_df': df}
    tables.update(dbs_tables(sqlContext, verbose=verbose))
    fdf = tables['fdf']

    cols = ['FILE_LFN', 'FILE_SIZE', 'CLIENT_HOST', 'CLIENT_DOMAIN',
            'SERVER_HOST', 'SERVER_DOMAIN', 'SITE_NAME', 'READ_BYTES',
            'USER_DN', 'FALLBACK', 'APP_INFO', 'START_TIME', 'f_file_size']
    cols = ['FILE_LFN', 'FILE_SIZE', 'SITE_NAME', 'APP_INFO', 'START_TIME',
            'f_file_size']

# An inner join selects only accesses to files registered in DBS, which
# leaves out e.g. files in /store/unmerged/..., which are almost always
# read locally and therefore do not need to be cached

    stmt = "SELECT %s FROM cmssw_df INNER JOIN fdf ON cmssw_df.FILE_LFN = fdf.f_logical_file_name" % ','.join(cols)
    joins = sqlContext.sql(stmt)
    
    fjoin = joins.withColumnRenamed('FILE_LFN', 'Filename')\
            .withColumnRenamed('FILE_SIZE', 'Filesize')\
            .withColumnRenamed('f_file_size', 'DBS_Filesize')\
            .withColumnRenamed('START_TIME', 'AccessTime')\
            .withColumnRenamed('SITE_NAME', 'SiteName')\
            .orderBy(col("AccessTime"))\
            .repartition(col("SiteName"))

    fjoin = fjoin.withColumn("JobType",
                             when(col("APP_INFO").contains('crab'),
                                  'analysis').otherwise('production'))\
                 .drop("APP_INFO")

    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    if fout:
        if site:
            fjoin.orderBy(col("AccessTime")).coalesce(1).write.format("csv")\
                .option("header", "true").save(fout)
        else:
            fjoin.orderBy(col("AccessTime")).coalesce(1).write.format("csv")\
                        .option("header", "true").save(fout)
    ctx.stop()

# toDo:
# 1) add size from DBS if available
# 1) implement garbage collection every N minutes
#    a) process one day
#    b) create a DataFrame with the files in the cache
#    c) the next day, make the intersection of the DataFrame of the day and the cache DataFrame
#    d) consider cached all files in the intersection
#    e) consider non cached the first appearance of the files not in the intersection


def main():
    optmgr = OptionParser('cache_data')
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    run(opts.site, opts.date, opts.fout, opts.yarn, opts.verbose)    

if __name__ == '__main__':
    main()
