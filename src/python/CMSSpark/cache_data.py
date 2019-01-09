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
from pyspark.sql.functions import asc, count, col, expr, sum, lag, lit, row_number

from CMSSpark.spark_utils import spark_context, jm_tables, dbs_tables, unionAll
from CMSSpark.conf import OptionParser

# global patterns
PAT_YYYYMMDD = re.compile(r'^20[0-9][0-9][0-1][0-9][0-3][0-9]$')
PAT_YYYY = re.compile(r'^20[0-9][0-9]$')
PAT_MM = re.compile(r'^(0[1-9]|1[012])$')
PAT_DD = re.compile(r'^(0[1-9]|[12][0-9]|3[01])$')

cache_size = 1.e12

def jm_date(date):
    "Convert given date into JobMonitoring date format"
    if  not date:
        date = time.strftime("year=%Y/month=%-m/day=%-d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)

def dateformat(value):
    """Return seconds since epoch for provided YYYYMMDD or number with suffix 'd' for days"""
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported format is YYYYMMDD or number with suffix 'd' for days"
    value = str(value).lower()
    if  PAT_YYYYMMDD.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            year = value[0:4]
            if  not PAT_YYYY.match(year):
                raise Exception(msg + ', fail to parse the year part, %s' % year)
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
        dates = [time.strftime("year=%Y/month=%-m/day=%-d", time.gmtime(time.time()-60*60*24))]
    else:
        dates = jm_dates(date)

    tables = {}
    dfs = []
    for date in dates:
        print date
        jm_df = jm_tables(ctx, sqlContext, date=date, verbose=verbose)
        if site:
            jm_df['jm_df'] = jm_df['jm_df'].where(col("Sitename") == site)
        dfs.append(jm_df['jm_df'])
    df = unionAll(dfs)
    df.registerTempTable('jm_df')
    jm_df = {'jm_df': df}
    tables.update(dbs_tables(sqlContext, verbose=verbose))
    fdf = tables['fdf']

    cols = ['JobId', 'Filename', 'ProtocolUsed', 'Type', 'SubmissionTool',
            'SiteName',
            'StartedRunningTimeStamp', 'f_file_size', 'JobExecExitCode']
    stmt = "SELECT %s FROM jm_df JOIN fdf ON jm_df.FileName = fdf.f_logical_file_name" % ','.join(cols)
    joins = sqlContext.sql(stmt)
    
    fjoin = joins.withColumnRenamed('f_file_size', 'Filesize')\
                 .withColumnRenamed('StartedRunningTimeStamp', 'AccessTime')\
                 .orderBy(col("AccessTime"))\
                 .repartition(col("SiteName"))

    windowSpec = Window.partitionBy("Filename", "Sitename")\
                 .rowsBetween(-9223372036854775808, 0)
    windowSpec2 = Window.partitionBy("Sitename")\
                 .rowsBetween(-9223372036854775808, 0)

    cached = count(col("Filename")).over(windowSpec) > 1
    cache_used = sum(col("Filesize") * (~col("cached")).cast("float"))\
                 .over(windowSpec2)

    fjoin2 = fjoin.withColumn("cached", cached)\
             .withColumn("cache_used", cache_used)

#    fjoin2 = fjoin2.withColumn("cache_full", row_number())

    fjoin2.persist(StorageLevel.MEMORY_AND_DISK)

    if fout:
        if site:
            fjoin2.orderBy(col("AccessTime")).coalesce(1).write.format("csv")\
                .option("header", "true").save(fout)
        else:
            fjoin2.orderBy(col("AccessTime")).write.format("csv")\
                        .option("header", "true").save(fout)
    ctx.stop()

# toDo:
# 1) implement garbage collection every N minutes
# 2) concatenate data from several days

def main():
    optmgr = OptionParser('cache_data')
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    run(opts.site, opts.date, opts.fout, opts.yarn, opts.verbose)    

if __name__ == '__main__':
    main()
