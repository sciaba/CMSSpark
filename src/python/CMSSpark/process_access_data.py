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
from __future__ import division


import os
import sys
import re
import time
import datetime
import calendar
import logging

import numpy as np
import pandas as pd

from pyspark.sql.window import Window
from pyspark.sql.functions import asc, avg, count, col, desc, expr, sum, max
from pyspark.sql.types import StructType, StructField, StringType, LongType

from CMSSpark.spark_utils import spark_session
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


def run(site, date, cache_size, fout, yarn=None, verbose=None):

    date_name = date

    logger = logging.getLogger('process_access_data')
    logger.setLevel(logging.INFO)
    logfile = 'logfile_' + site + '_' + cache_size + '_' + date_name + '.txt'
    fh = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    cache_limit = int(cache_size) * 1024.   # in GB
    clean_cache_limit = cache_limit * 0.8   # in GB
    gc_type = 'atime'

    logger.info("Cache limit: %d" % cache_limit)
    logger.info("Clean cache limit: %d" % clean_cache_limit)

    fmt = 'csv'

    results = pd.DataFrame({}, columns=['day', 'hits', 'misses', 'nfc',
                                        'cache_used'])

    spark = spark_session('cms', yarn, verbose)
    if not date:
        dates = [time.strftime("year=%Y/month=%-m/day=%-d",
                               time.gmtime(time.time()-60*60*24))]
    else:
        dates = jm_dates(date)

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

    i = 0
    for date in dates:
        logger.info("Day %s"% date)
        i += 1

        # Read file access data for the day
        logger.info("Reading file access information...")
        infile = fout + '/access_' + date
        fjoin = spark.read.format(fmt).option("header", "false")\
                                      .schema(schema)\
                                      .load(infile)

        logger.info("Collecting information of files accessed...")

        new_files_df = fjoin.groupBy("Filename")\
            .agg(avg("DBS_Filesize").alias("size"),
            count("AccessTime").alias("nacc"),
            max("AccessTime").alias("atime"))
        new_files_df.cache()
        new_files_df.createOrReplaceTempView("new_files_df")

        logger.info("Updating the cache...")
        if i == 1:
            totals = new_files_df.selectExpr("sum(nacc-1)", "count(1)").head()
            if totals[1] == 0:
                nhits = 0
                nmisses = 0
            else:
                nhits = totals[0]
                nmisses = totals[1]
            logger.info("Hits and misses calculated")
            cache_df = new_files_df
            cache_df.cache()
            cache_df.createOrReplaceTempView("cache_df")
            logger.info("Cache created")
        else:
            # Find number of cache hits and misses
            joinExpr = new_files_df.Filename == cache_df.Filename
            cached_df    = new_files_df.join(cache_df, joinExpr, 'left_semi')
            notcached_df = new_files_df.join(cache_df, joinExpr, 'left_anti')
            totals = notcached_df.selectExpr("sum(nacc-1)", "count(1)").head()
            if totals[1] == 0:
                nhits = toScalar2(cached_df.select(sum("nacc")))
                if nhits is None: nhits = 0
                nmisses = 0
            else:
                nhits = toScalar2(cached_df.select(sum("nacc")))
                if nhits is None: nhits = 0
                nhits += totals[0]
                nmisses = totals[1]
            logger.info("Hits and misses calculated")

            # Now, update the cache information

            joinExpr = "SELECT coalesce(cache_df.Filename, new_files_df.Filename) AS Filename, coalesce(cache_df.size, new_files_df.size) AS size, (nvl(cache_df.nacc, 0L) + nvl(new_files_df.nacc, 0L)) AS nacc, greatest(cache_df.atime, new_files_df.atime) AS atime FROM cache_df FULL OUTER JOIN new_files_df ON cache_df.Filename = new_files_df.Filename ORDER BY size ASC"
            cache_df = spark.sql(joinExpr)
            cache_df.cache()
            cache_df.createOrReplaceTempView("cache_df")
            logger.info("Cache updated")

        totals = cache_df.selectExpr("sum(size)", "count(1)").head()
        if totals[1] == 0:
            nfc = 0
            cache_used = 0.
        else:
            nfc = totals[1]
            cache_used = totals[0] / 1024.**3
        logger.info("No. of hits: %d" % nhits)
        logger.info("No. of nmisses: %d" % nmisses)
        logger.info("No. of files in cache: %d" % nfc)
        logger.info("Cache used: %f TB" % (cache_used / 1024.))

        results = results.append({'day': i, 'hits': nhits, 'misses': nmisses, 'nfc': nfc, 'cache_used': cache_used}, ignore_index=True)

        if cache_used > cache_limit:
            logger.info("Garbage collection!")
            wSpec = Window.orderBy(desc("atime"))\
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            cumsize = sum(col("size")).over(wSpec)
            cache_df = cache_df.withColumn("cumsize", cumsize)\
                .where(col("cumsize") < clean_cache_limit * 1024**3)\
                               .drop("cumsize")
            cache_df.cache()
            cache_df.createOrReplaceTempView("cache_df")
            totals = cache_df.selectExpr("sum(size)", "count(1)").head()
            nfc = totals[1]
            cache_used = totals[0] / 1024.**3
            logger.info("No. of hits: %d" % nhits)
            logger.info("No. of nmisses: %d" % nmisses)
            logger.info("No. of files in cache: %d" % nfc)
            logger.info("Cache used: %f TB" % (cache_used / 1024.))
            results = results.append({'day': i, 'hits': nhits, 'misses': nmisses, 'nfc': nfc, 'cache_used': cache_used}, ignore_index=True)


    # Writing out the processed data
    logger.info("Writing out the results...")

    cachefile = fout + '/cache_' + site + '_' + cache_size + '_' + date_name

    cache_df.write.save(cachefile)

    outfile = 'results_' + site + '_' + cache_size + '_' + date_name + '.csv'
    results.to_csv(outfile)
    logger.info("Job finished!")


def main():
    optmgr = OptionParser('cache_data')
    optmgr.parser.add_argument("--site", action="store",
            dest="site", default="", help='Select CMS site')
    optmgr.parser.add_argument("--cache_size", action="store",
            dest="cache_size", default="", help='Select cache size (in TB)')

    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    run(opts.site, opts.date, opts.cache_size, opts.fout, opts.yarn,
        opts.verbose)    

if __name__ == '__main__':
    main()
