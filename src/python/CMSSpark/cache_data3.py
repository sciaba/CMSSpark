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
from pyspark.sql.types import ByteType
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

    results = pd.DataFrame({}, columns=['day', 'hits', 'misses', 'nfc',
                                        'cache_used'])

    spark = spark_session('cms', yarn, verbose)
    ctx = spark.sparkContext
    sqlContext = HiveContext(ctx)
    if not date:
        dates = [time.strftime("year=%Y/month=%-m/day=%-d",
                               time.gmtime(time.time()-60*60*24))]
    else:
        dates = jm_dates(date)

    use_full_dbs = False
    save_reduced_dbs = use_full_dbs

    if use_full_dbs:
        tsprint("Reading DBS data from full DBS dump...")
        tables = {}
        tables.update(dbs_tables(sqlContext, verbose=verbose))
        fdf = tables['fdf']
        myfdf = fdf.select("f_logical_file_name", "f_file_size").orderBy("f_logical_file_name")
    else:
        tsprint("Reading DBS data from reduced DBS dump...")
        myfdf = spark.read.load("/cms/users/asciaba/myfdf")

    myfdf.createOrReplaceTempView('myfdf')
    myfdf.cache()

    if save_reduced_dbs:
        myfdf.write.mode("overwrite").save("/cms/users/asciaba/myfdf")

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
                'f_file_size']

        # An inner join selects only accesses to files registered in DBS, which
        # leaves out e.g. files in /store/unmerged/..., which are almost always
        # read locally and therefore do not need to be cached

        stmt = "SELECT %s FROM cmssw_df INNER JOIN myfdf ON cmssw_df.FILE_LFN = myfdf.f_logical_file_name" % ','.join(cols)
        joins = sqlContext.sql(stmt)
    
        fjoin = joins.withColumnRenamed('FILE_LFN', 'Filename')\
                     .withColumnRenamed('FILE_SIZE', 'Filesize')\
                     .withColumnRenamed('f_file_size', 'DBS_Filesize')\
                     .withColumnRenamed('START_TIME', 'AccessTime')\
                     .withColumnRenamed('SITE_NAME', 'SiteName')\
                     .orderBy(col("AccessTime"))

        # Determine if the job was analysis or production
        fjoin = fjoin.withColumn("JobType",
                                when(col("APP_INFO").contains('crab'),
                                     'analysis').otherwise('production'))\
        .drop("APP_INFO").cache()

        # Print number of partitions
        tsprint("Partitions: %d" % fjoin.rdd.getNumPartitions())

#        fjoin = fjoin.repartition(col("Filename")).coalesce(100)
#        tsprint("Partitions after repartition: %d" % fjoin.rdd.getNumPartitions())

        # Build list of accessed files with their size,
        # number of accesses in the day and time of latest access

        tsprint("Collecting information of files accessed...")

        new_files_df = fjoin.groupBy("Filename")\
                            .agg(avg("DBS_Filesize").alias("size"), count("AccessTime").alias("nacc"), max("AccessTime").alias("atime")).withColumn("cached", col("nacc") > 1)
        new_files_df.createOrReplaceTempView("new_files_df")

        tsprint("Updating the cache...")
        if i == 1:
            totals = new_files_df.selectExpr("sum(nacc-1)", "count(1)").head()
            # nhits = toScalar2(new_files_df.select(sum(col("nacc")-1)))
            # nmisses = new_files_df.count()
            nhits = totals[0]
            nmisses = totals[1]
            tsprint("Hits and misses calculated")
            cache_df = new_files_df
            tsprint("Cache created")
        else:
            # Find number of cache hits and misses
            joinExpr = new_files_df.Filename == cache_df.Filename
            cached_df    = new_files_df.join(cache_df, joinExpr, 'left_semi')
            notcached_df = new_files_df.join(cache_df, joinExpr, 'left_anti')
            totals = notcached_df.selectExpr("sum(nacc-1)", "count(1)").head()
            # nhits = toScalar2(cached_df.select(sum("nacc")))
            # + toScalar2(notcached_df.select(sum(col("nacc")-1)))
            # nmisses = notcached_df.count()
            nhits = toScalar2(cached_df.select(sum("nacc"))) + totals[0]
            nmisses = totals[1]
            tsprint("Hits and misses calculated")

            # Now, update the cache information
            joinExpr = "SELECT coalesce(cache_df.Filename, new_files_df.Filename) AS Filename, coalesce(cache_df.size, new_files_df.size) AS size, (nvl(cache_df.nacc, 0L) + nvl(new_files_df.nacc, 0L)) AS nacc, greatest(cache_df.atime, new_files_df.atime) AS atime FROM cache_df FULL OUTER JOIN new_files_df ON cache_df.Filename = new_files_df.Filename"
            cache_df = sqlContext.sql(joinExpr)
            tsprint("Cache updated")

        cache_df.createOrReplaceTempView("cache_df")

        # TODO: calculate nfc and cache_used in one go
        nfc = cache_df.cache().count()
        cache_used = toScalar2(cache_df.select(sum("size")))
        tsprint("No. of hits: %d" % nhits)
        tsprint("No. of nmisses: %d" % nmisses)
        tsprint("No. of files in cache: %d" % nfc)
        tsprint("Cache used: %f" % (cache_used / 1024**4))

        results = results.append({'day': i, 'hits': nhits, 'misses': nmisses, 'nfc': nfc, 'cache_used': cache_used}, ignore_index=True)

    # Writing out the processed data
    tsprint("Writing out the results...")

    results.to_csv('results.csv')
#    if fout:
#        if site:
#            fjoin.orderBy(col("AccessTime")).write.format("csv")\
#                .option("header", "true").save(fout)
#            new_files_df.coalesce(1).write.format("csv").option("header", "true").save(fout + '/cache')
#        else:
#            fjoin.orderBy(col("AccessTime")).write.format("csv")\
#                        .option("header", "true").save(fout)
    tsprint("Job finished!")

# toDo:
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
