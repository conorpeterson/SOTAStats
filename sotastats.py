# SOTAStats - a script to gather and report SOTA data specific to your association.
#
# By Conor Peterson, N5XR (n5xr@replyhazy.net), 2024
# License: CC BY 4.0 (https://creativecommons.org/licenses/by/4.0)
# 
# This script uses the public SOTA API (https://api2.sota.org.uk/docs/index.html) to
# monitor spots for your association, and perform deltas of your association's summits
# at regular intervals to see who has activated what.
#
# SOTA spots are refreshed each hour and stored to a local sqlite database.
# Once per month this data is summarized for your association code.
#
# Additionally, each month, the most recent activation data for every summit in the
# association is queried, saved, and compared to last month. All differences are
# then summarized.
#
# Reports are in plain text format.
#
# This script is intended to be run 24/7 on an unattended computer, i.e. server at home
# or in the cloud. You could run it on your desktop PC or even a raspberry pi if it's
# online 24/7.
#
# I developed this script for W5N (New Mexico, USA) so that our community could reach
# out to new activators, and to highlight initial and rare activations when they happen.
# Personally, I copy the monthly reports into an email to my association's reflector and
# write some color commentary. I never expected to release this code so please excuse
# its fragility. I just scraped it off my "workbench" directly into github.
# -- CN (N5XR) / Santa Fe, NM

import sys
import time
from datetime import datetime, timedelta
import requests
import pickle
import locale
from pprint import pprint
from collections import Counter
import sqlite3

# Main API reference:
# https://api2.sota.org.uk/docs/index.html

# Set locale for parsing numbers
locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

#TODO
# Add query period (-2 hours) and query frequency (1 hour) to config
# Log association stats to separate table (store all then has more meaning)
# All other stats to a specific table, too
# Output as HTML to specific path
# Normalize incoming spots immediately, just like summits
# Adjustable report intervals - why must summits be reported every month? Why not each week, or day for busy associations?

config = {
    "server" : "https://api2.sota.org.uk",
    "association" : "w5n",
    "dbname" : "sotastats.db",
    "stats"    : "querystats.csv",
    "spots_table" : "spots",
    "summits_table" : "summits",
    "store_all" : True,
    "report_file" : "w5n_spot_summary.txt",
}

conn = None # sqlite3 connection

def initdb():
    global conn, config
    queries = [
        "CREATE TABLE IF NOT EXISTS `{}` (`id` INTEGER PRIMARY KEY, `timeStamp` TEXT, `activatorCallsign` TEXT, `associationCode` TEXT, `summitCode` TEXT, `frequency` STRING, `mode` TEXT, `summitDetails` TEXT, `comments` TEXT, `highlightColor` TEXT, `callsign` TEXT, `activatorName` TEXT, `userID` INTEGER)".format(config["spots_table"]),
        "CREATE TABLE IF NOT EXISTS `{}` (`summitCode` TEXT, `name` TEXT, `points` INTEGER, `activationCount` INTEGER, `activationDate` TEXT, `activationCall` TEXT, `refreshed` TEXT)".format(config["summits_table"])
    ]
    
    conn = sqlite3.connect(config["dbname"])
    for q in queries:
        conn.execute(q)

def query_spots():
    req_str = f"{config['server']}/api/spots/{-2}/all"
    
    now = datetime.now()
    print(f"{now}", end="")
    try:
        response = requests.get(req_str).json()
    except Exception as e:
        print(f"Exception in request: {e}")
        return None
    
    l = len(response)
    print(f"\tread {l}")
    
    with open(config["stats"], "a") as f:
        f.write("{} {}\n".format(now, l))
        
    return response

def query_summits():
    # Summit data is retrieved by region code, so first query the association to learn its regions
    assoc_query = f"{config['server']}/api/associations/{config['association']}"

    now = datetime.now()
    print(f"{now} refreshing summits\t", end="")

    try:
        response = requests.get(assoc_query).json()
    except Exception as e:
        print(f"Exception in request for association {config['association']}: {e}")
        return None
    
    region_list = [entry["regionCode"] for entry in response["regions"]]
    
    # All summits are given the exact same timestamp to simplify batching
    batch_timestamp = datetime.utcnow()
    
    summits = []
    for r in region_list:
        print(f"{r} ", end="")
        summit_query = f"{config['server']}/api/regions/{config['association']}/{r}"
        try:
            response = requests.get(summit_query).json()
        except Exception as e:
            print(f"Exception in summit request for region {r}: {e}")
        for s in response["summits"]:
            summits.append(normalize_summit(s, batch_timestamp))
    print(f"\n\tRead {len(summits)} summits")
    return summits

# Examine and normalize a json summit from the api.
# 's' is a dict containing all of the summit fields as returned by the server.
# timestamp is a datetime object representing a batch date to apply
# Returns a new dict with normalized data or throws an exception
def normalize_summit(s, timestamp):
    n = dict()
    n["summitCode"] = s["summitCode"].upper().strip()
    n["name"] = s["name"].strip()
    n["points"] = int(s["points"])
    n["activationCount"] = int(s["activationCount"])
    if n["activationCount"] == 0:
        n["activationDate"] = None
        n["activationCall"] = None
    else:
        n["activationDate"] = normalize_sota_timestamp(s["activationDate"])
        n["activationCall"] = s["activationCall"].upper().strip()
    n["refreshed"] = timestamp
    return n

# Examine and normalize a json spot from the api.
# 's' is a dict containing all the spot fields
# Returns a new dict containing normalized data or throws an exception
def normalize_spot(s):
    n = dict()
    
    # id has to be an integer
    n["id"] = int(s["id"])
    
    n["timeStamp"] = normalize_sota_timestamp(s["timeStamp"])

    # Text fields - clean up whitespace, ensure upper case where appropriate
    n["activatorCallsign"] = s["activatorCallsign"].upper().strip()
    n["associationCode"] = s["associationCode"].upper().strip()
    n["summitCode"] = s["summitCode"].upper().strip()
    n["mode"] = s["mode"].strip()
    n["summitDetails"] = s["summitDetails"].strip()
    n["comments"] = s["comments"].strip() if type(s["comments"]) is str else None # Comments is sometimes None
    n["highlightColor"] = s["highlightColor"].strip()
    n["callsign"] = s["callsign"].upper().strip()
    n["activatorName"] = s["activatorName"].strip()
    
    # Frequency: store as text, but make sure it's a parseable number
    _ = locale.atof(s["frequency"]) # will throw a ValueError if can't be parsed as a float
    n["frequency"] = locale.atof(s["frequency"])
    
    # userID is currently provided by SOTA API but it's always zero. Save whatever we get, or if it's missing entirely just
    # set it to zero.
    try:
        n["userID"] = int(s["userID"])
    except (KeyError, ValueError):
        n["userID"] = 0
    
    return n

# The SOTA API drops trailing zeros from milliseconds which causes fromisoformat to choke.
# This is a little workaround that just drops the milliseconds completely (for now)
# timestamp is the exact string as returned from the API server.
# returns a datetime object
def normalize_sota_timestamp(timestamp):
    if "." in timestamp:
        ts,ms = timestamp.split(".")
        return datetime.fromisoformat(ts)
    else:
        return datetime.fromisoformat(timestamp)
    
def store_spots(spots):
    global config
    
    store_cmd = "REPLACE INTO `{}` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(config["spots_table"])

    for s in spots:
        try:
            n = normalize_spot(s)
        except Exception as e:
            print("Warning: could not normalize spot:")
            pprint(s)
            print(f"Reason given: {e}")
            continue        
        if n["associationCode"].upper() != config["association"].upper() and not config["store_all"]:
            print("Skipping association code {}".format(n["associationCode"]))
            continue
        try:
            conn.execute(store_cmd, (n["id"], n["timeStamp"], n["activatorCallsign"],
                                     n["associationCode"], n["summitCode"],
                                     n["frequency"], n["mode"], n["summitDetails"],
                                     n["comments"], n["highlightColor"], n["callsign"],
                                     n["activatorName"], n["userID"]))
        except Exception as e:
            print("Warning: could not store spot:")
            pprint(s)
            print(f"Reason given: {e}")

# store_summit expects the summit data to be normalized already, unlike store_spots
def store_summits(summits):
    global config
    store_cmd = "REPLACE INTO `{}` VALUES (?, ?, ?, ?, ?, ?, ?)".format(config["summits_table"])
    
    for s in summits:
        try:
            conn.execute(store_cmd, (s["summitCode"], s["name"], s["points"], s["activationCount"], s["activationDate"], s["activationCall"], s["refreshed"]))
        except Exception as e:
            print("Warning: could not store summit:")
            pprint(s)
            print(f"Reason given: {e}")
            
# daily_report queries the database for individual activations of that day.
# "ending on" is a timestamp reflecting the end period, with local timezone attached
# (for a day, the day ending on the supplied period)
def daily_report(ending_on):
    from collections import Counter
    
    end_timestamp = datetime(year=ending_on.year, month=ending_on.month, day=ending_on.day,
                             hour=23, minute=59, second=59)
    begin_timestamp = datetime(year=ending_on.year, month=ending_on.month, day=ending_on.day,
                               hour=0, minute=0, second=0)
        
    query = "SELECT date(timeStamp), activatorCallsign, summitCode FROM `{}` WHERE `associationCode` == '{}' AND `timeStamp` >= '{}' AND `timeStamp` <= '{}'".format(config["spots_table"], config["association"].upper(), begin_timestamp, end_timestamp)

    try:
        result = list(conn.execute(query))
    except Exception as e:
        print(f"Warning: unable to retrieve results. Reason given: {e}")
        return
    
    with open(config["report_file"], "a") as f:
        f.write("\n--------------------------------------------------------------------\n")        
        msg = "Daily summary of SOTA spots for {} from {} to {} UTC\n".format(
        config["association"].upper(),
        begin_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        end_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
        print(msg, end="")
        f.write(msg)

        if len(result) == 0:
            msg = "No spots were captured for the association during this period.\n"
            print(msg, end="")
            f.write(msg)
        else:
            for date, op, peak in Counter(result).keys():
                msg = f"{date:<14}{op:<14}{peak}\n"
                print(msg, end="")
                f.write(msg)
        msg = "Report generated {}\n".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
        print(msg, end="")
        f.write(msg)

def monthly_report_spots(year_number, month_number):
    begin_timestamp = datetime(year=year_number, month=month_number, day=1, hour=0, minute=0, second=0)
    if month_number == 12:
        end_timestamp = datetime(year=year_number+1, month=1, day=1, hour=0, minute=0, second=0)
    else:
        end_timestamp = datetime(year=year_number, month=month_number+1, day=1, hour=0, minute=0, second=0)
    query = "SELECT date(timeStamp), activatorCallsign, summitCode FROM `{}` WHERE `associationCode` == '{}' AND `timeStamp` >= '{}' AND `timeStamp` < '{}'".format(config["spots_table"], config["association"].upper(), begin_timestamp, end_timestamp)

    try:
        result = list(conn.execute(query))
    except Exception as e:
        print(f"Warning: unable to retrieve results. Reason given: {e}")
        return
    
    with open(config["report_file"], "a") as f:
        f.write("\n====================================================================\n")
        msg = "Monthly summary of SOTA spots for {} from {} to {} UTC\n".format(
        config["association"].upper(),
        begin_timestamp.strftime("%Y-%m-%d"),
        end_timestamp.strftime("%Y-%m-%d"))
        print(msg, end="")
        f.write(msg)
        
        if len(result) == 0:
            msg = "No spots were captured for the association during this period.\n"
            print(msg, end="")
            f.write(msg)
        else:
            for date, op, peak in Counter(result).keys():
                msg = f"{date:<14}{op:<14}{peak}\n"
                print(msg, end="")
                f.write(msg)
                
        msg = "Report generated {}\n".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
        print(msg, end="")
        f.write(msg)

# Tees output to both screen and an open file, f
def output(f, msg):
    f.write(msg)
    print(msg, end="")

# monthly_summit_report: this report depends on the summit list being refreshed exactly once
# per month, just prior to running each report, so that the activation count can be compared.
def monthly_report_summits(year_number, month_number):
    # Get a list of all summits activated for the month in question
    begin_timestamp = datetime(year=year_number, month=month_number, day=1, hour=0, minute=0, second=0)
    if month_number == 12:
        end_timestamp = datetime(year=year_number+1, month=1, day=1, hour=0, minute=0, second=0)
    else:
        end_timestamp = datetime(year=year_number, month=month_number+1, day=1, hour=0, minute=0, second=0)

    query = f"SELECT * FROM `summits` WHERE `activationDate` >= '{begin_timestamp}' and `activationDate` < '{end_timestamp}' ORDER BY `summitCode`, `refreshed`, `activationDate` ASC" 
    try:
        hot_summits = list(conn.execute(query))
    except Exception as e:
        print(f"Warning: unable to retrieve results. Reason given: {e}")
        return

    print("Hot summits list:")
    for s in hot_summits:
        print(s)
    # Deduplicate - this query won't always run exactly one month apart
    deduplicated = []
    for s in hot_summits:
        found = False
        for j in hot_summits:
            if j[0] == s[0]:
                found = True
                break
        if not found:
            deduplicated.append(s)
                
    
    print(f"The following summits were activated during the period between {begin_timestamp} and {end_timestamp}:")

    f = open(config["report_file"], "a")
    output(f, "\n====================================================================\n")
    output(f, "Ref         Name                      Ct. Tot.  Pts. Most Recently By\n")
    
    suppress_stale_data_warnings = False
    
    # For every summit in this list, query for the activation history of the summit, ordered by refresh date.
    # We only need to pull the two most recent records.
    # NOTE: ensure that the list of summits is refreshed just prior to running this report.
    
    saw_initial = False
    saw_rare = False
    
    for summit in hot_summits:
        query = "SELECT * FROM `summits` WHERE `summitCode` == '{}' ORDER BY `refreshed` DESC LIMIT 2".format(summit[0])
        summit_history = list(conn.execute(query))
        
        # Basic sanity checks
        if len(summit_history) != 2:
            output(f, "Error: not enough data for summit {}\n".format(summit[0]))
            continue
        
        # Possible that the data is not really a month old - throw a warning if so
        # ...but only the first time.
        if not suppress_stale_data_warnings:
            most_recent_refresh = datetime.fromisoformat(summit_history[0][6])
            previous_refresh = datetime.fromisoformat(summit_history[1][6])
            td = (most_recent_refresh - previous_refresh).days
            if td < 29: # I mean, that's about a month right?
                output(f, f"Warning: data less than a month old (td={td} days)\n")
            elif td > 31:
                output(f, f"Warning: data older than one month (td={td} days)\n")
            suppress_stale_data_warnings = True

        # Compare the activation counts of the first two entries in the list and print report
        #print("{}  {:<32}, {:<2} points, activated {} times, most recently by {}".format(
        #    summit[0], summit[1], summit[2],
        #    summit[3] - summit_history[1][3],
        #    summit[5]))
        
        # Summit, Name, Count, Total, Points, Most Recently, By
        ref = summit[0]
        name = summit[1]
        count = summit[3] - summit_history[1][3]
        total = summit[3]
        points = summit[2]
        most_recently = summit[4]
        by = summit[5]
        
        if ' ' in most_recently:
            date, time = most_recently.split(' ')
            most_recently = date
        if total <= 1:
            badge = "★"
            saw_initial = True
        elif total <= 5:
            badge = "☆"
            saw_rare = True
        else:
            badge = " "
        output(f, "{:12}{:24}  {:<2}  {:<4}  {:<2}   {} {} {}\n".format(ref, name, count, total, points, most_recently, by, badge))
    if saw_initial:
        output(f, "★: Initial activation. Congratulations!\n")
    if saw_rare:
        output(f, "☆: Rare summit, five or fewer activations\n")
    f.close()

def main():
    global conn
    initdb()
        
    previous = None
    while True:
        now = datetime.utcnow()
        if previous == None or now.hour != previous.hour:
            spot_data = query_spots()
            if spot_data is not None:
                store_spots(spot_data)
                conn.commit()
        if previous != None and now.day != previous.day:
            daily_report(previous)
        if previous != None and now.month != previous.month:
            monthly_report_spots(previous.year, previous.month)
            summit_data = query_summits()
            store_summits(summit_data)
            conn.commit()
            monthly_report_summits(previous.year, previous.month)
        previous = now
        time.sleep(1)
    conn.close()
    return 0
            
if __name__ == "__main__":
    sys.exit(main())
