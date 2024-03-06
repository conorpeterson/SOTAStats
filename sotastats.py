import sys
import time
from datetime import datetime, timedelta
import requests
import pickle
import locale
from pprint import pprint
from collections import Counter
import sqlite3

#TODO make everything UTC, no need to import timezone even

# Main API reference:
# https://api2.sota.org.uk/docs/index.html

# Set locale for parsing numbers
locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

config = {
    "server" : "https://api2.sota.org.uk",
    "dbname" : "sotastats.db",
    "stats"    : "querystats.csv",
    "spots_table" : "spots",
    "store_all" : True,
    "report_file" : "w5n_spot_summary.txt",
}

conn = None # sqlite3 connection

def query():
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

def initdb():
    global conn, config
    
    create_table_query = "CREATE TABLE IF NOT EXISTS `{}` (`id` INTEGER PRIMARY KEY, `timeStamp` TEXT, `activatorCallsign` TEXT, `associationCode` TEXT, `summitCode` TEXT, `frequency` STRING, `mode` TEXT, `summitDetails` TEXT, `comments` TEXT, `highlightColor` TEXT, `callsign` TEXT, `activatorName` TEXT, `userID` INTEGER)".format(config["spots_table"])
    
    conn = sqlite3.connect(config["dbname"])
    conn.execute(create_table_query)


# Examine and normalize a json spot from the response.
# 's' is a dict containing all the spot fields
# Returns a new dict containing normalized data or throws an exception
def normalize_spot(s):
    n = dict()
    
    # id has to be an integer
    n["id"] = int(s["id"])
    
    # Timestamp - SOTA API drops the zero off of milliseconds, which causes fromisoformat to choke.
    # Workaround by dropping milliseconds altogether
    if "." in s["timeStamp"]:
        ts,ms = s["timeStamp"].split(".")
        n["timeStamp"] = datetime.fromisoformat(ts)
    else:
        n["timeStamp"] = datetime.fromisoformat(s["timeStamp"])

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

def store(spots):
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
        if n["associationCode"].upper() != "W5N" and not config["store_all"]:
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

# select * from spots where timestamp <= '2024-02-26T03:30:00';
# month and year are integers
#def report_monthly(month, year):
#    begin_timestamp = f"{year}-{month:02}-01T00:00:00"
#    end_timestamp = "{year}-{month+1:02}-01T00:00:00"
#    query = "SELECT * from `spots` WHERE `timestamp` >= '{}' AND `timestamp` < '{}'".format(begin_timestamp, end_timestamp)
#    resultset = conn.execute(query)

# daily_report queries the database for individual activations of that day.
# "ending on" is a timestamp reflecting the end period, with local timezone attached
# (for a day, the day ending on the supplied period)
def daily_report(ending_on):
    from collections import Counter
    
    end_timestamp = datetime(year=ending_on.year, month=ending_on.month, day=ending_on.day,
                             hour=23, minute=59, second=59)
    begin_timestamp = datetime(year=ending_on.year, month=ending_on.month, day=ending_on.day,
                               hour=0, minute=0, second=0)
        
    query = "SELECT date(timeStamp), activatorCallsign, summitCode FROM `{}` WHERE `associationCode` == 'W5N' AND `timeStamp` >= '{}' AND `timeStamp` <= '{}'".format(config["spots_table"], begin_timestamp, end_timestamp)

    try:
        result = list(conn.execute(query))
    except Exception as e:
        print(f"Warning: unable to retrieve results. Reason given: {e}")
        return
    
    with open(config["report_file"], "a") as f:
        f.write("\n--------------------------------------------------------------------\n")        
        msg = "Daily summary of SOTA spots for W5N from {} to {} UTC\n".format(
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

def monthly_report(year_number, month_number):
    begin_timestamp = datetime(year=year_number, month=month_number, day=1, hour=0, minute=0, second=0)
    if month_number == 12:
        end_timestamp = datetime(year=year_number+1, month=1, day=1, hour=0, minute=0, second=0)
    else:
        end_timestamp = datetime(year=year_number, month=month_number+1, day=1, hour=0, minute=0, second=0)
    query = "SELECT date(timeStamp), activatorCallsign, summitCode FROM `{}` WHERE `associationCode` == 'W5N' AND `timeStamp` >= '{}' AND `timeStamp` < '{}'".format(config["spots_table"], begin_timestamp, end_timestamp)

    try:
        result = list(conn.execute(query))
    except Exception as e:
        print(f"Warning: unable to retrieve results. Reason given: {e}")
        return
    
    with open(config["report_file"], "a") as f:
        f.write("\n====================================================================\n")
        msg = "Monthly summary of SOTA spots for W5N from {} to {} UTC\n".format(
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
    
    
def main():
    global conn
    initdb()
    
    previous = None
    while True:
        now = datetime.utcnow()
        if previous == None or now.hour != previous.hour:
            data = query()
            if data is not None:
                store(data)
                conn.commit()
        if previous != None and now.day != previous.day:
            daily_report(previous)
        if previous != None and now.month != previous.month:
            monthly_report(previous.year, previous.month)
        previous = now
        time.sleep(1)
    conn.close()
    return 0
            
if __name__ == "__main__":
    sys.exit(main())
