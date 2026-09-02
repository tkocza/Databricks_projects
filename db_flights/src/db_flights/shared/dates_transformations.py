from pyspark.sql import functions as F

def create_date_column(year_col, month_col, day_col):
    date_col = F.to_date(F.concat_ws("-", year_col, month_col, day_col), 'yyyy-M-d')
    return date_col

def create_timestamp_column(date_col, time_hhmm_col):
    
    # add leading zeros eg. 5 -> 0005, 830 -> 0830
    hhmm = F.lpad(F.col(time_hhmm_col).cast("string"), 4, "0")

    # fix 2400: change to 0000
    fixed_hhmm = F.when(hhmm == "2400", F.lit("0000")).otherwise(hhmm)

    # build timestamp
    # ts = F.to_timestamp(F.concat_ws(" ", date_col, fixed_hhmm), "yyyy-M-d HHmm")
    ts = F.when(hhmm.isNotNull(), F.to_timestamp(F.concat_ws(" ", date_col, fixed_hhmm), "yyyy-M-d HHmm"))
    
    # if 2400, add one day
    ts_corrected = F.when(hhmm == "2400",ts + F.expr("INTERVAL 1 DAY")).otherwise(ts)

    return ts_corrected