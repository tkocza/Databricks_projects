from pyspark.sql import DataFrame
from pyspark.sql import functions as F



def build_row_hash(cols):
    record_hash = F.sha2(F.concat_ws("||",
                                     *[F.concat(F.lit(c), 
                                     F.lit("="), 
                                     F.coalesce(F.col(c).cast("string"), 
                                     F.lit("NULL"))) for c in cols]), 256)
    return record_hash