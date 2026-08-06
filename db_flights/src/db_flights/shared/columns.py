def to_lowercase_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df