def to_lowercase_columns(df):

    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    return df


def drop_metadata_columns(df):

    technical_columns = [
        "load_date",
        "source_file"
    ]

    return df.drop(*technical_columns)