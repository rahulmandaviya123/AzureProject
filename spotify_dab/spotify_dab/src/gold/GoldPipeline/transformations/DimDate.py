import dlt

@dlt.table
def dimdate_stg():
    df = spark.readStream.table("spotify_catalog.silver.dimdate")
    return df
dlt.create_streaming_table("DimDate")

dlt.create_auto_cdc_flow(
  target = "DimDate",
  source = "dimdate_stg",
  keys = ["date_key"],
  sequence_by = "date",
  stored_as_scd_type = 2
)