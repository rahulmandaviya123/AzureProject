import dlt

@dlt.table
def dimuser_stg():
    df = spark.readStream.table("spotify_catalog.silver.dimuser")
    return df
dlt.create_streaming_table("DimUser")

dlt.create_auto_cdc_flow(
  target = "DimUser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2
)
