import dlt

@dlt.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_catalog.silver.dimtrack")
    return df
dlt.create_streaming_table("DimTrack")

dlt.create_auto_cdc_flow(
  target = "DimTrack",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2
)
