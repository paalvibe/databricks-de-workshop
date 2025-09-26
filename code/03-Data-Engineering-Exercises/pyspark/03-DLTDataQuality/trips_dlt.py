@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_or_fail("pickup_borough_not_null", "pickup_borough IS NOT NULL")
def curated():
  return (
    spark.table("training.taxinyc_trips.yellow_taxi_trips_curated")
    .where(
        # limit dataset to get faster processing
        "trip_year = 2016 and trip_month == '02'"
    )
  )

@dlt.table(
  comment="Trips by month and borough."
)
@dlt.expect_or_fail("pickup_borough_not_null", "pickup_borough IS NOT NULL")
def trips_by_month_and_borough():
  return (
    dlt.read("curated")
      .groupBy("pickup_borough", "trip_month")
      .count()
  )