# NYC Taxi Analytics and ML

A minimal Scala and Apache Spark project for an NYC Taxi Data analytics and machine learning assignment.

## Tech Stack

- Scala 2.12
- Apache Spark Core
- Spark SQL
- Spark MLlib
- Akka HTTP
- sbt

## Project Structure

```text
.
├── build.sbt
├── project
│   └── build.properties
├── README.md
└── src
    └── main
        └── scala
            ├── api
            │   ├── ApiServer.scala
            │   ├── models
            │   │   ├── AverageDurationByHourResponse.scala
            │   │   ├── AverageFareByHourResponse.scala
            │   │   ├── FarePredictionRequest.scala
            │   │   ├── FarePredictionResponse.scala
            │   │   ├── HotspotResponse.scala
            │   │   ├── TripDurationPredictionRequest.scala
            │   │   └── TripDurationPredictionResponse.scala
            │   └── routes
            │       ├── AnalyticsRoutes.scala
            │       ├── FareRoutes.scala
            │       └── TripDurationRoutes.scala
            ├── Main.scala
            ├── config
            │   └── SparkSessionFactory.scala
            ├── features
            │   └── FeatureEngineer.scala
            ├── io
            │   └── DataLoader.scala
            ├── models
            │   ├── FeaturedTaxiTrip.scala
            │   └── TaxiTrip.scala
            ├── preprocessing
            │   └── DataCleaner.scala
            ├── usecases
            │   ├── analytics
            │   │   ├── AnalyticsQueryService.scala
            │   │   └── AnalyticsSqlQueries.scala
            │   ├── duration
            │   │   ├── TripDurationPredictionModel.scala
            │   │   └── TripDurationPredictionService.scala
            │   └── fare
            │       ├── FarePredictionModel.scala
            │       └── FarePredictionService.scala
            └── utils
                └── SchemaUtils.scala
```

## How to Run

Train and save models with a CSV or Parquet dataset path:

```bash
sbt "run train /path/to/nyc-taxi-dataset"
```

For local development, you can place downloaded NYC Taxi data anywhere convenient, such as a `data/` folder in this project, and pass that path as the first argument:

```bash
sbt "run train data/yellow_tripdata_2024-01.parquet"
```

CSV files are also supported:

```bash
sbt "run train data/yellow_tripdata_2024-01.csv"
```

For fast local development, use the 100-row sample:

```bash
sbt "run train data/yellow_tripdata_2016-03_sample_100.csv"
```

Saved models are written to:

```text
trained-models/fare-prediction
trained-models/trip-duration-prediction
```

Start the prediction APIs from saved models without loading the dataset:

```bash
sbt "run api"
```

Start prediction APIs plus Spark SQL analytics APIs:

```bash
sbt "run api-analytics data/yellow_tripdata_2016-03_sample_100.csv"
```

The API server runs at:

```text
http://localhost:8080
```

Test fare prediction with:

```bash
curl -X POST http://localhost:8080/predict/fare \
  -H "Content-Type: application/json" \
  -d '{
    "pickup_longitude": -73.97674560546875,
    "pickup_latitude": 40.76515197753906,
    "dropoff_longitude": -74.00426483154297,
    "dropoff_latitude": 40.74612808227539,
    "trip_distance": 2.5,
    "passenger_count": 1,
    "pickup_hour": 0,
    "pickup_day_of_week": 3,
    "pickup_month": 3,
    "is_weekend": 0
  }'
```

Example response:

```json
{
  "predicted_fare": 10.15
}
```

Test trip duration prediction with:

```bash
curl -X POST http://localhost:8080/predict/duration \
  -H "Content-Type: application/json" \
  -d '{
    "pickup_longitude": -73.97674560546875,
    "pickup_latitude": 40.76515197753906,
    "dropoff_longitude": -74.00426483154297,
    "dropoff_latitude": 40.74612808227539,
    "trip_distance": 2.5,
    "passenger_count": 1,
    "pickup_hour": 0,
    "pickup_day_of_week": 3,
    "pickup_month": 3,
    "is_weekend": 0
  }'
```

Example response:

```json
{
  "predicted_duration_minutes": 8.5
}
```

Get top pickup hotspots:

```bash
curl "http://localhost:8080/analytics/hotspots?limit=10"
```

Example response:

```json
[
  {
    "pickup_location_id": "coord:40.765,-73.977",
    "trip_count": 2
  }
]
```

For 2016 CSV files, `pickup_location_id` may be a coordinate key because the file has longitude/latitude instead of taxi zone IDs.

Get average fare by pickup hour:

```bash
curl http://localhost:8080/analytics/average-fare-by-hour
```

Example response:

```json
[
  {
    "pickup_hour": 0,
    "average_fare": 12.45
  }
]
```

Get average trip duration by pickup hour:

```bash
curl http://localhost:8080/analytics/average-duration-by-hour
```

Example response:

```json
[
  {
    "pickup_hour": 0,
    "average_duration_minutes": 9.75
  }
]
```

The app currently:

- starts a local Spark session
- loads CSV or Parquet input
- prints the raw schema and sample rows
- selects normalized NYC Taxi trip columns
- supports newer zone ID columns and older longitude/latitude columns
- removes invalid rows
- prints row counts before and after cleaning
- shows a sample of cleaned rows
- engineers reusable analytics and ML features
- trains a baseline fare prediction model
- trains a baseline trip duration prediction model
- runs Spark SQL analytics for hotspots, average fares, and average durations
- exposes prediction and analytics endpoints as local JSON APIs

