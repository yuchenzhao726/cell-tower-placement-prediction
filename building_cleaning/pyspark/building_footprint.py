from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, DoubleType
from shapely.geometry import Polygon
import pyproj
import argparse

def get_polygon_center(points):
    centroid = Polygon(points).centroid
    return centroid.x, centroid.y

def get_polygon_area(points):
    # Define the target CRS as a projected CRS suitable for area calculations in square meterss
    crs_transformer = pyproj.Transformer.from_crs('EPSG:4326', 'EPSG:3395', always_xy=True)
    x_coords, y_coords = zip(*points)
    transformed_x, transformed_y = crs_transformer.transform(x_coords, y_coords)
    transformed_points_list = list(zip(transformed_x, transformed_y))
    transformed_polygon = Polygon(transformed_points_list)
    return transformed_polygon.area

if __name__ == "__main__":
    TRAIN_LONGITUDE_L = -125.0
    TRAIN_LONGITUDE_H = -71.0
    TRAIN_LATITUDE_L = 33.0
    TRAIN_LATITUDE_H = 42.0

    TEST_LONGITUDE_L = -124.0
    TEST_LONGITUDE_H = -117.0
    TEST_LATITUDE_L = 46.0
    TEST_LATITUDE_H = 48.0

    parser = argparse.ArgumentParser(description="BuildingFootprint")
    parser.add_argument('input_directory', help='HDFS input direcotry')
    parser.add_argument('training_output', help='HDFS training data set output directory')
    parser.add_argument('test_output', help='HDFS test data set output directory')
    args = parser.parse_args()

    # Create the Spark session
    spark = SparkSession.builder.appName("BuildingFootprint").getOrCreate()

    # Read the geoJson file from the directory
    df = spark.read.option("multiLine", "true").json(args.input_directory)

    # Expand the polygon coordinates under tag features
    building_df = df.select(explode(col("features")).alias("building")) \
                    .select(col("building.geometry.coordinates")[0].alias("coordinates"))

    # Define UDFs to get polygon centroid
    get_centroid_udf = udf(lambda points:get_polygon_center(points), ArrayType(DoubleType()))

    # Apply UDFs to the coordinates column and create new columns for centroid
    result_df = building_df.withColumn("polygon_centroid", get_centroid_udf(col("coordinates"))) \
        .withColumn("center_longitude", col("polygon_centroid")[0]) \
        .withColumn("center_latitude", col("polygon_centroid")[1]) \
        .drop("polygon_centroid")

    # Filter the result to get the training and test set
    train_df = result_df.filter(
        (col("center_longitude") <= TRAIN_LONGITUDE_H) &
        (col("center_longitude") >= TRAIN_LONGITUDE_L) &
        (col("center_latitude") <= TRAIN_LATITUDE_H) &
        (col("center_latitude") >= TRAIN_LATITUDE_L)
    ).sample(0.3, 123)
    test_df = result_df.filter(
        (col("center_longitude") <= TEST_LONGITUDE_H) &
        (col("center_longitude") >= TEST_LONGITUDE_L) &
        (col("center_latitude") <= TEST_LATITUDE_H) &
        (col("center_latitude") >= TEST_LATITUDE_L)
    ).sample(0.3, 123)

    # Define UDFs to get polygon area
    get_area_udf = udf(lambda points:get_polygon_area(points), DoubleType())

    # Apply UDF to get polygon area and add new columns as area
    result_train_df = train_df.withColumn("area", get_area_udf(col("coordinates"))).drop("coordinates")
    result_test_df = test_df.withColumn("area", get_area_udf(col("coordinates"))).drop("coordinates")

    result_train_df.write.mode("append").format("parquet").option("compression", "snappy").save(args.training_output)
    result_test_df.write.mode("append").format("parquet").option("compression", "snappy").save(args.test_output)

    spark.stop()
