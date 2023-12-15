# spark-submit --deploy-mode cluster --conf spark.yarn.maxAppAttempts=5 --py-files pyproj-3.6.1.tar.gz,shapely-2.0.2.tar.gz building_footprint.py <input> <train_output> <test_output>

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, size
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField, StringType, MapType
from shapely.geometry import Polygon
import pyproj
import argparse

def parse_coordinates(points, geo_type):
    # Regardless the geometry shape we want to convert the output to a list of Polygons [[[x,y]]]
    # Also need to ensure all the coordinates are in double format (some of them is int in original dataset)
    # Ignore the possible z coordinates
    input_data = eval(points)
    if geo_type == "LineString":
        return [[[float(coordinate) for coordinate in point[:2]] for point in input_data]]
    if (geo_type == "Polygon"):
        return [[[float(coordinate) for coordinate in point[:2]] for point in polygon] for polygon in input_data]
    if geo_type == "MultiPolygon":
        if len(input_data) == 1:
            return [[[float(coordinate) for coordinate in point[:2]] for point in polygon] for polygon in input_data[0]]
        else:
            return [[[float(coordinate) for coordinate in point[:2]] for point in polygon[0]] for polygon in input_data]

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

    # There are multiple data shape (Polygon, LineString, MultiPolygon) in the geoJson file
    # With different shape there are different structure inside of coordinates column
    validSchema = StructType([
        StructField("type", StringType()),
        StructField("geometry", StructType([
          StructField("coordinates", StringType()),
          StructField("type", StringType(), False)
        ]), False),
        StructField("properties", MapType(StringType(), StringType()))
    ])

    # Read the geoJson file from the directory
    df = spark.read.json(args.input_directory, schema=validSchema).select(col("geometry.coordinates"), col("geometry.type"))

    parse_coordinates_udf = udf(lambda points, geo_type: parse_coordinates(points, geo_type), ArrayType(ArrayType(ArrayType(DoubleType()))))

    building_df = df.select("type", explode(parse_coordinates_udf(col("coordinates"), col("type"))).alias("uniformed_coordinates")) \
                    .filter(~((size(col("uniformed_coordinates")) < 3) | (col("type") == "Point"))) \
                    .persist()

    # Define UDFs to get polygon centroid
    get_centroid_udf = udf(lambda points: get_polygon_center(points), ArrayType(DoubleType()))

    # Apply UDFs to the coordinates column and create new columns for centroid
    result_df = building_df.withColumn("polygon_centroid", get_centroid_udf(col("uniformed_coordinates"))) \
        .withColumn("center_longitude", col("polygon_centroid")[0]) \
        .withColumn("center_latitude", col("polygon_centroid")[1]) \
        .drop("polygon_centroid") 

    # Filter the result to get the training and test set
    train_df = result_df.filter(
        (col("center_longitude") <= TRAIN_LONGITUDE_H) &
        (col("center_longitude") >= TRAIN_LONGITUDE_L) &
        (col("center_latitude") <= TRAIN_LATITUDE_H) &
        (col("center_latitude") >= TRAIN_LATITUDE_L)
    )
    test_df = result_df.filter(
        (col("center_longitude") <= TEST_LONGITUDE_H) &
        (col("center_longitude") >= TEST_LONGITUDE_L) &
        (col("center_latitude") <= TEST_LATITUDE_H) &
        (col("center_latitude") >= TEST_LATITUDE_L)
    )

    # Define UDFs to get polygon area
    get_area_udf = udf(lambda points: get_polygon_area(points), DoubleType())

    # Apply UDF to get polygon area and add new columns as area
    result_train_df = train_df.withColumn("area", get_area_udf(col("uniformed_coordinates"))).drop("uniformed_coordinates").drop("type")
    result_test_df = test_df.withColumn("area", get_area_udf(col("uniformed_coordinates"))).drop("uniformed_coordinates").drop("type")

    result_train_df.write.mode("append").format("parquet").option("compression", "snappy").save(args.training_output)
    result_test_df.write.mode("append").format("parquet").option("compression", "snappy").save(args.test_output)

    spark.stop()
