from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.window import Window
from pyspark.ml.stat import Summarizer
from pyspark.ml.functions import vector_to_array

def kmeans_fit(data: DataFrame, init: DataFrame, k: int = 4, max_iter: int = 10) -> DataFrame:
    numeric_columns = [col for col in data.columns if data.schema[col].dataType.simpleString() in ('int', 'double', 'float')]

    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
    centroids = assembler.transform(init.select("*")).select("features")
    centroids = centroids.withColumn("label", f.row_number().over(Window.orderBy("features")))
    vector_df = assembler.transform(data).select("features")

    for _ in range(max_iter):
        # Broadcast centroids to speed up the join operation
        broadcast_centroids = f.broadcast(centroids.withColumnRenamed("features", "centroid_features").withColumnRenamed("label", "centroid_label"))

        vector_df = vector_df.withColumn("features_array", vector_to_array(f.col("features")))
        broadcast_centroids = broadcast_centroids.withColumn("centroid_features_array", vector_to_array(f.col("centroid_features")))

        # Cross join the DataFrames
        cross_joined_df = vector_df.crossJoin(f.broadcast(broadcast_centroids))

        # Compute the squared distance element-wise and sum them up
        squared_distance_expr = sum((f.col("features_array")[i] - f.col("centroid_features_array")[i]) ** 2 for i in range(3))

        cross_joined_df = cross_joined_df.withColumn("squared_distance", squared_distance_expr)
        cross_joined_df = cross_joined_df.withColumn("L2", f.sqrt(f.col("squared_distance")))


        ranked_df = cross_joined_df.withColumn("rank", f.row_number().over(Window.partitionBy("features").orderBy("L2")))

        # Filter to keep only the closest centroid (rank = 1) for each original vector
        result_df = ranked_df.filter(f.col("rank") == 1).select("features", "centroid_label")

        new_centroids = result_df.groupBy("centroid_label").agg(\
                                  Summarizer.metrics("mean").summary(f.col("features")).alias("features"))\
                                  .withColumn("features", f.col("features.mean"))


        centroids = centroids.withColumn("features_array", vector_to_array(f.col("features")))
        new_centroids = new_centroids.withColumn("features_array", vector_to_array(f.col("features")))

        # Compute L2 (Euclidean) distance for ordering
        centroids = centroids.withColumn("dist", f.sqrt(sum(f.col("features_array")[i] ** 2 for i in range(3))))
        new_centroids = new_centroids.withColumn("dist", f.sqrt(sum(f.col("features_array")[i] ** 2 for i in range(3))))

        # Order the DataFrames by the computed distance
        centroids_ordered = centroids.orderBy("dist")
        new_centroids_ordered = new_centroids.orderBy("dist")
        window_spec = Window.orderBy("dist")
        centroids_with_row_num = centroids.withColumn("row_num", f.row_number().over(window_spec))
        new_centroids_with_row_num = new_centroids.withColumn("row_num", f.row_number().over(window_spec))

        comparison_df = centroids_with_row_num.join(
        new_centroids_with_row_num,
        centroids_with_row_num["row_num"] == new_centroids_with_row_num["row_num"],"inner").select(\
              centroids_with_row_num["features"].alias("old_features"),\
                  new_centroids_with_row_num["features"].alias("new_features"))

        comparison_df = comparison_df.withColumn("old_features_array", vector_to_array(f.col("old_features")))
        comparison_df = comparison_df.withColumn("new_features_array", vector_to_array(f.col("new_features")))

        # Compute squared distances element-wise and sum them up
        squared_distance_expr = sum((f.col("old_features_array")[i] - f.col("new_features_array")[i]) ** 2 for i in range(3))

        # Add squared distance and distance columns
        comparison_df = comparison_df.withColumn("squared_distance", squared_distance_expr)
        comparison_df = comparison_df.withColumn("distance", f.sqrt(f.col("squared_distance")))


        has_large_distance = comparison_df.agg(
            f.max(f.when(f.col("distance") > 0.001, 1).otherwise(0)).alias("has_large_distance")
        ).first()["has_large_distance"]

        if has_large_distance == 0:
            break
        centroids = new_centroids.select("centroid_label", "features")

    return centroids.select(f.col("features").alias("centroids"))


