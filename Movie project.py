# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType



# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://project-movies-data@moviesdatatutorial.blob.core.windows.net',
    mount_point='/mnt/project-movies-data',
    extra_configs = {'fs.azure.account.key.moviesdatatutorial.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}
)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/project-movies-data"
# MAGIC

# COMMAND ----------

action = spark.read.format("csv").load("/mnt/project-movies-data/raw-data/action.csv")

# COMMAND ----------

action.show()

# COMMAND ----------

action = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/action.csv")

# COMMAND ----------

action.show()

# COMMAND ----------

action = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/action.csv")
adventure = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/adventure.csv")
horror = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/horror.csv")
scifi = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/scifi.csv")
thriller = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/thriller.csv")

# COMMAND ----------

action.printSchema()

# COMMAND ----------

action = action.withColumn("rating", col("rating").cast(IntegerType()))

# COMMAND ----------

all_movies_rated_highest = action.orderBy("rating", ascending=False).limit(20).show()

# COMMAND ----------

all_movies_rated_highest = action.orderBy("rating", ascending=False).select("movie_name", "genre", "rating").limit(15).show()

# COMMAND ----------

comedy_movies = action.filter(col("genre").contains("Comedy")).limit(15).show()

# COMMAND ----------

action.write.option("header",'true').csv("/mnt/project-movies-data/transformed-data/action")

# COMMAND ----------

action.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/action")

# COMMAND ----------

adventure.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/adventure")
horror.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/horror")
scifi.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/scifi")
thriller.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/thriller")

# COMMAND ----------

action = action.withColumn("year", col("year").cast(DateType()))

# COMMAND ----------

from pyspark.sql.functions import col, date_format

action = action.withColumn("year", date_format(col("year"), "yyyy"))


# COMMAND ----------

all_movies_rated_highest = action.orderBy("rating", ascending=False).select("movie_name", "genre", "rating").limit(15).show()

# COMMAND ----------

# Read the CSV file as a Spark DataFrame - Like we did above in the top cells
action = spark.read.format("csv") \
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .load("/mnt/project-movies-data/raw-data/action.csv") \
                  .createOrReplaceTempView("temp_table")

# Create a Spark table from the temporary view - temp_table
spark.sql("CREATE TABLE IF NOT EXISTS actiontb USING parquet AS SELECT * FROM temp_table")

# Query for movies with rating = 8
query_result = spark.sql("SELECT year, movie_name, rating FROM actiontb WHERE rating = 8")

# Import Plotly Express for visualisation
import plotly.express as px

# Create a Pandas DataFrame for plotting if ya want to.
pandas_df = query_result.toPandas()

# Group by year, count movies, and create a DataFrame with "year" and "count" columns
grouped_df = pandas_df.groupby("year").size().to_frame(name="count").reset_index()

# Create the bar chart using Plotly
fig = px.bar(grouped_df, x="year", y="count")
fig.update_layout(width=900, height=600)  # Set plot size
fig.show()  # Display the plot


# COMMAND ----------


# Read the CSV file as a Spark DataFrame
action = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/action.csv") \
    .createOrReplaceTempView("temp_table")

# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS actiontb USING parquet AS SELECT * FROM temp_table")

# Query for movies with rating = 8
query_result = spark.sql("SELECT year, movie_name, rating FROM actiontb WHERE rating = 8")

# Import Plotly Express for visualisation
import plotly.express as px

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by year, count movies, and create a DataFrame with "year" and "count" columns
grouped_df = pandas_df.groupby("year").size().to_frame(name="count").reset_index()

# Create the bar chart using Plotly (with color customisation)
fig = px.bar(grouped_df, x="year", y="count", color="count", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()


# COMMAND ----------

# Read the CSV file as a Spark DataFrame
action = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/action.csv") \
    .createOrReplaceTempView("temp_table")

# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS actiontb USING parquet AS SELECT * FROM temp_table")

# Query for movies with rating = 8
query_result = spark.sql("SELECT year, movie_name, rating FROM actiontb WHERE rating = 8")

# Import Plotly Express for visualisation
import plotly.express as px

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by year, count movies, and create a DataFrame with "year" and "count" columns
grouped_df = pandas_df.groupby("year").size().to_frame(name="count").reset_index()

# Create the bar chart using Plotly (with color customisation)
my_palette = ["#FF4858", "#1B7F79", "#00CCC0", "#72F2EB", "#747F7F"]  # Define your color palette
fig = px.scatter(grouped_df, x="year", y="count", color="count", color_continuous_scale=my_palette)

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()


# COMMAND ----------

from pyspark.sql.functions import col, date_format
scifi = scifi.withColumn("year", col("year").cast(DateType()))


# COMMAND ----------

scifi = scifi.withColumn("rating", col("rating").cast(IntegerType()))

# COMMAND ----------

adventure.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/adventure")
horror.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/horror")
thriller.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/thriller")


# COMMAND ----------

# Read the CSV files as Spark DataFrames
action = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/action.csv")

scifi = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/scifi.csv")

adventure = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/adventure.csv")

horror = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/horror.csv")

thriller = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/thriller.csv")

# Create temporary views for querying
action.createOrReplaceTempView("action_view")
scifi.createOrReplaceTempView("scifi_view")
adventure.createOrReplaceTempView("adventure_view")
horror.createOrReplaceTempView("horror_view")
thriller.createOrReplaceTempView("thriller_view")

# Combine movie counts by year
query_result = spark.sql("""
SELECT year,
       SUM(CASE WHEN genre = 'action' THEN 1 ELSE 0 END) AS action_count,
       SUM(CASE WHEN genre = 'scifi' THEN 1 ELSE 0 END) AS scifi_count,
       SUM(CASE WHEN genre = 'horror' THEN 1 ELSE 0 END) AS horror_count,
       SUM(CASE WHEN genre = 'thriller' THEN 1 ELSE 0 END) AS thriller_count,
       SUM(CASE WHEN genre = 'adventure' THEN 1 ELSE 0 END) AS adventure_count
FROM (
  SELECT year, movie_name, rating, 'action' AS genre
  FROM action_view
  UNION ALL
  SELECT year, movie_name, rating, 'scifi' AS genre
  FROM scifi_view
   UNION ALL
  SELECT year, movie_name, rating, 'adventure' AS genre
  FROM adventure_view
   UNION ALL
  SELECT year, movie_name, rating, 'horror' AS genre
  FROM horror_view
   UNION ALL
  SELECT year, movie_name, rating, 'thriller' AS genre
  FROM thriller_view
) combined
GROUP BY year
ORDER BY year ASC
""")

# Import Plotly Express for visualisation
import plotly.express as px

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Create the bar chart using Plotly
fig = px.bar(pandas_df, x="year", y=["action_count", "scifi_count", "horror_count", "adventure_count", "thriller_count"], barmode="group", color_continuous_scale="Turbo")
             
# Set y-axis range to 0-200
fig.update_layout(yaxis_range=[0, 165]) 

# Change `barmode` from "stack" to "group"
fig.update_layout(barmode="group")

# Set plot size and other formatting options
fig.update_layout(width=1200, height=800, legend_title="Genre", legend_title_font_size=12)

# Display the plot
fig.show()

