#!/usr/bin/env python
# coding: utf-8

# In[8]:


#get_ipython().run_line_magic('pip', 'install pandas')


# In[12]:


#get_ipython().run_line_magic('pip', 'install pyjstat')


# In[18]:


#get_ipython().run_line_magic('pip', 'install matplotlib')


# In[21]:


#get_ipython().run_line_magic('pip', 'install seaborn')


# In[2]:


import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyjstat import pyjstat
import os
from pyspark.sql.functions import col, lit
from dotenv import load_dotenv
import sys
from pyspark.sql import functions as F
import pyspark
from urllib.parse import quote_plus
import matplotlib.pyplot as plt


# In[3]:


from pyspark.sql.functions import to_date, col, trim
from pyspark.sql.functions import avg, sum, count, when, lag, desc 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# Set up output directory for plots
output_dir = "plots"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    
# ### 1. Fetch the Projected Population Data (PEC26) produced by CSO Ireland

# In[4]:


# Define the API URL for Table PEC26 (Regional Projections)
cso_url = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/PEC26/JSON-stat/1.0/en"

# fetch the data
print("Connecting to CSO PxStat API...")
response = requests.get(cso_url)

if response.status_code == 200:
    # Parse the JSON-stat data into a Pandas DataFrame
    dataset = pyjstat.Dataset.read(response.text)
    projections_df = dataset.write('dataframe')

    print("Success! Row count:", projections_df.count())
    # Show the first few rows
    print(projections_df.head())
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")


# In[5]:


# filter for the 'M2F1' scenario which is the standard 'Moderate' projection
filtered_df = projections_df[
    (projections_df['Criteria for Projection'] == 'Method - M1') & 
    (projections_df['Sex'] == 'Both sexes') &
    (projections_df['Age'] == 'All ages') 
]

# Ensure 'Year' is an integer for Spark join
filtered_df['Year'] = filtered_df['Year'].astype(int)

print("Success! Row count:", filtered_df.count())
print(filtered_df.head())


# #### 2. Set Package lists and Create Spark Session

# In[1]:


import os

os.environ.pop("SPARK_HOME", None)
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.hadoop:hadoop-azure:3.3.4,"
    "com.microsoft.azure:azure-storage:8.6.6,"
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
    "pyspark-shell"
)


# In[4]:


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("azure-mongo-project") \
    .getOrCreate()

print("Spark started")
print("Spark version:", spark.version)


# In[6]:


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("analysis-session") \
    .getOrCreate()

print("Spark version:", spark.version)


# In[7]:


# load azure and mongodb credentials
from dotenv import load_dotenv
# Load variables from .env into the system environment
load_dotenv("/home/miracle/project/.env")

azure_key = os.getenv("AZURE_STORAGE_KEY")
account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
mongo_user = os.getenv("MONGO_USER")
mongo_password = quote_plus(os.getenv("MONGO_PASSWORD"))
mongo_hosts = os.getenv("MONGO_HOSTS")
container= "raw-data"
print("account_name:", account_name)
print("container:", container)
print("azure_key loaded:", azure_key is not None)


# In[10]:


mongo_uri = (
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_hosts}/"
    f"?ssl=true&replicaSet=atlas-13ali9-shard-0&authSource=admin&appName=Analyticslab"
)

print(mongo_uri.split("@")[1][:120])


# In[8]:


spark.range(5).show()


# ### 4. Load and Merge Data for Azure

# In[16]:


# Load the multi-year OpenData files (2023-2026) using wildcards
open_data_df = spark.read.csv("OpenData_*.csv", header=True, inferSchema=True)

# convert Jupyter Pandas DataFrame to a Spark DataFrame
pec26_spark_df = spark.createDataFrame(filtered_df)

# Verify the schema 
pec26_spark_df.printSchema()


# In[42]:


print("Success! Row count:", open_data_df.count())


# In[17]:


hospital_map = {
    "Bantry General Hospital": "South-West",
    "Beaumont Hospital": "Dublin",
    "Cavan General Hospital": "Border",
    "Connolly Hospital Blanchardstown": "Dublin",
    "Coombe Women and Infants University Hospital": "Dublin",
    "Cork University Hospital": "South-West",
    "Cork University Maternity Hospital": "South-West",
    "Croom Orthopaedic Hospital": "Mid-West",
    "Ennis Hospital": "Mid-West",
    "Galway University Hospitals": "West",
    "Kilcreene Regional Orthopaedic Hospital": "South-East",
    "Letterkenny University Hospital": "Border",
    "Louth County Hospital": "Border",
    "Mallow General Hospital": "South-West",
    "Mater Misericordiae University Hospital": "Dublin",
    "Mayo University Hospital": "West",
    "Mercy University Hospital": "South-West",
    "Midland Regional Hospital Mullingar": "Midlands",
    "Midland Regional Hospital Portlaoise": "Midlands",
    "Midland Regional Hospital Tullamore": "Midlands",
    "Naas General Hospital": "Mid-East",
    "National Maternity Hospital (Holles Street)": "Dublin",
    "National Orthopaedic Hospital Cappagh": "Dublin",
    "Nenagh Hospital": "Mid-West",
    "Our Lady of Lourdes Hospital Drogheda": "Border",
    "Our Lady's Hospital Navan": "Mid-East",
    "Portiuncula University Hospital": "West",
    "Roscommon University Hospital": "West",
    "Rotunda Hospital": "Dublin",
    "Royal Victoria Eye and Ear Hospital": "Dublin",
    "Sligo University Hospital": "Border",
    "South Infirmary Victoria University Hospital": "South-West",
    "St. Columcille's Hospital": "Dublin",
    "St. James's Hospital": "Dublin",
    "St. John's Hospital Limerick": "Mid-West",
    "St. Luke's General Hospital Kilkenny": "South-East",
    "St. Michael's Hospital": "Dublin",
    "St. Vincent's University Hospital": "Dublin",
    "Tallaght University Hospital": "Dublin",
    "Tipperary University Hospital": "Mid-West",
    "University Hospital Kerry": "South-West",
    "University Hospital Limerick": "Mid-West",
    "University Hospital Waterford": "South-East",
    "Wexford General Hospital": "South-East",
    "Children's Health Ireland": "Dublin"
}

# use a mapping expression to add 'NUTS 3 Region' to Open Data
flattened_map = [F.lit(x) for k, v in hospital_map.items() for x in (k, v)]
mapping_expr = F.create_map(flattened_map)
open_data_enriched = open_data_df.withColumn("NUTS 3 Region", mapping_expr[F.col("HospitalName")])


# In[18]:


# Aggregate Hospital Data by Year and County
hospital_agg = open_data_enriched.groupBy("ArchiveDate", "NUTS 3 Region").agg(
    F.sum("0-6 Months").alias("Wait_Short_Term"),
    F.sum("6-12 Months").alias("Wait_Medium_Term")
)

# Extract Year from ArchiveDate
open_data_cleaned = hospital_agg.withColumn(
    "Year", F.year(F.to_date(F.col("ArchiveDate"), "dd/MM/yyyy"))
)

# Merge pec26 and open data
integrated_df = open_data_cleaned.join(pec26_spark_df, on=["Year", "NUTS 3 Region"], how="inner")


# ### 5. Write and Read Raw data to Azure

# In[8]:


spark.conf.set(
    f"fs.azure.account.key.{account_name}.blob.core.windows.net",
    azure_key
)


# In[19]:


output_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/integrated_pec26_hospitals"
integrated_df.write.mode("overwrite").parquet(output_path)


# In[9]:


input_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/integrated_pec26_hospitals"
blob_df = spark.read.parquet(input_path)
blob_df.show(5)
blob_df.printSchema()


# ### 6. Preprocess Data for Mongo Storage

# In[10]:


clean_df = blob_df \
    .withColumnRenamed("Year", "year") \
    .withColumnRenamed("NUTS 3 Region", "region") \
    .withColumnRenamed("ArchiveDate", "archive_date") \
    .withColumnRenamed("Wait_Short_Term", "wait_short_term") \
    .withColumnRenamed("Wait_Medium_Term", "wait_medium_term") \
    .withColumnRenamed("Statistic", "statistic") \
    .withColumnRenamed("Sex", "sex") \
    .withColumnRenamed("Criteria for Projection", "projection_method") \
    .withColumnRenamed("Age", "age") \
    .withColumnRenamed("value", "projected_population")


clean_df = clean_df.withColumn("archive_date", to_date(col("archive_date"), "dd/MM/yyyy"))


# In[9]:


# check null values
clean_df.select([
    F.sum(col(c).isNull().cast("int")).alias(c)
    for c in clean_df.columns
]).show()


# In[11]:


# Trim strings
for c in ["region", "statistic", "sex", "projection_method", "age"]:
    clean_df = clean_df.withColumn(c, trim(col(c)))
# Filter valid values
clean_df = clean_df.filter(
    (col("wait_short_term") >= 0) &
    (col("wait_medium_term") >= 0) &
    (col("projected_population") >= 0)
)

# Create total waiting list
clean_df = clean_df.withColumn("wait_total", col("wait_short_term") + col("wait_medium_term"))
clean_df.show(5)
clean_df.printSchema()


# #### 7. Connect to MongoDB 

# In[12]:


database_name = "diss_db"
collection_name = "clean_hospital_data"

clean_df.write \
    .format("mongodb") \
    .mode("append") \
    .option("connection.uri", mongo_uri) \
    .option("database", database_name) \
    .option("collection", collection_name) \
    .save()


# #### 8. Read data from MongoDB

# In[11]:


analysis_df = spark.read \
    .format("mongodb") \
    .option("connection.uri", mongo_uri) \
    .option("database", "diss_db") \
    .option("collection", "clean_hospital_data") \
    .load()

analysis_df.printSchema()
analysis_df.show(5, truncate=False)
print("Row count:", analysis_df.count())

#### Analysis 1: K-Means Clustering of Regions based on Population and Wait Times
# Prepare the features
# VectorAssembler to combine the columns into a single 'features' vector
assembler = VectorAssembler(
    inputCols=["projected_population", "wait_total"], 
    outputCol="features"
)
cluster_data = assembler.transform(analysis_df)

# Scale the data so that population and wait times are on the same scale for clustering
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
scaler_model = scaler.fit(cluster_data)
scaled_df = scaler_model.transform(cluster_data)

# Training the K-Means Model
# We will create 3 clusters: e.g., 'Under-resourced', 'At-capacity', and 'Stable'
kmeans = KMeans(featuresCol="scaledFeatures", k=3, seed=1)
model = kmeans.fit(scaled_df)

# Applying the model (Predictions)
predictions = model.transform(scaled_df)

# Summary Statistics of Clusters
cluster_summary = predictions.groupBy("prediction").agg(
    F.avg("projected_population").alias("avg_pop"),
    F.avg("wait_total").alias("avg_wait"),
    F.count("*").alias("count")
).orderBy("avg_pop")

print("Cluster Characteristics:")
cluster_summary.show()

# Visualization
# Convert  to Pandas for plotting
pdf_clusters = predictions.select("projected_population", "wait_total", "prediction").toPandas()

plt.figure(figsize=(10, 6))
scatter = plt.scatter(
    pdf_clusters["projected_population"], 
    pdf_clusters["wait_total"], 
    c=pdf_clusters["prediction"], 
    cmap='viridis',
    alpha=0.6
)
plt.colorbar(scatter, label='Cluster ID')
plt.title("K-Means Clustering: Population Projection vs. Total Wait List")
plt.xlabel("Projected Population")
plt.ylabel("Total Waiting")
plt.grid(True, linestyle='--', alpha=0.7)
plt.savefig(os.path.join(output_dir, "kmeans_cluster_analysis.png"))

print("K-Means analysis complete. Plot saved as kmeans_cluster_analysis.png")

# ### Analysis 2: Time Trend Analysis

# In[12]:


time_trend_df = analysis_df.groupBy("year").agg(
    F.sum("projected_population").alias("projected_population"),
    F.sum("wait_short_term").alias("wait_short_term"),
    F.sum("wait_medium_term").alias("wait_medium_term"),
    F.sum("wait_total").alias("wait_total")
).orderBy("year")

time_trend_df.show(truncate=False)


# In[14]:


# Year over Year Growth

w = Window.orderBy("year")

time_trend_growth_df = time_trend_df.withColumn(
    "prev_population", F.lag("projected_population").over(w)
).withColumn(
    "prev_wait_total", F.lag("wait_total").over(w)
).withColumn(
    "population_yoy_pct",
    ((F.col("projected_population") - F.col("prev_population")) / F.col("prev_population")) * 100
).withColumn(
    "wait_total_yoy_pct",
    ((F.col("wait_total") - F.col("prev_wait_total")) / F.col("prev_wait_total")) * 100
)

time_trend_growth_df.show(truncate=False)


# In[16]:


#Region growth overtime 
region_year_trend = analysis_df.groupBy("region", "year").agg(
    F.sum("projected_population").alias("projected_population"),
    F.sum("wait_total").alias("wait_total")
).orderBy("region", "year")

region_year_trend.show(truncate=False)


# In[19]:


import matplotlib.pyplot as plt


pdf = time_trend_df.toPandas()

plt.figure(figsize=(10,5))
plt.plot(pdf["year"], pdf["projected_population"], marker='o', label="Projected Population")
plt.plot(pdf["year"], pdf["wait_total"], marker='o', label="Wait Total")
plt.legend()
plt.title("Yearly Trend of Projected Population and Wait Total")
plt.xlabel("Year")
plt.ylabel("Value")
plt.grid(True)
plt.savefig(os.path.join(output_dir, "trend_by_year.png"))


# ### Analysis 3: Regional Segmentation Analysis

# In[29]:


#Region Analysis
region_analysis = analysis_df.groupBy("region").agg(
    F.sum("projected_population").alias("total_projected_population"),
    F.sum("wait_total").alias("total_wait_total"),
    F.avg("wait_total").alias("avg_wait_total")
).orderBy(F.desc("total_wait_total"))
region_pd = region_analysis.toPandas()

# Waiting burden by region (Waiting Burden = wait_total / projected_population)
region_burden = analysis_df.groupBy("region").agg(
    F.sum("projected_population").alias("total_projected_population"),
    F.sum("wait_total").alias("total_wait_total")
).withColumn(
    "wait_burden_ratio",
    F.when(
        F.col("total_projected_population") != 0,
        F.col("total_wait_total") / F.col("total_projected_population")
    )
).orderBy(F.desc("wait_burden_ratio"))

region_burden_pd = region_burden.toPandas()


# In[24]:


import seaborn as sns


# In[30]:


# Total wait by Region Barcharts
plt.figure(figsize=(14, 6))
sns.barplot(data=region_pd, x="region", y="total_wait_total", palette="viridis")
plt.title("Total Wait by Region")
plt.xlabel("Region")
plt.ylabel("Total Wait")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "total_wait_region.png"))

# Wait burden by Region
plt.figure(figsize=(14, 6))
sns.barplot(data=region_burden_pd, x="region", y="wait_burden_ratio", palette="magma")
plt.title("Wait Burden Ratio by Region")
plt.xlabel("Region")
plt.ylabel("Wait Burden Ratio")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "wait_burden_region.png"))


# In[32]:


#Short-term vs medium-term wait by region

region_wait_components = analysis_df.groupBy("region").agg(
    F.sum("wait_short_term").alias("wait_short_term"),
    F.sum("wait_medium_term").alias("wait_medium_term")
)

region_wait_components_pd = region_wait_components.toPandas()
region_wait_components_pd = region_wait_components_pd.sort_values("wait_short_term", ascending=False)

region_wait_components_pd.set_index("region")[["wait_short_term", "wait_medium_term"]].plot(
    kind="bar",
    stacked=True,
    figsize=(14, 6),
    colormap="Set2"
)

plt.title("Short-term and Medium-term Wait by Region")
plt.xlabel("Region")
plt.ylabel("Wait Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "term_wait_region.png"))


# In[38]:


# Region Trend Over Time
region_year_trend = analysis_df.groupBy("region", "year").agg(
    F.sum("projected_population").alias("projected_population"),
    F.sum("wait_short_term").alias("wait_short_term"),
    F.sum("wait_medium_term").alias("wait_medium_term"),
    F.sum("wait_total").alias("wait_total")
).orderBy("region", "year")

region_year_pd = region_year_trend.toPandas()

plt.figure(figsize=(14, 7))
sns.lineplot(data=region_year_pd, x="year", y="wait_total", hue="region", marker="o")
plt.title("Regional Wait Total Trend Over Time")
plt.xlabel("Year")
plt.ylabel("Wait Total")
plt.legend(title="Region", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "region_wait_trend.png"))


# In[36]:


# Region Burden Trend Over Time
region_year_burden = analysis_df.groupBy("region", "year").agg(
    F.sum("projected_population").alias("projected_population"),
    F.sum("wait_total").alias("wait_total")
).withColumn(
    "wait_burden_ratio",
    when(col("projected_population") != 0,
         col("wait_total") / col("projected_population"))
).orderBy("region", "year")

region_year_burden_pd = region_year_burden.toPandas()

plt.figure(figsize=(14, 7))
sns.lineplot(data=region_year_burden_pd, x="year", y="wait_burden_ratio", hue="region", marker="o")
plt.title("Regional Wait Burden Ratio Trend Over Time")
plt.xlabel("Year")
plt.ylabel("Wait Burden Ratio")
plt.legend(title="Region", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "regional_waitburden_trend.png"))


# ### Analysis 4: Population vs Wait Demand Relationship  by Region and Year 

# In[40]:


corr_total = analysis_df.stat.corr("projected_population", "wait_total")
corr_short = analysis_df.stat.corr("projected_population", "wait_short_term")
corr_medium = analysis_df.stat.corr("projected_population", "wait_medium_term")

print("Correlation with wait_total:", corr_total)
print("Correlation with wait_short_term:", corr_short)
print("Correlation with wait_medium_term:", corr_medium)


# In[39]:


region_year_analysis = analysis_df.groupBy("region", "year").agg(
    F.sum("projected_population").alias("projected_population"),
    F.sum("wait_total").alias("wait_total"),
    F.sum("wait_short_term").alias("wait_short_term"),
    F.sum("wait_medium_term").alias("wait_medium_term")
)

print("Region-Year Corr(projected_population, wait_total):",
      region_year_analysis.stat.corr("projected_population", "wait_total"))

pdf_rel = region_year_analysis.toPandas()

plt.figure(figsize=(8,6))
plt.scatter(pdf_rel["projected_population"], pdf_rel["wait_total"])
plt.title("Projected Population vs Wait Total")
plt.xlabel("Projected Population")
plt.ylabel("Wait Total")
plt.grid(True)
plt.savefig(os.path.join(output_dir, "proj_pop_wait.png"))


# In[ ]:




