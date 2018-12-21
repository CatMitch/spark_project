
# coding: utf-8
To ensure the correct calls are made, I set the current working directory to be my Spark final submission folder. Please ensure this is the correct location: it should be the submission folder of my Spark project
# In[ ]:


directory = '/Users/catrionamitchison/Documents/Ensai/Courses/Spark/Spark_final_sub_CM'


# In[ ]:


import os

subdirectory = directory + '/gettingGithubFiles'
os.chdir(subdirectory)
cwd = os.getcwd()
cwd


# In[ ]:


filepath = './files'
filerights = '705'


# In[ ]:


get_ipython().system('./read_function.sh jupyter $filepath $filerights')


# In[ ]:


import zipfile

# create a function to unzip all the files downloaded from Github through the shell command

def extract_files(directory, extension = ".zip"):
    dir_name = directory
    extension = extension
    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = dir_name + "/" + item # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(dir_name) # extract file to dir
            zip_ref.close() # close file
            os.remove(file_name) # delete zipped filee


# In[ ]:


extract_files(subdirectory + '/files') # extract the files into the directory defined above


# In[ ]:


import pyspark
import pyspark.sql.functions as F

We now initialise our Spark environment, ensuring that our spark home is where our Spark has been exploded. Again, for reproducibility, please ensure this is correct.
# In[ ]:


os.environ['SPARK_HOME'] = '/Users/catrionamitchison/spark-2.4.0-bin-hadoop2.7'


# In[ ]:


sc = pyspark.SparkContext()


# In[ ]:


sc


# In[ ]:


sql_sc = pyspark.SQLContext(sc)

Next, we change the column names to better understand what we are dealing with. We only rename the unique columns, we will do the others later.
# In[ ]:


def convert_prices_to_ddf(file):
    res_ddf = (sql_sc
               .read
               .option('header', 'false')
               .option('sep', ';')
               .option('inferSchema', 'true')
               .csv(file)
               .selectExpr(
                 '_c0 as station_id',
                 '_c1 as post_code',
                 '_c2 as station_type',
                 '_c3 as latitude',
                 '_c4 as longitude',
                 '_c5 as price_date',
                 '_c6 as gas_type',
                 '_c7 as gas_label',
                 '_c8 / 1000 as price')
              )
    return res_ddf


# In[ ]:


def convert_other_to_ddf(file):
    red_ddf = (sql_sc
               .read
               .option('header', 'false')
               .option('sep', '|')
               .option('inferSchema', 'true')
               .csv(file)
              )
    return red_ddf


# In[ ]:


from os import listdir
import glob

pricefiles = sorted(glob.glob(subdirectory + '/files/Prix*.csv'))
pricefiles


# In[ ]:


for filename in pricefiles:
    if filename.endswith('2014.csv'):
        temp_df = convert_prices_to_ddf(str(filename))
        prices_ddf = temp_df
    else:
        temp_df = convert_prices_to_ddf(filename)
        prices_ddf = prices_ddf.union(temp_df)


# In[ ]:


prices_ddf.cache()


# In[ ]:


stations_ddf = ((convert_other_to_ddf(subdirectory + '/files/Stations2017.csv'))
                .withColumnRenamed("_c0", "station_id")
                .withColumnRenamed("_c1", "post_code")
                .withColumnRenamed("_c2", "station_type")
                .withColumnRenamed("_c3", "latitude")
                .withColumnRenamed("_c4", "longitude")
                .withColumnRenamed("_c5", "address")
                .withColumnRenamed("_c6", "city")
               ).cache()
                
services_ddf = ((convert_other_to_ddf(subdirectory + '/files/Services2017.csv'))
                .withColumnRenamed("_c0", "station_id")
                .withColumnRenamed("_c1", "post_code")
                .withColumnRenamed("_c2", "station_type")
                .withColumnRenamed("_c3", "latitude")
                .withColumnRenamed("_c4", "longitude")
                .withColumnRenamed("_c5", "services")
               ).cache()


# In[ ]:


# check everything is as it should be
prices_ddf.show()
stations_ddf.show()
services_ddf.show()

Now, we merge the dataframes based on the five columns they have in common. We do a full merge, keeping all rows from every dataframe.
# In[ ]:


all_data_ddf = (((prices_ddf
                .join(stations_ddf, on = ["station_id", "post_code", "station_type", "latitude", "longitude"]))
                .join(services_ddf, on = ["station_id", "post_code", "station_type", "latitude", "longitude"])
               ).distinct()
                .dropna()
               ).cache()
all_data_ddf.show()

We have already added the week index during the creation of our dataframes, so we will now split the date column into year, month, week and day and remove any observaions where the date is missing
# In[ ]:


all_correct_ddf = (all_data_ddf
                .withColumn("year", F.year(F.col("price_date")))
                .withColumn("month", F.month(F.col("price_date"))) # separating the month
                .withColumn("day", F.dayofyear(F.col("price_date"))) # extracts the day of the year
                .withColumn("latitude", F.col("latitude")/100000) 
                .withColumn("longitude", F.col("longitude")/100000) # get the long and lat to the correct scale
                .withColumn("week_index", (F.weekofyear(F.col("price_date"))+((F.year("price_date") - 2014)*52)))
               )
all_correct_ddf.show()


# In[ ]:


all_correct_ddf.createOrReplaceTempView("all_data") # save dataframe as a table for use with SQL


# In[ ]:


# create a new table summarising the average price of different types of gas over the course of a day in each station
av_daily_station = (sql_sc.sql(
    'SELECT station_id, gas_label, year, day, AVG(price) AS average_price_station '
    'FROM all_data GROUP BY year, day, gas_label, station_id '
    'ORDER BY year, day')
                  )

# create another table giving the average again but not for France as a whole, not per station
av_daily_france = (sql_sc.sql(
    'SELECT gas_label, year, day, AVG(price) AS average_price_france '
    'FROM all_data GROUP BY year, day, gas_label '
    'ORDER BY year, day')
                  )

av_daily_station.show(15) # display the tables for comparison
av_daily_france.show(15)

Next, we combine our two tables with our averaged results with the original dataframe. Then we will manipulate the columns to create a further column with the Price Index.
# In[ ]:


all_with_averages = (all_correct_ddf
        .join(av_daily_france, ['gas_label', 'year', 'day'], "left")
        .join(av_daily_station, ['station_id','gas_label', 'year', 'day'], "left")
       )

all_correct_ddf.printSchema() # here we can see the columns have been added correctly

Here, we add the Price Index, by first manipulating the average price per station and average price for all of France columns.
# In[ ]:


all_ddf = (all_with_averages
           .withColumn("price_index", # add the price index column
                       100*((F.col("average_price_station")-F.col("average_price_france"))
                            /F.col("average_price_france")+1)
                      ))          
all_ddf.show(5)

Now, we calculate the average price over the week index calculated previously.
# In[ ]:


week_summary = (all_ddf
                .groupBy('gas_label', 'week_index')
                .agg(F.mean('price').alias('week_price'))
                .dropna()
)
week_summary.show(5)


# In[ ]:


pandas_df = week_summary.toPandas() # convert to a pandas dataframe for use in plotting

Having successfully completed the data preparation and manipulation, we now convert our dataframe into a pandas dataframe, before plotting it with matplotlib
# In[ ]:


import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


grid = sns.FacetGrid(pandas_df, 
                     hue = "gas_label",
                     height = 5, 
                     aspect = 1.5
                    )
grid.map(sns.lineplot, "week_index", "week_price")
grid.add_legend()
plt.show()

To display this information as a heatmap, we must retrieve the gas name and the average daily price index of each gas type over all stations from our main all_data_ddf.
Before, we plotted the average PRICE over the week index, however this time we will plot the average PRICE INDEX over the week.
# In[ ]:


heatmap_data = (all_ddf
                .groupBy(['gas_label','week_index'])
                .agg(F.mean('price_index').alias('av_gas_price'))
                .dropna()
               )


# An alternative way to perform the same command:
# heatmap_data = (sql_sc.sql("SELECT gas_label, day, AVG(price_index) AS av_gas_price '
#                            'FROM all_data GROUP BY gas_name, day"))

heatmap_data.show(5)


# In[ ]:


pdheatmap = heatmap_data.toPandas() # convert this information to Pandas dataframe for plotting

To be readable to the Seaborn heatmap function, the data must be in the form of a pivot table. 
# In[ ]:


result = pdheatmap.pivot(index = 'week_index', columns = 'gas_label', values = 'av_gas_price')


# In[ ]:


plt.subplots(figsize=(12,7))
htmp = sns.heatmap(result, 
                   fmt = "g",
                   cmap = 'viridis'
                  )

Having completed ths visualisation and manipulation part, we will now move onto modelling. Due to our estimating a continuous variable, I have chosen to implement a linear regression, using the pyspark library for machine learning. First, I add columns with 1-4 lags of the price to my dataframe. Then I create a pipeline which will carry out the column manipulations so the data is in the correct format, and the regression function itself.To build my model, I chose gas type 1, corresponding to Gazole, and station ID: 28190001.
# In[ ]:


from pyspark.sql.window import Window
import pyspark.sql.functions as func

# average daily price has been stored in the price index, so we drop any multiple daily entries
model_data = (all_ddf
              .filter(F.col('station_id').isin(['28190001']))
              .filter(F.col('gas_type').isin(['1']))
              .dropDuplicates(['station_id', 'gas_label', 'year', 'week_index', 'day'])
              .drop('station_id','post_code','latitude','longitude','average_price_france', 
                    'address','services','city','price_index', 'pop', 'gas_label','week',
                    'gas_type', 'station_type','year','month','price_date', 'average_price_station')
              .na.drop()
              )

model_data.show(5)


# In[ ]:


model_data.cache() # cache the data to save computing time


# In[ ]:


my_window = Window.partitionBy().orderBy("week_index","day")


# In[ ]:


useful_lag = (
    model_data
    .withColumn('price_lag1',
                func.lag(model_data['price'],1).over(my_window))
    .withColumn('price_lag2',
                func.lag(model_data['price'],2).over(my_window))
    .withColumn('price_lag3',
                func.lag(model_data['price'],3).over(my_window))
    .withColumn('price_lag4',
                func.lag(model_data['price'],4).over(my_window))
    .na.drop(
        subset = ["price_lag1", "price_lag2", "price_lag3", "price_lag4"])
)
useful_lag.printSchema()


# In[ ]:


useful_lag.show(5) # let's see if the figures seem correct


# In[ ]:


from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# set the parameters of my linear regression
lr = LinearRegression(maxIter = 10,
                      labelCol = "price")

# creating features column
assembler = VectorAssembler(
    inputCols = ["price_lag1","price_lag2","price_lag3","price_lag4"],
    outputCol = "features")

Here, we define our test and training set, and create our pipeline including the steps we have defined above. This will enable us to quickly and easily (not including model fitting time) fit our model to our test data when we want to. I chose a 75%, 25% training, testing split.
# In[ ]:


train, test = useful_lag.randomSplit([0.75, 0.25], seed=12345)


# In[ ]:


pipeline = Pipeline(stages = [assembler, lr]) 


# In[ ]:


# Fit the model
lrModel = pipeline.fit(train)


# In[ ]:


# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.stages[-1].coefficients))
print("Intercept: %s" % str(lrModel.stages[-1].intercept))

# Summarize the model over the training data and print out some metrics
trainingSummary = lrModel.stages[-1].summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))

print("RMSE: %f" % trainingSummary.rootMeanSquaredError) # RMSE
print("r2: %f" % trainingSummary.r2) # R squared


# In[ ]:


train.describe().show() # get some general info


# In[ ]:


lr_predictions = lrModel.transform(test) # predict values for the test set


# In[ ]:


lr_evaluator = RegressionEvaluator(predictionCol = "prediction", labelCol = "price", metricName = "r2")
lr_evaluator.evaluate(predictions)
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions)) # test data R squared value

With an R squared value of 0.853292, we are satisfied that 85% of the variation in the data is explained by the model.
# In[ ]:


lr_predictions.show() # visualise our dataframe to see what we need ot keep for our dispersion plot


# In[ ]:


disp_data = (lr_predictions
             .select("price", "prediction")
            )
disp_data.show()


# In[ ]:


disp_plot_df = disp_data.toPandas()


# In[ ]:


plt.figure(figsize=(12,7))
plt.scatter(disp_plot_df['price'], disp_plot_df['prediction'], alpha=0.7)
plt.title("Dispersion plot of predicted prices vs true prices")
plt.xlabel("real price")
plt.ylabel("predicted price")
plt.show()


# In[ ]:


sc.stop() # ending our Spark session

