"""
@author Niraj
The Objective of the project is to perform Covid 19 Data Analysis and visualize the Results.
1> females affected by Covid visualize it
2> Getting which city is most affected
3> Recovery rate of each city or state
4> Age group mostly affected
5> Age group mostly dead

Library Used:-
1> pyspark Version 3.0.1
    For distributed Computation

2> Seaborn And Matplotlib
    For Visualization

"""
import os
import pyspark
import sys
import seaborn as sns
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import expr

from lib.logger import Log4j

class CovidDataAnalysis:
    """
       A class to represent a CovidData.

       ...

       Attributes
       ----------
       covid_df : DataFrame
           A spark dataframe that stores the covid 19 details of India.

       Methods
       -------
       1> get_reports
            - To visualize the covid 19 reports

       2> gender_wise_visualisation
            - To plot gender wise covid 19 affected rate

       3> city_wise_covid_cases
            - To plot the top 10 most affected cities

       4> state_wise_recovery_rate
            - To plot the top 10 states having high recovery rate

       5> age_wise_affected
            - To plot the top 10 age group mostly affected

        6> age_group_death_rate
            - To plot the top 10 age group having high death rate

       """

    # Taking Input
    def __init__(self, spark_session_obj, path):
        try:
            spark_session_obj.sparkContext.setLogLevel("ERROR")
            self.covid_df = spark_session_obj.\
                read.\
                csv(path, header=True, sep=",", inferSchema=True)
            self.logger = Log4j(spark_session_obj)
        except FileNotFoundError as ex:
            print("Path Is Wrong", ex.with_traceback())

    # Visualizing Reports
    def get_reports(self):
        try:
            choice = int(input("Enter Your Choice\n1.Gender Wise Infection Report\n2.Top Infected Cities\n3.State "
                               "Wise Recovery Rate\n4.Age Wise Infected People\n5.Age Wise Death Rate\nElse to "
                               "Stop\n"))
            if choice == 1:
                self.gender_wise_visualisation()

            elif choice == 2:
                self.city_wise_covid_cases()
            elif choice == 3:
                self.state_wise_recovery_rate()
            elif choice == 4:
                self.age_wise_affected()
            elif choice == 5:
                self.age_group_death_rate()
            else:
                print("Invalid Choice")
                return
            self.get_reports()
        except ValueError as _:
            print("Enter Numbers Only")
            self.get_reports()
        except Exception as _:
            raise Exception("Unexpected Errors Occurred")

    # females affected by Covid visualize it
    def gender_wise_visualisation(self):
        try:
            self.logger.info("Gender Wise Covid Infection Rate")
            gender_count_df = self.covid_df.groupBy("gender").count()
            gender_count_df.show(5)
            sns.barplot("gender", "count", data=gender_count_df.toPandas())
            plt.xlabel("Gender")
            plt.ylabel("Number of Peoples Affected")
            plt.title("Gender Wise Covid Infection Rate")
            plt.show(block=False)
            plt.pause(10)
            plt.close()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Getting which city is most affected
    def city_wise_covid_cases(self):
        try:
            self.logger.info("City Wise Covid Infection Rate")
            city_wise_count_df = self.covid_df.groupBy("detectedCity").count().orderBy("count", ascending=False)
            city_wise_count_df.show(5)
            top10_cities = city_wise_count_df.toPandas()["detectedCity"].values[:10]
            top10_cities_count = city_wise_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_cities_count, top10_cities)
            plt.xlabel("Detected City")
            plt.ylabel("Number of Persons Affected")
            plt.title("Top 10 Cities Mostly Affected")
            plt.show(block=False)
            plt.pause(10)
            plt.close()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Recovery rate of each city or state
    def state_wise_recovery_rate(self):
        try:
            self.logger.info("State Wise Covid Infection Rate")
            state_wise_recoveryrate_count_df = self.covid_df.where(expr("lower(currentstatus) = 'recovered' ")).groupBy(
                "detectedstate").count().orderBy("count", ascending=False)
            state_wise_recoveryrate_count_df.show(5)
            top10_states = state_wise_recoveryrate_count_df.toPandas()["detectedstate"].values[:10]
            top10_states_count = state_wise_recoveryrate_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_states, top10_states_count)
            plt.xlabel("Detected States")
            plt.ylabel("Recovery Rate City Wise")
            plt.title("Top 10 Cities Having High Recovery Rate")
            plt.show(block=False)
            plt.pause(10)
            plt.close()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Age group mostly affected
    def age_wise_affected(self):
        try:
            self.logger.info("Age Wise Covid Infection Rate")
            age_wise_affected_count_df = self.covid_df.groupBy("agebracket").count().orderBy("count", ascending=False)
            age_wise_affected_count_df.show(5)
            top10_ages = age_wise_affected_count_df.toPandas()["agebracket"].values[:10]
            top10_ages_count = age_wise_affected_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_ages, top10_ages_count)
            plt.xlabel("Age")
            plt.ylabel("Number of Persons Affected")
            plt.title("Top 10 Age Having High Infection Rate")
            plt.show(block=False)
            plt.pause(10)
            plt.close()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Age group mostly dead
    def age_group_death_rate(self):
        try:
            self.logger.info("Age Wise Covid Death Rate")
            age_wise_dead_count_df = self.covid_df.where(expr("lower(currentstatus) == 'deceased' ")).groupBy(
                "agebracket").count().orderBy("count", ascending=False)
            age_wise_dead_count_df.show()
            top10_ages = age_wise_dead_count_df.toPandas()["agebracket"].values[:10]
            top10_ages_count = age_wise_dead_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_ages, top10_ages_count)
            plt.xlabel("Age Groups")
            plt.ylabel("Number of Person Died")
            plt.title("Top 10 Age Having High Dead Rate")
            plt.show(block=False)
            plt.pause(10)
            plt.close()

        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")


# Main Function
if __name__ == '__main__':
    sparkSessionObj = SparkSession.builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.0.0,com.amazonaws:aws-java-sdk:1.7.4") \
        .appName("Covid19 Visualization").master("local[*]").getOrCreate()
    sc = sparkSessionObj.sparkContext
    # sparkSessionObj._jsc.hadoopConfiguration().set("fs.s3a.access.key",os.environ["AWS_ACCESS_KEY"])
    # sparkSessionObj._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    # sparkSessionObj._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # sparkSessionObj._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # sparkSessionObj._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
    #                                      "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    # sparkSessionObj._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3a.ap-south-1.amazonaws.com")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    print(sys.argv[1])
    covidDataAnalysisObj = CovidDataAnalysis(sparkSessionObj, sys.argv[1])
    covidDataAnalysisObj.get_reports()