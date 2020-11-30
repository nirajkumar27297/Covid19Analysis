import pyspark
import sys
import seaborn as sns
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import expr


class CovidDataAnalysis:
    # Taking Input
    def __init__(self, spark_session_obj, path):
        try:
            spark_session_obj.sparkContext.setLogLevel("ERROR")
            self.covid_df = spark_session_obj.read.csv(path, header=True, sep=",", inferSchema=True)
        except FileNotFoundError as ex:
            print("Path Is Wrong", ex.with_traceback())

    # VisualizingReports
    def getReports(self):
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
            self.getReports()
        except ValueError as _:
            print("Enter Numbers Only")
            self.getReports()
        except Exception as _:
            raise Exception("Unexpected Errors Occurred")

    # females affected by Covid visualize it
    def gender_wise_visualisation(self):
        try:
            gender_count_df = self.covid_df.groupBy("gender").count()
            gender_count_df.show(5)
            sns.barplot("gender", "count", data=gender_count_df.toPandas())
            plt.xlabel("Gender")
            plt.ylabel("Count")
            plt.title("Count of Gender")
            plt.show()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Getting which city is most affected
    def city_wise_covid_cases(self):
        try:
            city_wise_count_df = self.covid_df.groupBy("detectedCity").count().orderBy("count", ascending=False)
            city_wise_count_df.show(5)
            top10_cities = city_wise_count_df.toPandas()["detectedCity"].values[:10]
            top10_cities_count = city_wise_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_cities_count, top10_cities)
            plt.xlabel("DetectedCity")
            plt.ylabel("Count")
            plt.title("Top 10 Cities Mostly Affected")
            plt.show()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Recovery rate of each city or state
    def state_wise_recovery_rate(self):
        try:
            state_wise_recoveryrate_count_df = self.covid_df.where(expr("lower(currentstatus) = 'recovered' ")).groupBy(
                "detectedstate").count().orderBy("count", ascending=False)
            state_wise_recoveryrate_count_df.show(5)
            top10_states = state_wise_recoveryrate_count_df.toPandas()["detectedstate"].values[:10]
            top10_states_count = state_wise_recoveryrate_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_states, top10_states_count)
            plt.xlabel("States")
            plt.ylabel("Recovery Rate Count")
            plt.title("Top 10 Cities Having High Recovery Rate")
            plt.show()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Age group mostly affected
    def age_wise_affected(self):
        try:
            age_wise_affected_count_df = self.covid_df.groupBy("agebracket").count().orderBy("count", ascending=False)
            age_wise_affected_count_df.show(5)
            top10_ages = age_wise_affected_count_df.toPandas()["agebracket"].values[:10]
            top10_ages_count = age_wise_affected_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_ages, top10_ages_count)
            plt.xlabel("Age")
            plt.ylabel("Persons Affected Count")
            plt.title("Top 10 Age Having High Infection Rate")
            plt.show()
        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")

    # Age group mostly dead
    def age_group_death_rate(self):
        try:
            age_wise_dead_count_df = self.covid_df.where(expr("lower(currentstatus) == 'deceased' ")).groupBy(
                "agebracket").count().orderBy("count", ascending=False)
            age_wise_dead_count_df.show()
            top10_ages = age_wise_dead_count_df.toPandas()["agebracket"].values[:10]
            top10_ages_count = age_wise_dead_count_df.toPandas()["count"].values[:10]
            sns.barplot(top10_ages, top10_ages_count)
            plt.xlabel("Age")
            plt.ylabel("Persons Dead Count")
            plt.title("Top 10 Age Having High Dead Rate")
            plt.show()

        except pyspark.sql.utils.AnalysisException as ex:
            print(ex.with_traceback)
            raise Exception("SQl Analysis Exception Occurred\n")

        except Exception as ex:
            print(ex.with_traceback)
            raise Exception("UnExpected Error Occurred\n")


if __name__ == '__main__':
    sparkSessionObj = SparkSession.builder.appName("Covid19 Visualization").master("local[*]").getOrCreate()
    covidDataAnalysisObj = CovidDataAnalysis(sparkSessionObj, sys.argv[1])
    covidDataAnalysisObj.getReports()
