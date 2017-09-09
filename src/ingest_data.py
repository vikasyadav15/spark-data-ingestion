""" This program will ingest data from mysql database to hdfs using spark """

import json
# pylint: disable-msg=E0401
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

# pylint: disable-msg=C0103
with open('../config.json', 'r') as configFile:
    config = json.load(configFile)

def readDataFromDB():
    "This method connect to mysql server to fetch the credit card data"
    creditCardData = sqlContext.read.format("jdbc").options(
        url=config["DATABASE"]["url"],
        driver=config["DATABASE"]["driver"],
        user=config["DATABASE"]["user"],
        password=config["DATABASE"]["password"],
        dbtable=config["DATABASE"]["dbtable"]
    ).load()
    return creditCardData

def readDataFromFile(fileName):
    "This method read data from given filename"
    data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(fileName)
    return data

def writeDataToMySQL(data):
    "This method writes data to mysql db"
    data.write.format("jdbc").options(
        url=config["DATABASE"]["url"],
        driver=config["DATABASE"]["driver"],
        user=config["DATABASE"]["user"],
        password=config["DATABASE"]["password"],
        dbtable=config["DATABASE"]["dbtable"]
    ).save()
    print "Data saved into MySQL database successfully"

def writeDataToHive(data):
    "This method writes data to hive"
    data.write.format("orc").saveAsTable("creditCard")

def showHiveDataDetail():
    "this method print hive database details"
    hiveContext.sql("show tables")
    count = hiveContext.sql("SELECT * FROM employees").count()
    print count

def loadFileDataToMySQL():
    "This method calls read csv file data and store it into mysql db"
    data = readDataFromFile(config["FILE_PATH"])
    writeDataToMySQL(data)

def ingestData():
    "This method ingest the data form mysql to hive using spark"
    creditCardData = readDataFromDB()
    writeDataToHive(creditCardData)

# pylint: disable-msg=W0621
def main():
    "This method loads data from csv file to mysql then mysql to hive using spark"
    loadFileDataToMySQL()
    ingestData()
    print "Successfully data ingested form mysql to hive"

if __name__ == "__main__":
    #Spark Configuration
    # pylint: disable-msg=C0103
    conf = SparkConf().setAppName(config["APP_NAME"])
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)
    #Execute main function
    main()
