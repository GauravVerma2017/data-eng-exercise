# Data Engineer: Coding Exercise

This is a coding exercise to help us assess candidates looking to join the Data Science & Engineering team at Condé Nast.

The test is intended to be completed at home, and we expect you to spend no more than 1-2 hours on it.
If you do not have time to fully complete the exercise or you get stuck, that's fine and not entirely unexpected, just send us as much as you have done. 
We look forward to talking about your experiences.

We understand that your time is valuable, and appreciate any time that you contribute towards building a strong team here.
If you really cannot spare the time, you may want to take a look at Option 2.

## Option 1

Implement an Airflow ([Apache Airflow](https://airflow.apache.org/)) DAG to process data from two separate sources and merge it into a single output file.  The DAG should run on a daily basis.  For the purpose of the exercise, both input and output can use the local file system.

Folders `input_source_1/` and `input_source_2/` will contain the input data. 
You can assume that a new file will appear daily in each of those folders, with the filename specifying the date of the data it contains. 
Source 1 data is expected to arrive between 1-3am the following day, in JSON format, while Source 2 data is expected to arrive between 4-5am, in CSV format (e.g. data_20210125.json will appear in the folder on January 26th between 1am and 3am, and engagement_20210125.csv will appear on January 26th between 4am and 5am).
See the sample files provided to familiarise yourself with their format and structure.

The DAG should write to a CSV file the following: date, post id, shares, comments, and likes. All other fields should be ignored.  

Although the sample files are small, your program should be designed such that it could be used in the future to merge large files.

It is totally permitted to make use of any sample code, libraries or other resources available on the internet. Directly copying someone else's answer to this exact question is not advisable.

# Solution Delivery

### pre-requisite setup
1. Setup Spark in local machine.
2. setup airflow in local machine.
3. copy content of dags folder under $AIRFLOW_HOME location.

### Solution Design
1. Written a dag with 3 tasks [2 file sensors and 1 pyspark task to do transformation]
2. Dag will be triggered at 4 AM daily. File Sensor tasks will cover poke for files from source_1 and source_2.
3. Based on SLA we can setup poking interval and setup alerting for on_retry, on_failure or on_sla.
4. Once both files are available, next downstream task is pyspark job that will take run_date as input_argument.
5. Pyspark job will read both files and pull data as per requirement and write in a output location. we enable logging inside pyspark job.
