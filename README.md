s3-file processing
=====================

This project covers the following use cases

1. Downloads the Zip and extracts csv to the local file system.
2. Scan the csv for the search string and creates an intermediate csv
3. Upload the file to S3 bucket

To do 
1. The intermediate file has to converted to parquet file before uploading

Clarification or Notes

The CSV downloaded from S3 must be standadised.
 It is recommended practise to have header to the csv for proper parquet file format.
 
 Improvisations
 1. Terminal CSV's created with the _FilteredFile can be improvised to have json which may ease to convert to parquet file.
 2. Uploading the terminal csv should be refactored.
 3. Reading the CSV uses brute force approach, could be refactored
 4. Need to write more tests
 
 Design Approach on creating Parquet file in java
 1. Added necessary dependencies in maven to do this activity
 2.Determine the path of the csv and the output paraquet files 
 3. Determine the Schema for each csv have separate schema definition.
 2. Read from the _filtered.csv and then map the data wrt to the schema.
 3. create a ParquetWriter and write the data to it using the Schema file
 
 Setup 
1. Please replace the proper credentials in the application.properties before running the project