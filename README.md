# Peta-scale-architecture-data-engineering

# The use case - details
We have 1PB of data of weather sensors.
We need that data to make accessible to analytics purposes.
 Data does not grow anymore - 1PB and that's it. :)
 One table that contains 5 columns.
o Timestamp, geo_id, sensor_id, value, sensor_type
 The data located on storage server on prem data center.
 All the data is in CSV format
 All the data is compressed in GZIP (200TB after compression 1PB before compression)
 10 analytics users working full time job will be querying the data all day long.
 Assume 3 years worth of data, frequency of sensor every 10 seconds. Several sensors,
several geo
Requirements
1. Choose infra (cloud vendor / datacenter / tools)
2. Design an ETL to extract data and load it to destination DB
3. Design how to Load the data one time. Estimate the loading time.
4. Design the DB (technology, compute for performance, estimated the costs
