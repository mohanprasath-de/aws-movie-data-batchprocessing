## QUALITY MOVIE DATA ANALYSIS
Batch Processing - Analysisng the movie data to get the quality movie based on ratings and extra quality metrics

## TECHNOLOGIES

| S.No | TECH                                   | Services Used                          |
|------|----------------------------------------|----------------------------------------|
| 1    | Source                                 | AWS S3                                 |    
| 2    | Crawlers                               | Glue Crawlers & Catalogue              |
| 3    | Transformation                         | PySpark                                |
| 4    | Data Warehouse                         | AWS RedShift                           |
| 5    | Failed data analysis                   | AWS Athena                             |

## ARCHITECTURE

![Pipline_Architecture_Diagram](https://github.com/mohanprasath-de/movie-data-batchprocessing/blob/main/BatchProcessing_architecture.png)

## EXPLANATION

1. S3 will act as a data lake for the source
2. Using the Glue crawler, create the table for the source and target in the AWS Glue Database
3. Using the help of Glue data quality drafted data quality rules and PySpark for data transformation
4. Uploaded the qualified data into AWS RedShift warehouse and failed data in to another S3 bucket
5. Using Athena for analysing failed data
6. Connected dashboard with Redshift table for visualising the results
