# Movie Ratings Analysis with PySpark

This project performs various analyses on movie ratings data using PySpark's structured API. It consists of three main tasks that help analyze user behavior, identify churn risk, and track movie watching trends.

## Project Structure

```
handson-7-spark-structured-api-movie-ratings-analysis/
├── input/
│   └── movie_ratings_data.csv
├── Outputs/
│   ├── binge_watching_patterns.csv
│   ├── churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── README.md
└── docker-compose.yml
```

## Dataset

The input dataset (movie_ratings_data.csv) contains movie ratings data with the following schema:
- UserID (INT)
- MovieID (INT)
- MovieTitle (STRING)
- Genre (STRING)
- Rating (FLOAT)
- ReviewCount (INT)
- WatchedYear (INT)
- UserLocation (STRING)
- AgeGroup (STRING)
- StreamingPlatform (STRING)
- WatchTime (INT)
- IsBingeWatched (BOOLEAN)
- SubscriptionStatus (STRING)

## Approach

### General Approach

PySpark's structured API is used in this project to effectively evaluate massive amounts of movie ratings data. The method uses five standard procedures for every task:

1. *Data Loading*: Every task starts a SparkSession and loads data according to a predetermined structure.
2. *Data Transformation*: PySpark DataFrame operations are used to apply task-specific transformations.
3. *Result Generation*: Outcomes are calculated and presented in compliance with the specifications.
4. *Output Writing*: The outcomes are written to CSV files and translated to pandas DataFrames.

### Task-Specific Approaches

#### Task 1: Detect Binge Watching Patterns

For this task, the approach involves:
- Filtering the dataset to isolate users who have binge-watched movies
- Grouping the filtered data by age groups
- Performing an aggregation to count users in each group
- Calculating the percentage of binge-watchers relative to the total users in each age group
- Rounding the percentages to two decimal places for readability

#### Task 2: Identify Churn Risk Users

The approach for identifying churn risk:
- Applying multiple filter conditions simultaneously to identify users with canceled subscriptions and low watch time
- Counting the distinct users that meet these criteria
- Presenting the results in a format that highlights the potential churn risk segment

#### Task 3: Analyze Movie Watching Trends

For trend analysis:
- Grouping data by the year movies were watched
- Counting the number of movie watches per year
- Ordering the results by year to create a chronological view of trends
- Using the resulting time series to identify patterns in movie watching behavior

## Findings

### Binge Watching Patterns

Based on the analysis, we found that:
- Different age groups display varying tendencies toward binge-watching
- The output shows the percentage of users in each age group (Teen, Adult, Senior) who engage in binge-watching
- This information can help streaming platforms tailor content recommendations and release strategies for different age segments

### Churn Risk Users

Key findings about churn risk:
- A specific segment of users exhibiting both canceled subscriptions and low watch time was identified
- These users represent a critical cohort for potential re-engagement strategies
- The count of such users provides a baseline for measuring the effectiveness of retention efforts

### Movie Watching Trends

The trend analysis revealed:
- Patterns in movie watching behavior over different years
- Years with higher movie consumption, potentially correlating with major releases or external factors
- Potential seasonal or annual trends that could inform content acquisition and marketing strategies

## Challenges Faced and Solutions

### Challenge 1: CSV Output Format
*Challenge:*  Because Spark's write.csv() method by default generates a directory containing part files instead of a single CSV file, it was challenging to generate the desired output format.


*Solution:* To convert Spark DataFrames to pandas DataFrames, the write_output function was modified. Then, a single CSV file was written using pandas' to_csv() method. Post-processing part files were no longer necessary with this method.

### Challenge 2: Data Type Handling
*Challenge:* Special treatment was required for boolean values in the input data, especially for the IsBingeWatched column.

*Solution:*  To guarantee correct data type interpretation, an explicit schema specification was used during data loading. Boolean values were explicitly selected using col("IsBingeWatched") == To guarantee compatibility with Spark's SQL expression evaluation, use true syntax.

### Challenge 3: Output Format for Churn Risk Analysis
*Challenge:* In contrast to the normal aggregation result, a particular presentation was needed for the churn risk analysis's intended output format.

*Solution:* Using the createDataFrame() method, a custom DataFrame with the required structure was created. It contained a tuple with the descriptive text and count value, as well as specifically stated column names. This preserved the analytical results while guaranteeing the output complied with the required format.

### Challenge 4: Percentage Calculation and Rounding
*Challenge:* Accurately calculating binge-watcher percentages while making sure to round to two decimal places.

*Solution:* utilized the spark_round function to compute and round percentages and Spark's mathematical functions for division. To guarantee precise ratio computation, I joined the DataFrames with the overall user and binge-watcher counts before computing percentages.

### Challenge 5: Resource Management
*Challenge:* Efficient resource utilization when processing potentially large datasets.

*Solution:* Implemented proper SparkSession management with explicit spark.stop() calls to release resources after job completion. Used appropriate DataFrame operations like filtering early in the processing pipeline to reduce data volume in subsequent operations.

## Implementation Details

- Every task is carried out in a distinct Python file, and they all process data using PySpark's structured API.
- The write_output method was changed to use pandas to write results straight to CSV files.
- The Outputs folder contains the results.

## Running the Code

To run any of the tasks:

```bash
spark-submit src/task1_binge_watching_patterns.py

spark-submit src/task2_churn_risk_users.py

spark-submit src/task3_movie_watching_trends.py
```

## Dependencies

- PySpark
- pandas

## Conclusion

This project shows off how well PySpark's structured API can analyze massive amounts of movie ratings data. We can learn a lot about user behavior and preferences by looking at binge-viewing patterns, churn risk user identification, and movie watching trends. Streaming platforms can use these insights to guide their content purchase choices, retention tactics, and recommendation systems.

The code's modular design makes it simple to add more analyses or modify it to accommodate modifications to the data schema. Creating readable CSV files from Spark DataFrames is made easier by using pandas for output writing.

By solving the aforementioned difficulties, we have developed a solid analytical solution that uses Spark's distributed processing capabilities for scalable data analysis while generating clear, practical outputs.