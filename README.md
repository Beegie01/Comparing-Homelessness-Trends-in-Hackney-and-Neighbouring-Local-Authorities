# Quarterly Homelessness Trends: Hackney vs Neighbouring Local Authorities
This data analysis project involved creating an automated ETL pipeline to extract source data from multiple files in cloud storage, transform it, and load the refined data to support a real-time Power BI dashboard with insightful report.
### DATA
Dataset is publicly available.

### TOOLS USED
1. PyCharm IDE: Used for creating and running Python scripts.
2. PostgreSQL & PgAdmin4: Used for writing and executing SQL queries.
3. Power BI & Power Query: Used for building automated dashboards using DAX and M language.
4. OneDrive & OneLake: Used for secure, cloud-based data storage and access.
5. PowerPoint: Used to deliver the presentation

### PROJECT PROCESS OVERVIEW
![image](flow_diagram_for_dashboard_project.png)

### ETL ALGORITHMIC STEPS
Python:
1.Extract source data from Excel file into a source DataFrame
2.Drop completely empty rows and almost empty columns
3.Map columns to appropriate column names
4.Remove duplicate records
5.Keep only relevant columns
6.Drop blank system_id rows
7.Drop almost empty rows
8.Engineer quarter_indicator, is_neighbouring_la columns
9.Replace system placeholder for missing values “..” with more intuitive ””
10.Recast columns to appropriate data types
11.Repeat previous steps for remaining files and append final output to the same source DataFrame
12.Engineer a delta column to identify uniqueness of each row based on its content for source DataFrame
13.Connect to and extract existing data from a corresponding database table into a destination DataFrame
14.Enforce data type consistency between source and destination DataFrames
15.Engineer a delta column to identify uniqueness of each row based on its content for destination DataFrame
16.Compare delta columns of both DataFrames only keeping rows in the source DataFrame where data is fresh
17.Load fresh data from source DataFrame into target data warehouse table in the database
18.Refresh and load final output of transformations in the data warehouse into a DataFrame
19.Load data from DataFrame to CSV file stored in a cloud-based folder

SQL:
1.Get data from staging table of interest
2.Using the is_neighbouring_la column, select only records of the following local authorities: Southwark, Islington, Haringey, Lambeth, Tower Hamlets, Camden, Waltham Forest, Hammersmith & Fulham, and Newham
3.Calculate aggregated metrics (average) for selected records
4.Engineer columns system_id and local_authority for calculated aggregates to enable merging with more granular council records
5.Append grouped records to Hackney’s
6.Calculate percentages for metrics
7.Engineer year_qtr to enhance axis sorting in dashboard visuals
8.Unpivot metric columns into two columns named “metrics” (categorical) and “households” (values)
9.Pivot unique values of the local_authority into separate columns hackney_la and neighbouring_la
10.Store result as view


### REPORT SUMMARY
Full version of this report is available [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/Hackney%20Quarterly%20Report%20-%20MAR2025.pdf).<br>

### HOUSEHOLDS AND INITIAL ASSESSMENT
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/initial_assessment_report.pdf)

### OWED PREVENTION DUTY
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/prevention_duty_report.pdf)

### PREVENTION DUTY ENDED
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/prevention_duty_ending_report.pdf)

### OWED RELIEF DUTY
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/relief_duty_report.pdf)

### RELIEF DUTY ENDED
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/relief_duty_ending_report.pdf)

### HOUSEHOLDS IN TEMPORARY ACCOMMODATION
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/temp_accommodation_report.pdf)

### TEMPORARY ACCOMMODATION HOUSEHOLD COMPOSITION
View a snapshot of the dashboard [here](https://github.com/Beegie01/Comparing-Homelessness-Trends-in-Hackney-and-Neighbouring-Local-Authorities/blob/main/temp_accommodation_households_report.pdf)

### RECOMMENDATIONS<br>
1. Educate households on importance of encouraging friends or family members to apply early for their own housing options while relationships are still positive.
2. Encourage household members to report early signs of domestic abuse to help identify potential risks of homelessness and enable timely preventive measures.
3. Strengthen the council’s capacity to secure accommodation and expand its use of LA/HA stock to reduce reliance on more expensive alternatives.
4. Direct greater investment towards reducing the length of time families spend in temporary accommodation, in order to minimize the negative impact on growing children.
5. Investigate the root causes of unexpectedly high use of temporary accommodation by  single adult males.
