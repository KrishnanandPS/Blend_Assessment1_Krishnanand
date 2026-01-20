
# Load data into SQL table
# print("Loading data into SQL table 'trips'...")
# df.to_sql('trips', conn, if_exists='replace', index=False)
# print("✓ Data loaded to database")

import pandas as pd
import sqlite3

print("="*60)
print("SQL ANALYTICS LAYER")
print("="*60)

# Connect to existing database
conn = sqlite3.connect('taxi_analytics.db')
print("\n✓ Connected to existing database")

print("\n" + "="*60)
print("EXECUTING ANALYTICAL QUERIES")
print("="*60)

# Query 1: Peak Demand Hours
print("\n📊 QUERY 1: Peak Demand Hours")
print("-" * 60)
query1 = """
SELECT 
    hour,
    COUNT(*) as total_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as total_revenue
FROM trips
GROUP BY hour
ORDER BY total_trips DESC
LIMIT 10;
"""

result1 = pd.read_sql(query1, conn)
print(result1.to_string(index=False))
result1.to_csv('query1_peak_hours.csv', index=False)
print("✓ Saved: query1_peak_hours.csv")

# Query 2: Trip Distance Categories
print("\n\n📊 QUERY 2: Trip Distance Categories")
print("-" * 60)
query2 = """
SELECT 
    CASE 
        WHEN trip_distance < 1 THEN 'Short (< 1 mile)'
        WHEN trip_distance < 3 THEN 'Medium (1-3 miles)'
        WHEN trip_distance < 10 THEN 'Long (3-10 miles)'
        ELSE 'Very Long (10+ miles)'
    END as distance_category,
    COUNT(*) as total_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as total_revenue
FROM trips
GROUP BY distance_category
ORDER BY total_trips DESC;
"""

result2 = pd.read_sql(query2, conn)
print(result2.to_string(index=False))
result2.to_csv('query2_distance_categories.csv', index=False)
print("✓ Saved: query2_distance_categories.csv")

# Query 3: Weekday Analysis
print("\n\n📊 QUERY 3: Average Fare by Day of Week")
print("-" * 60)
query3 = """
SELECT 
    day_of_week,
    CASE day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END as day_name,
    COUNT(*) as total_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as total_revenue
FROM trips
GROUP BY day_of_week
ORDER BY day_of_week;
"""

result3 = pd.read_sql(query3, conn)
print(result3.to_string(index=False))
result3.to_csv('query3_weekday_analysis.csv', index=False)
print("✓ Saved: query3_weekday_analysis.csv")

# Query 4: Payment Type Analysis
print("\n\n📊 QUERY 4: Payment Type Analysis")
print("-" * 60)
query4 = """
SELECT 
    payment_type,
    COUNT(*) as total_trips,
    ROUND(AVG(tip_percentage), 2) as avg_tip_pct,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as total_revenue
FROM trips
GROUP BY payment_type
ORDER BY total_trips DESC;
"""

result4 = pd.read_sql(query4, conn)
print(result4.to_string(index=False))
result4.to_csv('query4_payment_analysis.csv', index=False)
print("✓ Saved: query4_payment_analysis.csv")

# Query 5: Peak vs Off-Peak Revenue
print("\n\n📊 QUERY 5: Peak vs Off-Peak Analysis")
print("-" * 60)
query5 = """
SELECT 
    CASE 
        WHEN is_peak = 1 THEN 'Peak Hours (7-9 AM, 5-7 PM)'
        ELSE 'Off-Peak Hours'
    END as period,
    COUNT(*) as total_trips,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(fare_amount), 2) as avg_fare
FROM trips
GROUP BY is_peak
ORDER BY is_peak DESC;
"""

result5 = pd.read_sql(query5, conn)
print(result5.to_string(index=False))
result5.to_csv('query5_peak_analysis.csv', index=False)
print("✓ Saved: query5_peak_analysis.csv")

# Query 6: Top Revenue Days
print("\n\n📊 QUERY 6: Top 10 Highest Revenue Days")
print("-" * 60)
query6 = """
SELECT 
    DATE(tpep_pickup_datetime) as date,
    COUNT(*) as total_trips,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(fare_amount), 2) as avg_fare
FROM trips
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY total_revenue DESC
LIMIT 10;
"""

result6 = pd.read_sql(query6, conn)
print(result6.to_string(index=False))
result6.to_csv('query6_top_revenue_days.csv', index=False)
print("✓ Saved: query6_top_revenue_days.csv")

# Query 7: Passenger Count Analysis
print("\n\n📊 QUERY 7: Passenger Count Analysis")
print("-" * 60)
query7 = """
SELECT 
    passenger_count,
    COUNT(*) as total_trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(AVG(trip_distance), 2) as avg_distance
FROM trips
GROUP BY passenger_count
ORDER BY passenger_count;
"""

result7 = pd.read_sql(query7, conn)
print(result7.to_string(index=False))
result7.to_csv('query7_passenger_analysis.csv', index=False)
print("✓ Saved: query7_passenger_analysis.csv")

# Save all queries to SQL file
print("\n" + "="*60)
print("SAVING QUERIES TO FILE")
print("="*60)

sql_file_content = f"""-- NYC Taxi Analytics - SQL Queries
-- Generated for January 2015 Dataset
-- Total Trips: 12,631,148
-- Total Revenue: $190,467,222.96

-- Query 1: Peak Demand Hours
{query1}

-- Query 2: Trip Distance Categories
{query2}

-- Query 3: Weekday Analysis
{query3}

-- Query 4: Payment Type Analysis
{query4}

-- Query 5: Peak vs Off-Peak Analysis
{query5}

-- Query 6: Top Revenue Days
{query6}

-- Query 7: Passenger Count Analysis
{query7}
"""

with open('analytics_queries.sql', 'w') as f:
    f.write(sql_file_content)

print("✓ Saved: analytics_queries.sql")

conn.close()

print("\n" + "="*60)
print("SQL ANALYTICS COMPLETE!")
print("="*60)
print("\n✅ Generated 7 CSV files + analytics_queries.sql")

