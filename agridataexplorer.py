import pandas as pd
import sqlite3

#Load the cleaned dataset
df = pd.read_csv(r"F:/GUVI/guvi-2 projectss/cleaned_agriculture_data.csv")

#Create SQLite connection
conn = sqlite3.connect("agriculturequeries.db")

#Save DataFrame to SQLite table named 'agriculture'
df.to_sql("agriculture", conn, if_exists="replace", index=False)

#1) Year-wise Trend of Rice Production Across States (Top 3)
query1 = """
WITH RiceProduction AS (
SELECT [Year], [State Name], SUM([RICE PRODUCTION (1000 tons)]) AS TotalProduction
FROM agriculture
GROUP BY [Year], [State Name]
),
TopStates AS (
SELECT [State Name]
FROM RiceProduction
GROUP BY [State Name]
ORDER BY SUM(TotalProduction) DESC
LIMIT 3
)
SELECT r.*
FROM RiceProduction r
JOIN TopStates t ON r.[State Name] = t.[State Name]
ORDER BY r.Year, r.TotalProduction DESC;
"""

#Execute query and save results to CSV
result1 = pd.read_sql_query(query1, conn)
result1.to_csv("Q1_Yearwise_Rice_Trend.csv", index=False)

#2)Top 5 Districts by Wheat Yield Increase Over the Last 5 Years 

query2 = """
WITH WheatData AS (
SELECT [Dist Name], [Year], [WHEAT YIELD (Kg per ha)] AS Yield
FROM agriculture
),
LatestYear AS (
SELECT MAX(Year) AS max_year FROM WheatData
),
WheatGrowth AS (
SELECT
w1.[Dist Name],
w1.Yield AS Yield_5_Years_Ago,
w2.Yield AS Yield_Latest,
(w2.Yield - w1.Yield) AS YieldIncrease
FROM WheatData w1
JOIN WheatData w2
ON w1.[Dist Name] = w2.[Dist Name]
JOIN LatestYear l
ON w2.Year = l.max_year AND w1.Year = l.max_year - 5
)
SELECT *
FROM WheatGrowth
ORDER BY YieldIncrease DESC
LIMIT 5;
"""
result2 = pd.read_sql_query(query2, conn)
result2.to_csv("Q2_Wheat_Yield_Growth.csv", index=False)

#3)States with the Highest Growth in Oilseed Production (5-Year Growth Rate) 
query3 = """
WITH OilseedData AS (
    SELECT [State Name], [Year], SUM([OILSEEDS PRODUCTION (1000 tons)]) AS TotalProd
    FROM agriculture
    GROUP BY [State Name], [Year]
),
LatestYear AS (
    SELECT MAX(Year) AS max_year FROM OilseedData
),
GrowthCalc AS (
    SELECT
        o1.[State Name],
        o1.TotalProd AS Prod_5_Years_Ago,
        o2.TotalProd AS Prod_Latest,
        ROUND((o2.TotalProd - o1.TotalProd) * 100.0 / o1.TotalProd, 2) AS GrowthRate
    FROM OilseedData o1
    JOIN OilseedData o2 
        ON o1.[State Name] = o2.[State Name]
    JOIN LatestYear l 
        ON o2.Year = l.max_year AND o1.Year = l.max_year - 5
)
SELECT * FROM GrowthCalc
ORDER BY GrowthRate DESC;
"""
result3 = pd.read_sql_query(query3, conn)
result3.to_csv("Q3_Oilseed_Growth.csv", index=False)

#4)District-wise Correlation Between Area and Production for Major Crops (Rice,Wheat, and Maize)
query4 = """
SELECT 
    [Dist Name] AS District,
    [RICE AREA (1000 ha)] AS Rice_Area,
    [RICE PRODUCTION (1000 tons)] AS Rice_Production,
    [WHEAT AREA (1000 ha)] AS Wheat_Area,
    [WHEAT PRODUCTION (1000 tons)] AS Wheat_Production,
    [MAIZE AREA (1000 ha)] AS Maize_Area,
    [MAIZE PRODUCTION (1000 tons)] AS Maize_Production
FROM agriculture;
"""
result4 = pd.read_sql_query(query4, conn)
result4.to_csv("Q4_Correlation_Area_Production.csv", index=False)

#5).Yearly Production Growth of Cotton in Top 5 Cotton Producing States 
query5 = """
WITH CottonTotal AS (
    SELECT [State Name], SUM([COTTON PRODUCTION (1000 tons)]) AS TotalCotton
    FROM agriculture
    GROUP BY [State Name]
    ORDER BY TotalCotton DESC
    LIMIT 5
)
SELECT a.[Year], a.[State Name], SUM(a.[COTTON PRODUCTION (1000 tons)]) AS CottonProduction
FROM agriculture a
JOIN CottonTotal t ON a.[State Name] = t.[State Name]
GROUP BY a.[Year], a.[State Name]
ORDER BY a.[Year], CottonProduction DESC;
"""
result5 = pd.read_sql_query(query5, conn)
result5.to_csv("Q5_Cotton_Production_Trend.csv", index=False)

#6)Districts with the Highest Groundnut Production in 2020 
query6 = """
SELECT [Dist Name] AS District, SUM([GROUNDNUT PRODUCTION (1000 tons)]) AS GroundnutProduction
FROM agriculture
WHERE Year = 2020
GROUP BY [Dist Name]
ORDER BY GroundnutProduction DESC
LIMIT 10;
"""
result6 = pd.read_sql_query(query6, conn)
result6.to_csv("Q6_Groundnut_2020.csv", index=False)

#7)Annual Average Maize Yield Across All States 
query7 = """
SELECT Year, ROUND(AVG([MAIZE YIELD (Kg per ha)]), 2) AS AvgMaizeYield
FROM agriculture
GROUP BY Year
ORDER BY Year;
"""
result7 = pd.read_sql_query(query7, conn)
result7.to_csv("Q7_Maize_Yield.csv", index=False)

#8)Total Area Cultivated for Oilseeds in Each State
query8 = """
SELECT [State Name], SUM([OILSEEDS AREA (1000 ha)]) AS TotalOilseedArea
FROM agriculture
GROUP BY [State Name]
ORDER BY TotalOilseedArea DESC;
"""
result8 = pd.read_sql_query(query8, conn)
result8.to_csv("Q8_Oilseed_Area.csv", index=False)

#9)Districts with the Highest Rice Yield
query9 = """
SELECT [Dist Name] AS District, ROUND(AVG([RICE YIELD (Kg per ha)]), 2) AS AvgRiceYield
FROM agriculture
GROUP BY [Dist Name]
ORDER BY AvgRiceYield DESC
LIMIT 10;
"""
result9 = pd.read_sql_query(query9, conn)
result9.to_csv("Q9_Top_Rice_Yield_Districts.csv", index=False)

#10)Compare the Production of Wheat and Rice for the Top 5 States Over 10 Years 
query10 = """
WITH StateProduction AS (
    SELECT [State Name],
           SUM([WHEAT PRODUCTION (1000 tons)]) + SUM([RICE PRODUCTION (1000 tons)]) AS TotalProduction
    FROM agriculture
    GROUP BY [State Name]
    ORDER BY TotalProduction DESC
    LIMIT 5
)
SELECT [Year],
       a.[State Name],
       SUM([WHEAT PRODUCTION (1000 tons)]) AS WheatProduction,
       SUM([RICE PRODUCTION (1000 tons)]) AS RiceProduction
FROM agriculture a
JOIN StateProduction s ON a.[State Name] = s.[State Name]
GROUP BY [Year], a.[State Name]
ORDER BY [Year], a.[State Name];
"""
result10 = pd.read_sql_query(query10, conn)
result10.to_csv("Q10_Wheat_vs_Rice.csv", index=False)

