SELECT *
FROM layoffs
;
CREATE TABLE layoffs_staging 
LIKE layoffs
;

SELECT *
FROM layoffs_staging
;

INSERT layoffs_staging
SELECT *
FROM layoffs
;

-- Cleaning data

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company,
	industry, 
	total_laid_off,
	`date`,
	stage,
	country,
	funds_raised_millions) AS row_num
FROM layoffs_staging
;

WITH duplicate_cte  AS
( 
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company,
	location, 
	industry, 
	total_laid_off,
	`date`,
	stage,
	country,
	funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1
;

SELECT *
FROM layoffs_staging
WHERE company = 'casper'
;

CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging_2
WHERE row_num > 1
;

INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,
	location, 
	industry, 
	total_laid_off,
	`date`,
	stage,
	country,
	funds_raised_millions) AS row_num
FROM layoffs_staging
;

DELETE 
FROM layoffs_staging_2
WHERE row_num > 1
;

SELECT * 
FROM layoffs_staging_2
;

-- Standardizing Data 

SELECT *
FROM layoffs_staging_2
;

UPDATE layoffs_staging_2
SET company = TRIM(company) 
;

SELECT DISTINCT industry
FROM layoffs_staging_2
ORDER BY industry
;

SELECT *
FROM layoffs_staging_2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging_2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT location 
FROM layoffs_staging_2
ORDER BY 1
;

SELECT DISTINCT country
FROM layoffs_staging_2
ORDER BY 1
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging_2
ORDER BY 1
;

UPDATE layoffs_staging_2 
SET country = TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%'
;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging_2
;

UPDATE layoffs_staging_2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')
; 

ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` date
;

SELECT DISTINCT stage
FROM layoffs_staging_2
ORDER BY 1
;

SELECT * 
FROM layoffs_staging_2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT * 
FROM layoffs_staging_2 
WHERE industry IS NULL 
OR industry = ''
;

SELECT * 
FROM layoffs_staging_2
WHERE company = 'Airbnb'
; 

SELECT * 
FROM layoffs_staging_2 AS t2
JOIN layoffs_staging_2 AS t1
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
; 

UPDATE layoffs_staging_2 
SET industry = null 
WHERE industry =''
; 

UPDATE layoffs_staging_2 AS t2
JOIN layoffs_staging_2 AS t1
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL
;

SELECT *
FROM layoffs_staging_2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL 
; 

DELETE 
FROM layoffs_staging_2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL 
; 

SELECT *
FROM layoffs_staging_2
;

ALTER TABLE layoffs_staging_2 
DROP COLUMN row_num
;

SELECT * 
FROM layoffs_staging_2
;

SELECT MAX(total_laid_off), Max(percentage_laid_off) 
FROM layoffs_staging_2
;

-- TRYING TO IDENTIFY WHAT THE MAX PEOPLE LAID OFF WAS AND THE HIGHEST %
-- IN THE ABOVE QUERY WE SEE THAT THE HIGHEST AMOUNT OF PEOPLE LAIDED FROM A SINGLUAR COMPANY IN A SINGLE YEAR WAS 12,000 PEOPLE AND THAT ATLEAST ONE COMPANY ALSO HAD A 100% LAYOFF FROM 2020-2023 

SELECT * 
FROM layoffs_staging_2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
;

-- IN THE ABOVE QUERY TRYING TO IDENTIFY WHAT WHICH COMPANIES HAD A 100% lay off and I ordered it by amount of employees laid off, this can also tell us how big the company was 
-- we can also see that the majority of those companies that had a 100% layoff rate had less than 1,00 employees

SELECT * 
FROM layoffs_staging_2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;

-- Here we are looking at the same thing however instead ordering the data by total laid off we are ordering it by funds raised 
-- This could tell us how big the commpany was and their economic impact and we can also see that majority of the companies that had that100% layoff were companies that had less than $600 million raised 

SELECT company, SUM(total_laid_off) 
FROM layoffs_staging_2
GROUP BY company 
ORDER BY 2 DESC
;

-- Here I was tryigbn to look at which companies had the highest people layed off over the 4 years, as we can see Amazon tops the list at 18150 people layed off 
-- The query also is ordered by the SUM(total_laid_off) 

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging_2
GROUP BY industry 
ORDER BY 2 DESC
;

-- 

SELECT funds_raised_millions, (select AVG(funds_raised_millions) FROM layoffs_staging_2) AS AVG_Funds
FROM layoffs_staging_2
;

SELECT *, (SELECT ROUND((AVG(funds_raised_millions))) FROM (SELECT funds_raised_millions 
	FROM layoffs_staging_2
	WHERE funds_raised_millions IS NOT NULL
	ORDER BY funds_raised_millions DESC
	LIMIT 10) AS FUNDS_TABLE) AS Avg_top_5
FROM layoffS_staging_2
; 
-- Here we are looking for the average funds raised from the 10 companies that had the highest funds raised 

SELECT country, SUM(total_laid_off) as Layoffs_per_country
FROM layoffs_staging_2
GROUP BY country 
ORDER BY 2 DESC
;
-- Looking at top-down approach we are starting by looking at the amount of layoff per country from 2020-2023
-- the data could be skewed towards the U.S. as seen in regards to the 2 queries below, this dataset is heavily leaned towards the U.S. as they make up 64.9% of all data entries   

SELECT country, count(country) as companies_per_country 
FROM layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC
;

SELECT COUNT(country)
FROM layoffs_staging_2
;

SELECT country, total_laid_off, COUNT(country) as countries_with_nulls 
FROM layoffs_staging_2
GROUP BY 1,2
HAVING total_laid_off IS NULL
ORDER BY 3 DESC
;
-- The Purpose of this query was to locate the amount of NULL values per country 
-- As we can see, the U.S. has the most amount of NULL values which would not affect its place in this dataset as being the country with the highest amount of layoffs, HOWEVER we have to go back to the fact that the U.S. represent 64.9% of the dataset 

SELECT *
FROM (SELECT country, Count(country) as companies_per_country 
FROM layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC
) AS t1
JOIN  (SELECT country, total_laid_off, COUNT(country) as countries_with_nulls 
FROM layoffs_staging_2
GROUP BY 1,2
HAVING total_laid_off IS NULL
ORDER BY 3 DESC) AS t2 
	ON t1.country = t2.country
ORDER BY companies_per_country DESC
;
-- Here I just wanted to compare the available as have to the null values. This was just to understand if the null values had actually numeric values, would the ranking of which countries have the most layoffs change 
-- Looking at this data even though Canada had the 3 highest amount of companies that laid off employees they actually ranked around 8th for total laid off employees assuming that the missing data doesn't cause major change
-- Looking at the bottom countries with the fewest reported layoffs we can assume that the data is underrepresented due to smaller population sizes or limited data availability, which may not accurately reflect the actual employment landscape.

SELECT industry, SUM(total_laid_off) as Layoffs_per_industry
FROM layoffs_staging_2
GROUP BY industry  
ORDER BY 2 DESC
;
-- Moving on tin the top-down approach we are moving on to the amount of layoffs per industry
-- as we can see here Consumer and retail had the most amount of layoffs in 2020-2023 which makes sense as during covid consumers felt that the cost of living was getting higher and due to high amount of the population getting sick and missing work people had less money to spend 

SELECT * 
FROM (SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS layoffs_per_year, industry  
FROM layoffs_staging_2
GROUP BY year, industry
HAVING industry = 'Consumer'
ORDER BY 2 DESC) AS t1
JOIN (SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS layoffs_per_year, industry  
FROM layoffs_staging_2
GROUP BY year, industry
HAVING industry = 'Retail'
ORDER BY 2 DESC) AS t2
	ON t1.year = t2.year 
;

-- Here we can see that most of the layoffs here was in 2022 which could be represented as a lagging indcator as conumser spending is normally viewed this way 

SELECT *
FROM (SELECT industry, Count(total_laid_off) as Layoffs_per_industry
FROM layoffs_staging_2
GROUP BY industry  
ORDER BY 2 DESC
) AS I1
JOIN ( SELECT industry, total_laid_off, COUNT(industry) as industry_with_nulls 
FROM layoffs_staging_2
GROUP BY 1,2
HAVING total_laid_off IS NULL
ORDER BY 3 DESC) AS I2
	ON I1.industry = I2.industry
ORDER BY Layoffs_per_industry DESC
;
-- When comparing the entries with available info vs without we can see that healthcare had a similar situation as Canada where they had a large amount of companies that laid off employees but the total amount of laid off employees was around 6th highest 
-- We could also look at media vs cryto, cryto would most likely beat media in most layoffs which is somewhat interesting as cryto took off during covid due to "HYPE" that surrounded Doge coin and bitcoin  
-- Looking at the bottom of the list we can see that Aerospace remained very stable, I think taking a look at the hiring of staff would be an interesting insight as we could see how much change actually occurred within this industry as it seemingly looks very stable

SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS layoffs_per_year, industry  
FROM layoffs_staging_2
GROUP BY year, industry
HAVING industry = 'Healthcare'
ORDER BY 2 DESC
;

-- Here I just wanted to see when the layoffs were happening for healthcare, and as we can see, the vast majority of layoffs occurred in 2022 which somewhat makes sense as the healthcare sector was over staffed with temporary staff and contract workers to help fight off covid so seeing a decrease here isnt that suprising 

SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS layoffs_per_year, industry  
FROM layoffs_staging_2
GROUP BY year, industry
HAVING industry = 'Crypto'
ORDER BY 2 DESC
;
-- Here i just wanted to see when the layoffs were happening for cryto, and I noticed that most of the reported cases were from 2022. This timing aligns with the period when economies were starting to recover from the COVID-19 pandemic. It seems likely that the downturn in crypto during that time was partly a reaction to the shift in economic conditions

SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS layoffs_per_year
FROM layoffs_staging_2
GROUP BY year
ORDER BY 2 DESC
;

-- I kept seeing a pattern where 2022 had the mostt amount of layoffs in each industry and thought "what if similar to the data being skewed towards the U.S., the data is skewed towards the year 2022." And as we can see that data is skewed toward the year 2022 as it represents more than 50% of the dataset 

SELECT DISTINCT industry 
FROM layoffs_staging_2
;
-- I noticed that companies like google were labeled as consumer but I believe that they are a more tech based company so I looked if "tech" was anywhere on the list as it was not the closet thing weas fin-tech but that is a separate category

SELECT company, industry, SUM(total_laid_off) AS Layoffs_per_company, stage
FROM layoffs_staging_2
GROUP BY 1,2,4
ORDER BY 3 DESC
;
-- here we can see that the companies with thge higest layoffs are some of the largest companies in the world so this makes sense

SELECT stage, COUNT(stage)
FROM layoffs_staging_2
GROUP BY 1
ORDER BY 2 DESC
;
-- I noticed that most of the companies with the highest layoffs were companies that were post IPO and wanted to check if the data was skewed towards post IPO companies but the distrbution somewhat makes sense however Unknown section takes up more of the dataset than I would like

