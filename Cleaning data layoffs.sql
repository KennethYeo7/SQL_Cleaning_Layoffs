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






