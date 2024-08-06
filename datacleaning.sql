use layoffstatistics;

SELECT * FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize data
-- 3. Null/Blank values
-- 4. Remove unnecessary rows or columns

CREATE TABLE layoffs_dev
LIKE layoffs;

SELECT * FROM layoffs_dev;

INSERT layoffs_dev
SELECT * 
FROM layoffs;

-- REMOVING DUPLICATES
-- Step 1
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_no
FROM layoffs_dev;

-- Step2 - We need to get rows row_no greater than 1 , which means they are duplicates
WITH duplicate_cte AS(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_no
	FROM layoffs_dev
)
SELECT * 
FROM duplicate_cte
where duplicate_cte.row_no > 1;

SELECT * FROM layoffs_dev
WHERE company = 'Oda';

-- For Oda company, there is no duplicates. Since we have taken only some  columns in CTE, based on those columns, this is considered as duplicate
-- To avoid that we need to consider all columns in the table
WITH duplicate_cte AS(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
		stage, country, funds_raised_millions) as row_no
	FROM layoffs_dev
)
SELECT * 
FROM duplicate_cte
where duplicate_cte.row_no > 1;

SELECT * FROM layoffs_dev
WHERE company = 'Casper';



CREATE TABLE `layoffs_dev2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_no` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_dev2;

INSERT INTO layoffs_dev2
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
		stage, country, funds_raised_millions) as row_no
	FROM layoffs_dev;
    
SELECT * FROM layoffs_dev2
WHERE row_no >1;

-- SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_dev2
WHERE row_no >1;

-- SET SQL_SAFE_UPDATES = 1;

-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_dev2;

UPDATE layoffs_dev2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_dev2
ORDER BY 1;

SELECT *
FROM layoffs_dev2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_dev2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT DISTINCT location
FROM layoffs_dev2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_dev2
ORDER BY 1;

SELECT *
FROM layoffs_dev2
WHERE country like 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_dev2
order by 1;

Update layoffs_dev2
set country = TRIM(TRAILING '.' FROM country)
WHERE country like 'United States%';

SELECT `date`
from layoffs_dev2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_dev2;

Update layoffs_dev2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_dev2
MODIFY COLUMN `date` DATE;

-- Handling Null and Blank Values
SELECT *
FROM layoffs_dev2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_dev2
WHERE industry IS NULL 
OR industry = "";

SELECT * 
FROM layoffs_dev2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_dev2 t1
JOIN layoffs_dev2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');

UPDATE layoffs_dev2 t1
JOIN layoffs_dev2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');

SELECT * 
FROM layoffs_dev2
WHERE company = "Bally's Interactive";

SELECT *
FROM layoffs_dev2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_dev2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_dev2;

ALTER TABLE layoffs_dev2
DROP COLUMN row_no;