-- data cleaning
select *
from layoffs
 ;
 create table layoffs2
 like layoffs ;
 
 insert layoffs2
 select *
 from layoffs;
 
 select*
 from layoffs2 ;
 
 select *,
 row_number() over(
 partition by company,location,industry,total_laid_off,percentage_laid_off,`date`)as row_no
 from layoffs2;
 
 with duplicate_cte as
 (
	 select *,
 row_number() over(
 partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country)as row_no
 from layoffs2
 )
select * 
from duplicate_cte
;

CREATE TABLE `layoffs3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs3
select *,
 row_number() over(
 partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country)as row_no
 from layoffs2;

 select *
 from layoffs3 ;
 
 select company,trim(company)
 from layoffs3;
 
 update layoffs3
 set company = trim(company);
 
 select *
 from layoffs3
 where country like'united states%';
 
update layoffs3
set country ='United States'
where country like'united states%';

select distinct country
from layoffs3
order by 1; 

select `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
from layoffs3;

update layoffs3
set `date`= STR_TO_DATE(`date`,'%m/%d/%Y');

alter table layoffs3
modify column `date` date;

select *
from layoffs3
where industry is null
or industry = '' ;

update layoffs3 t1
join layoffs2 t2 
	on t1.company= t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs3
where total_laid_off is null 
and percentage_laid_off is null;

delete
from layoffs3
where total_laid_off is null 
and percentage_laid_off is null;

alter table layoffs3
drop column row_num;
-- exploratory data analysis

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs2
GROUP BY stage
ORDER BY 2 DESC;

WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs3
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs3
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;