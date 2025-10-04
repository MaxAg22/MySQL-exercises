Ej6) 
Listar el nombre de cada país con la cantidad de habitantes de su ciudad 
más poblada. (Hint: Hay dos maneras de llegar al mismo resultado. 
Usando consultas escalares o usando agrupaciones, encontrar ambas).

SELECT country.name, max(city.population)
FROM city
INNER JOIN country
    ON country.code = city.countrycode
group by country.name
limit 20;

Ej7)
Listar aquellos países y sus lenguajes no oficiales cuyo porcentaje de 
hablantes sea mayor al promedio de hablantes de los lenguajes oficiales.

SELECT country.name, countrylanguage.language
FROM country
LEFT JOIN countrylanguage
ON country.code = countrylanguage.countrycode
AND countrylanguage.IsOfficial = 'F'
INNER JOIN (   SELECT country.code, avg(CL.percentage) as avgper 
            FROM country
            LEFT JOIN countrylanguage AS CL
            ON country.code = CL.countrycode AND CL.IsOfficial = 'T'
            GROUP BY country.code
        ) AS ca ON country.code = ca.code
WHERE countrylanguage.percentage > ca.avgper;

Ej8) Listar la cantidad de habitantes por continente ordenado en forma descendente.

SELECT country.continent as continent, sum(country.population) as population
FROM country
GROUP BY country.continent
ORDER BY  population desc;

Ej9) Listar el promedio de esperanza de vida (LifeExpectancy) 
por continente con una esperanza de vida entre 40 y 70 años.

SELECT country.continent as continent, avg(country.LifeExpectancy) as LifeExpectancy
FROM country
GROUP BY country.continent
HAVING LifeExpectancy >= 40 and LifeExpectancy <= 70
order by LifeExpectancy desc;

Ej10) Listar la cantidad máxima, mínima, promedio y suma de habitantes por continente.

SELECT  country.continent as continent, 
        min(country.population) as min,
        max(country.population) as max,
        sum(country.population) as sum
FROM country
GROUP BY country.continent;
        

