-- 1) Cree una tabla de `directors` con las columnas: 
-- Nombre, Apellido, Número de Películas.

CREATE TABLE directors (
    name varchar(300),
    lastName varchar(300),
    movies int
);

-- 2) El top 5 de actrices y actores de la tabla `actors` que tienen la mayor 
-- experiencia (i.e. el mayor número de películas filmadas) son también directores 
-- de las películas en las que participaron. Basados en esta información, inserten, 
-- utilizando una subquery los valores correspondientes en la tabla `directors`.

insert into directors (name, lastName, movies)
with
    actor_movies as (
        SELECT actor_id as actorID, COUNT(*) as total_movies
        from film_actor
        group by actorID
        order by total_movies desc
        limit 5
    ),
    actor_info as (
        select actor_id, first_name, last_name, total_movies
        from actor
        join actor_movies on actor_movies.actorID = actor_id  
    )
select first_name as name, last_name as lastName, total_movies as movies
from actor_info;

-- 3) Agregue una columna `premium_customer` que tendrá un valor 'T' o 'F' 
-- de acuerdo a si el cliente es "premium" o no. Por defecto ningún cliente será premium.

alter table customer
add column premium_customer enum('T','F') default 'F';

-- 4) Modifique la tabla customer. Marque con 'T' en la columna `premium_customer` 
-- de los 10 clientes con mayor dinero gastado en la plataforma.

-- ======================
UPDATE first_table, second_table
SET first_table.color = second_table.color
WHERE first_table.id = second_table.foreign_id

UPDATE table t
JOIN (
    sub_query
) top_customers ON (condition)
SET t.col1 = value;
-- ======================

update customer, (
    select customer_id, sum(amount) as spent
    from payment
    group by customer_id
    order by spent DESC
    limit 10
) as topSpent
set premium_customer = 'T'
where customer.customer_id = topSpent.customer_id;

-- 5) Listar, ordenados por cantidad de películas (de mayor a menor), los distintos 
-- ratings de las películas existentes (Hint: rating se refiere en este caso a la 
-- clasificación según edad: G, PG, R, etc).

-- rating A (50 peliculas)
-- rating B (30 peliculas)
-- rating C (20 peliculas)

select rating, count(*) as total_movies
from film
group by rating
order by total_movies desc;

-- 6) ¿Cuáles fueron la primera y última fecha donde hubo pagos?
-- payment.payment_date

select min(payment_date) as first_payment, max(payment_date) as last_payment
from payment;

-- 7) Calcule, por cada mes, el promedio de pagos 
-- (Hint: vea la manera de extraer el nombre del mes de una fecha).

DELIMITER //

CREATE FUNCTION monthName(input_date DATETIME)
RETURNS VARCHAR(200)
DETERMINISTIC
BEGIN
    DECLARE month_name VARCHAR(200);

    IF MONTH(input_date) = 1 THEN
        SET month_name = 'Enero';
    ELSEIF MONTH(input_date) = 2 THEN
        SET month_name = 'Febrero';
    ELSEIF MONTH(input_date) = 3 THEN
        SET month_name = 'Marzo';
    ELSEIF MONTH(input_date) = 4 THEN
        SET month_name = 'Abril';
    ELSEIF MONTH(input_date) = 5 THEN
        SET month_name = 'Mayo';
    ELSEIF MONTH(input_date) = 6 THEN
        SET month_name = 'Junio';
    ELSEIF MONTH(input_date) = 7 THEN
        SET month_name = 'Julio';
    ELSEIF MONTH(input_date) = 8 THEN
        SET month_name = 'Agosto';
    ELSEIF MONTH(input_date) = 9 THEN
        SET month_name = 'Septiembre';
    ELSEIF MONTH(input_date) = 10 THEN
        SET month_name = 'Octubre';
    ELSEIF MONTH(input_date) = 11 THEN
        SET month_name = 'Noviembre';
    ELSE
        SET month_name = 'Diciembre';
    END IF;

    RETURN month_name;
END //

DELIMITER ;

select monthName(payment_date) as Month, avg(amount) as Avg
from payment
group by Month;

+----------+----------+
| Month    | Avg      |
+----------+----------+
| May      | 4.169775 |
| June     | 4.166038 |
| July     | 4.227968 |
| August   | 4.232835 |
| February | 2.825165 |
+----------+----------+

-- 8) Listar los 10 distritos que tuvieron mayor cantidad de 
-- alquileres (con la cantidad total de alquileres).

with 
    customers_rent as (
        select c.customer_id as c_id, c.address_id as a_id, count(r.rental_id) as rents 
        from customer as c 
        inner join rental as r
        on c.customer_id = r.customer_id
        group by c.customer_id
    )
select a.district, sum(customers_rent.rents) as districtRents
from address as a
inner join customers_rent
on a.address_id = customers_rent.a_id
group by a.district
order by districtRents desc
limit 10;

+------------------+---------------+
| district         | districtRents |
+------------------+---------------+
| Buenos Aires     |           276 |
| California       |           252 |
| West Bengali     |           243 |
| So Paulo         |           237 |
| Shandong         |           236 |
| Uttar Pradesh    |           210 |
| Southern Tagalog |           204 |
| Maharashtra      |           203 |
| England          |           167 |
| Tamil Nadu       |           135 |
+------------------+---------------+

-- 9) Modifique la table `inventory_id` agregando una columna `stock` que 
-- sea un número entero y representa la cantidad de copias de una misma 
-- película que tiene determinada tienda. El número por defecto debería ser 5 copias.

alter table inventory add column stock int not null default 5;  

-- 10) Cree un trigger `update_stock` que, cada vez que se agregue un nuevo registro a 
-- la tabla rental, haga un update en la tabla `inventory` restando una copia al stock 
-- de la película rentada (Hint: revisar que el rental no tiene información directa 
-- sobre la tienda, sino sobre el cliente, que está asociado a una tienda en particular).

-- CREATE TRIGGER trigger_name trigger_time trigger_event 
-- ON table_name FOR EACH ROW
-- BEGIN
--      [trigger_order]
--      trigger_body 
-- END;

-- trigger_time: {BEFORE | AFTER} 
-- trigger_event: {INSERT | UPDATE | DELETE}
-- trigger_order: {FOLLOWS| PRECEDES} other_trigger_name 

-- cliente renta y se agrega una fila a rental (tenemos inventory_id)
-- tenemos la info del cliente, en particular address_id con eso obtenemos el store_id 
-- podemos actualizar el stock 

DELIMITER //

create trigger update_stock after insert
on rental for each row
begin 
    update inventory
    set stock = stock - 1
    where inventory_id = NEW.inventory_id;
end //

DELIMITER;

select stock from inventory where inventory_id = 1;
+-------+
| stock |
+-------+
|     5 |
+-------+

INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
VALUES ('2005-05-24 22:53:30', 1, 1, '2005-05-24 22:53:30', 1);

mysql> select stock from inventory where inventory_id = 1;
+-------+
| stock |
+-------+
|     4 |
+-------+

-- 11) Cree una tabla `fines` que tenga dos campos: `rental_id` y `amount`. 
-- El primero es una clave foránea a la tabla rental y el segundo es un valor 
-- numérico con dos decimales.

create table fines (
    rental_id int not null,
    amount decimal(10,2),
    foreign key (rental_id) references rental(rental_id)
);

--12) Cree un procedimiento `check_date_and_fine` que revise la tabla `rental` 
-- y cree un registro en la tabla `fines` por cada `rental` cuya devolución 
-- (return_date) haya tardado más de 3 días (comparación con rental_date). 
-- El valor de la multa será el número de días de retraso multiplicado por 1.5.

DELIMITER //

CREATE PROCEDURE check_date_and_fine()
BEGIN
    INSERT INTO fines (rental_id, amount)
    SELECT rental_id, (DATEDIFF(return_date, rental_date) - 3) * 1.5
    FROM rental
    WHERE return_date IS NOT NULL
      AND DATEDIFF(return_date, rental_date) > 3;
END$$

DELIMITER ;

-- 13) Crear un rol `employee` que tenga acceso de inserción, eliminación 
-- y actualización a la tabla `rental`.


create role employee;

grant insert on rental to employee;
grant update on rental to employee;
grant delete on rental to employee;

-- 14) Revocar el acceso de eliminación a `employee` y crear un rol `administrator` 
-- que tenga todos los privilegios sobre la BD `sakila`.

REVOKE DELETE
ON sakila.rental
FROM 'employee';

-- 15) Crear dos roles de empleado. A uno asignarle los permisos de `employee` 
-- y al otro de `administrator`.

create role employee_chill;
create role employee_hard;

grant employee to employee_chill;
grant all privileges on sakila.* to employee_hard;