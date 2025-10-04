-- 1. Obtener los usuarios que han gastado más en reservas

SELECT u.name AS UserName, SUM(p.amount) AS Spent
FROM users AS u
INNER JOIN payments AS p ON u.id = p.user_id
GROUP BY u.id
ORDER BY Spent DESC
LIMIT 10;

+------------------------+---------+
| UserName               | Spent   |
+------------------------+---------+
| Omer Williamson        | 9729.00 |
| Fr. Charlie Walsh      | 9466.00 |
| Marjory Fadel II       | 8751.00 |
| Fredric Kiehn          | 8016.00 |
| Mr. Jarrod Oberbrunner | 7552.00 |
| Dr. Reba Glover        | 7524.00 |
| Fr. Shauna Mills       | 7516.00 |
| Annamaria Lubowitz DDS | 7063.00 |
| Rep. Tonita McGlynn    | 7055.00 |
| Ricky Konopelski       | 7050.00 |
+------------------------+---------+

-- 2. Obtener las 10 propiedades con el mayor ingreso total por reservas

SELECT p.name AS Property, SUM(b.total_price) as totalEarn 
FROM properties AS p
INNER JOIN bookings AS b ON b.property_id = p.id
WHERE b.status = 'confirmed'
GROUP BY Property
ORDER BY totalEarn DESC
LIMIT 10;

+--------------------+-----------+
| Property           | totalEarn |
+--------------------+-----------+
| Paradise Heights   |  11558.00 |
| Paradise Oaks      |   7646.00 |
| Autumn Pointe      |   7397.00 |
| Autumn Square      |   6251.00 |
| Willow Estates     |   5875.00 |
| Summer Heights     |   5038.00 |
| Paradise Square    |   4838.00 |
| University Gardens |   4734.00 |
| Autumn Oaks        |   4495.00 |
| Autumn Court       |   4006.00 |
+--------------------+-----------+

-- 3. Crear un trigger para registrar automáticamente reseñas negativas en la tabla de
-- mensajes. Es decir, el owner recibe un mensaje al obtener un review menor o igual a 2.


DELIMITER $$

CREATE TRIGGER register_negative_review after INSERT
ON reviews FOR EACH ROW
BEGIN 
    DECLARE ownerID INT;

    IF NEW.rating <= 2 THEN
        SELECT owner_id INTO ownerID
        FROM properties 
        WHERE id = NEW.property_id;

        INSERT INTO messages (sender_id, receiver_id, property_id, content)
        VALUES (NEW.user_id, ownerID, NEW.property_id, NEW.comment);
    END IF;
  
END$$

DELIMITER ; 

select * from bookings limit 1;
+------+-------------+---------+------------+------------+-------------+-----------+---------------------+
| id   | property_id | user_id | check_in   | check_out  | total_price | status    | created_at          |
+------+-------------+---------+------------+------------+-------------+-----------+---------------------+
| 1302 |        1619 |    1747 | 2024-11-29 | 2024-12-12 |     4838.00 | confirmed | 2024-10-02 16:24:46 |
+------+-------------+---------+------------+------------+-------------+-----------+---------------------+

insert into reviews (booking_id, user_id, property_id, rating, comment) values (1302, 1747, 1619, 1, 'Horrible wacho');
Query OK, 1 row affected (0.00 sec)

select * from messages where property_id = 1619 and sender_id = 1747;
+-----+-----------+-------------+-------------+----------------+---------------------+
| id  | sender_id | receiver_id | property_id | content        | sent_at             |
+-----+-----------+-------------+-------------+----------------+---------------------+
| 301 |      1747 |        1829 |        1619 | Horrible wacho | 2025-10-02 22:01:47 |
+-----+-----------+-------------+-------------+----------------+---------------------+
1 row in set (0.00 sec)


-- 4. Crear un procedimiento llamado process_payment que:
-- Reciba los siguientes parámetros:
-- input_booking_id (INT): El ID de la reserva.
-- input_user_id (INT): El ID del usuario que realiza el pago.
-- input_amount (NUMERIC): El monto del pago.
-- input_payment_method (VARCHAR): El método de pago utilizado (por ejemplo,
-- "credit_card", "paypal").
-- Requisitos: verificar si la reserva asociada existe y está en estado confirmed. Insertar
-- un nuevo registro en la tabla payments. Actualizar el estado de la reserva a paid.
-- No es necesario manejar errores ni transacciones en este procedimiento.

DELIMITER $$

CREATE PROCEDURE process_payment(
    IN input_booking_id INT,
    IN input_user_id INT,
    IN input_amount NUMERIC,
    IN input_payment_method VARCHAR(200))

BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM bookings AS b
        WHERE   b.id = input_booking_id AND b.user_id = input_user_id AND b.status = 'confirmed'
    ) THEN
        INSERT INTO payments (booking_id, user_id, amount, payment_method, status)
        VALUES (input_booking_id, input_user_id, input_amount, input_payment_method, 'confirmed');
    END IF;
END$$

DELIMITER ;

