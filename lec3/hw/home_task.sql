/*
Завдання на SQL до лекції 03.
*/

/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT c.name AS film_category, COUNT(fc.film_id) AS film_count
FROM film_category fc
    JOIN category c ON fc.category_id = c.category_id
GROUP BY
    c.name
ORDER BY film_count DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT CONCAT(first_name, ' ', last_name) AS actor, COUNT(r.rental_id) AS rental_count
FROM
    actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN inventory i ON fa.film_id = i.film_id
    JOIN rental r ON r.inventory_id = i.inventory_id
GROUP BY
    a.actor_id
ORDER BY rental_count DESC
LIMIT 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT c.name AS top_category, SUM(p.amount) AS total_spent
FROM
    category c
    JOIN film_category fc ON c.category_id = fc.category_id
    JOIN inventory i ON fc.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN payment p ON r.rental_id = p.rental_id
GROUP BY
    c.category_id
ORDER BY total_spent DESC
LIMIT 1;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT f.title AS film_not_in_inventory
FROM film f
    LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE
    i.film_id IS NULL;

/*
Варіант з NOT EXISTS:
*/

SELECT f.title AS film_not_in_inventory
FROM film f
WHERE
    NOT EXISTS (
        SELECT *
        FROM inventory i
        WHERE
            i.film_id = f.film_id
    );

/*
Варіант з EXCEPT:
*/

SELECT f.title
FROM film f EXCEPT
SELECT f.title
FROM film f
    JOIN inventory i ON f.film_id = i.film_id;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT
    CONCAT(
        a.first_name,
        ' ',
        a.last_name
    ) AS actor,
    COUNT(fa.film_id) AS appear_in_children_films
FROM
    actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
WHERE
    c.name = 'Children'
GROUP BY
    a.actor_id
ORDER BY appear_in_children_films DESC
LIMIT 3;