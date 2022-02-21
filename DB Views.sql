--vista1
--buscamos a las personas que han llegado con y sin tapaboca por dia contando la cdad de cada uno
create or replace view vista1 as
select has_mask, arrival_time::date, 
div(sum(case when has_mask = 'true' then 2 else 0 end), 2) as Tapaboca,
sum(case when has_mask = 'false' then 1 else 0 end) as SinTapaboca
from entry_registry
group by has_mask, arrival_time::date


--vista2
--buscamos por dia el promedio de tiempo de estadia en tienda de los clientes
create or replace view vista2 as
select arrival_time::date, avg(exit_time - arrival_time) as tiempo_prom
from entry_registry
where has_mask = 'true'
group by arrival_time::date

--vista3
--buscamos por dia el promedio de temperatura de los clientes que llegan a la tienda
create or replace view vista3 as
select arrival_time::date, avg(body_temp) as temp_prom
from entry_registry
group by arrival_time::date

--vista4
--buscamos las 3 categorias menos vendidas ded las tiendas
create or replace view vista4 as
select category.name, sum(sale_details.quantity) from sale_details
inner join product_category
on sale_details.product_id = product_category.product_id
inner join category
on product_category.category_id = category.category_id
inner join sale
on sale_details.sale_id = sale.sale_id
inner join customer
on sale.customer_id = customer.customer_id
--where customer.affiliate = 'true' 
group by sale.store_id, category.category_id
order by sum(sale_details.quantity) asc limit 3

--vista5
--buscamos las personas rechazadas en la tienda totales
create or replace view vista5 as
select
count(allowed_in) AS personas_rechazadas
from entry_registry
where allowed_in = 'false'

--vista6
-- buscamos los 5 productos mas vendidos en las tiendas
create or replace view vista6 as
SELECT
              prod.name AS product_name,
			  count(prod.name) as cuenta
FROM
              product prod
              INNER JOIN sale_details pago ON pago.product_id = prod.product_id
group by product_name
order by cuenta desc limit 5;

--vista7
--error
create or replace view vista7 as
--query solo 1 tienda
select 
	customer_id,
	count(store_id) as tiendas
from sale
group by customer_id
except
select
customer_id,
	count(store_id) as tiendas
from sale
having count(store_id) = 1
group by customer_id

--quiery2
select 
	customer_id,
	count(store_id) as tiendas
from sale
group by customer_id
except
select
customer_id,
	count(store_id) as tiendas
from sale
having count(store_id) = 2
group by customer_id
--error


--vista8
--buscamos por estantes de cada tienda la cdad de productos por categoria vendidos es decir que han rotado
create or replace view vista8 as
select
b.shelf_id as estante,
d.store_id as tienda,
f.quantity as cdad,
h.category_id as categoria
from smart_shelf as b
join store d on b.store_id = d.store_id
join sale e on d.store_id = e.store_id
join sale_details f on e.sale_id = f.sale_id
join product i on f.product_id = i.product_id
join product_category h on i.product_id = h.product_id
group by b.shelf_id, d.store_id, e.sale_id, f.sale_id, f.product_id, f.quantity, h.category_id
order by f.quantity desc;


--vista9
--buscamos a los clientes que han pagado con dos bancos distintos y que esten afiliados
create or replace view vista9 as
select 
b.customer_id as cliente,
b.affiliate as fidelidad,
d.bank as banco,
count(d.bank)
from customer as b
join sale d on b.customer_id = d.customer_id
where b.affiliate = 'true' 
group by b.customer_id, b.fidelity_lvl, d.bank
having count(d.bank) = 2