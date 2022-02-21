-- SI EL CLIENTE QUE LLEGA ESTA AFILIADO SE LE AGREGA 1 A SU NIVEL DE FIDELIDAD
CREATE OR REPLACE FUNCTION update_fidelidad_afiliados() 
RETURNS TRIGGER AS $$
	BEGIN
	IF (SELECT affiliate FROM customer WHERE customer_id = NEW.customer_id) THEN
		UPDATE customer 
		SET fidelity_lvl = fidelity_lvl+1;
	END IF;
	RETURN NEW;
	END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_fidelidad AFTER INSERT ON entry_registry
FOR EACH ROW EXECUTE PROCEDURE update_fidelidad_afiliados();

----------------------------------------------------------------------------------
-- SI UN CLIENTE NO AFILIADO REALIZA SU 4ta COMPRA SE VUELVE AFILIADO
CREATE OR REPLACE FUNCTION update_estado_afiliado() 
RETURNS TRIGGER AS $$
DECLARE
	num_compras integer;
BEGIN
	-- GET NUM DE COMPRAS
	SELECT COUNT(customer_id)
	INTO num_compras
	FROM sale 
	WHERE customer_id=NEW.customer_id;
	-- SI HA COMPRADO MAS DE 4 VECES SE VUELVE AFILIADO
	IF (num_compras>4)THEN
		UPDATE customer
		SET affiliate = 'true'
		WHERE customer_id = NEW.customer_id;
	END IF;
	RETURN NEW;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_afiliado AFTER INSERT ON entry_registry
FOR EACH ROW EXECUTE PROCEDURE update_estado_afiliado();
--------------------------------------------------------------------------------------
-- CHECK EN LA ENTRADA A VER SI SE LES DEJA PASAR
-- SI EL CLIENTE TIENE MAS DE 40 DE FIEBRE SE AGREGA EL MENSAJE A LA BD
-- TAMBIEN CHEQUEA QUE EL NUM DE CLIENTES POR TIENDA NO EXEDA LOS 20
CREATE OR REPLACE FUNCTION control_entrada() 
RETURNS TRIGGER AS $$
DECLARE
	num_people integer;
BEGIN
	-- Cuenta cuanta gente hay en la tienda a la que se desea entrar
	SELECT count(customer_id)
	INTO num_people
	FROM entry_registry
	WHERE store_id = NEW.store_id AND exit_time = null;
	NEW.allowed_in:='true';
	-- Check de mascara temperatura y num de gente en tienda
	IF (NEW.body_temp >= 38 OR NEW.has_mask = 'false' OR num_people>=20) THEN 
		NEW.allowed_in := 'false';
		NEW.exit_time := NEW.arrival_time;
		-- Log llamada a ambulancia
		IF NEW.body_temp>=40 THEN
			INSERT INTO message(message,customer_id, date)
			VALUES('Llamar ambulancia',NEW.customer_id,NEW.arrival_time);
		END IF;
		-- La tienda esta full
		IF num_people>=20 THEN
			INSERT INTO message(message,customer_id, date)
			VALUES('Entrada negada, tienda full',NEW.customer_id,NEW.arrival_time);
		END IF;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_entrada BEFORE INSERT ON entry_registry
FOR EACH ROW EXECUTE PROCEDURE control_entrada();
--------------------------------------------------------------------------------------
-- DESCUENTO DE 5% A LOS CLIENTES AFILIADOS
CREATE OR REPLACE FUNCTION descuento_afiliado() 
RETURNS TRIGGER AS $$
BEGIN
	IF(SELECT affiliate FROM CUSTOMER WHERE customer_id = NEW.customer_id) THEN
		NEW.amount := NEW.amount*0.95;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_descuento BEFORE INSERT ON sale
FOR EACH ROW EXECUTE PROCEDURE descuento_afiliado();
---------------------------------------------------------------------------------------
-- CADA VEZ QUE UN ESTANTE BAJA DE 20% SE REFILEA Y SE RESTA DEL INVENTARIO
CREATE OR REPLACE FUNCTION update_inventario() 
RETURNS TRIGGER AS $$
DECLARE
	refill_limit integer;-- 20%
	replace_num integer;--num de items a reemplazar
BEGIN
	replace_num = NEW.max_quantity-NEW.quantity;
	refill_limit = NEW.max_quantity/5;
	IF(NEW.quantity<refill_limit) THEN
		-- Update al inventario
		UPDATE inventory
		SET quantity = quantity-replace_num
		WHERE store_id = NEW.store_id AND product_id = NEW.product_id;
		-- Update al estante
		NEW.quantity = NEW.max_quantity;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_inventario BEFORE UPDATE ON smart_shelf
FOR EACH ROW EXECUTE PROCEDURE update_inventario();
---------------------------------------------------------------------------------------
-- CHEQUEAR QUE AL MOMENTO DE SALIR EL CLIENTE HAYA PAGADO
CREATE OR REPLACE FUNCTION check_ladron() 
RETURNS TRIGGER AS $$
BEGIN
	-- Revisa que el usuario no tenga un carrito activo a la hora de salir
	-- El exit_time es null hasta que el cliente sale por la puerta
	IF(NEW.exit_time IS NOT NULL) THEN
		IF(SELECT 1 FROM active_cart WHERE customer_id = NEW.customer_id) THEN
			INSERT INTO message(message,customer_id,date)
			VALUES('Llamar policia por robo', NEW.customer_id, NEW.exit_time);
		END IF;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_ladron BEFORE UPDATE ON entry_registry
FOR EACH ROW EXECUTE PROCEDURE check_ladron();
---------------------------------------------------------------------------------------
-- UPDATEA LOS ESTANTES SEGUN LO QUE SE INGRESA EN LOS CARRITOS
CREATE OR REPLACE FUNCTION update_estante() 
RETURNS TRIGGER AS $$
DECLARE
	tienda integer;
BEGIN
	-- get store_id
	SELECT store_id
	INTO tienda
	FROM active_cart
	WHERE cart_id = NEW.cart_id;
	-- Si es un insert se le resta al estante
	-- Si es update es porque el cliente esta devolviendo el item
	IF OLD.quantity IS null THEN
		UPDATE smart_shelf
		SET quantity = quantity - NEW.quantity
		WHERE product_id = NEW.product_id AND
		store_id = tienda;
	ELSE
		UPDATE smart_shelf
		SET quantity = quantity + NEW.quantity
		WHERE product_id = NEW.product_id AND
		store_id = tienda;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_estantes BEFORE INSERT ON cart_content
FOR EACH ROW EXECUTE PROCEDURE update_estante();
---------------------------------------------------------------------------------------




