\set orderid random(1, 1000000)
BEGIN;
WITH cancelled AS (DELETE FROM orders WHERE id = :orderid RETURNING sku)
UPDATE inventory SET stock = stock + 1 FROM cancelled WHERE inventory.sku = cancelled.sku;
COMMIT;
