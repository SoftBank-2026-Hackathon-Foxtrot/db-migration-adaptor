\set sku random(1, 1024)
BEGIN;
WITH held AS (
  UPDATE inventory SET stock = stock - 1
  WHERE sku = 'sku-' || :sku AND stock > 0 RETURNING sku
)
INSERT INTO orders(request_id, sku) SELECT gen_random_uuid(), sku FROM held;
ROLLBACK;
