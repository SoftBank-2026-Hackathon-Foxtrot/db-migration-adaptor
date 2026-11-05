\set sku random(1, 1024)
SELECT stock FROM inventory WHERE sku = 'sku-' || :sku;
