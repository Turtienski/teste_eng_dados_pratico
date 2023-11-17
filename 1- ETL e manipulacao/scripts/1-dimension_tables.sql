# this queries don't update the row (just ignore if they have the same id)
INSERT IGNORE INTO product
SELECT product_id,
	null as description
FROM stage
GROUP BY product_id;
INSERT IGNORE INTO seller
SELECT seller_id,
	null as name,
	null as phone,
	null as birth_date,
	null as address
FROM stage
GROUP BY seller_id;
INSERT IGNORE INTO currency
SELECT null as currency_id,
	currency,
	null as to_dolar
FROM stage
GROUP BY currency;
