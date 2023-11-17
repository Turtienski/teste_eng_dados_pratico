INSERT IGNORE INTO sale
SELECT s.transaction_id,
	s.date,
	s.sale_value,
	s.product_id,
	s.seller_id,
	c.currency_id
FROM stage as s
JOIN currency as c
ON s.currency = c.currency