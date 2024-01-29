create database itau_data_egineer_test;

use itau_data_egineer_test;

create Table sales (
    transaction_id varchar(10),
    date varchar(10),
    product_id integer,
    seller_id integer,
    sale_value double,
    currency varchar(30),
    usd_sale_value double,
    primary key (transaction_id)
);
