CREATE KEYSPACE lambda WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

CREATE TABLE lambda.stream_activity_by_product (
product text,
timestamp_hour bigint,
purchase_count bigint,
add_to_cart_count bigint,
page_view_count bigint,
PRIMARY KEY (product, timestamp_hour)
) WITH CLUSTERING ORDER BY (timestamp_hour DESC);

CREATE TABLE lambda.stream_visitors_by_product (
product text,
timestamp_hour bigint,
unique_visitors bigint,
PRIMARY KEY (product, timestamp_hour)
) WITH CLUSTERING ORDER BY (timestamp_hour DESC);

CREATE TABLE lambda.batch_activity_by_product (
product text,
timestamp_hour bigint,
purchase_count bigint,
add_to_cart_count bigint,
page_view_count bigint,
PRIMARY KEY (product, timestamp_hour)
) WITH CLUSTERING ORDER BY (timestamp_hour DESC);

CREATE TABLE lambda.batch_visitors_by_product (
product text,
timestamp_hour bigint,
unique_visitors bigint,
PRIMARY KEY (product, timestamp_hour)
) WITH CLUSTERING ORDER BY (timestamp_hour DESC);