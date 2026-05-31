CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key TEXT PRIMARY KEY,
    source_customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_key TEXT PRIMARY KEY,
    source_seller_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key TEXT PRIMARY KEY,
    source_product_id INTEGER,
    name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    quantity INTEGER,
    pet_category TEXT,
    weight NUMERIC(12, 2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3, 1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id TEXT PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key TEXT PRIMARY KEY,
    source_sale_id INTEGER,
    sale_date DATE,
    customer_key TEXT,
    seller_key TEXT,
    product_key TEXT,
    store_id TEXT,
    supplier_id TEXT,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2)
);
