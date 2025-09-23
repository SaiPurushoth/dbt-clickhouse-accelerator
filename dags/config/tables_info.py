tables = [
    {
        "table_name": "raw_franchise",
        "s3_path": "franchise",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_franchise (
          franchise_id UInt64,
          first_name String,
          last_name String,
          city String,
          country String,
          e_mail String,
          phone_number String,
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (franchise_id)
      PARTITION BY intDiv(franchise_id, 1000)
  """,
    },
    {
        "table_name": "raw_country",
        "s3_path": "country",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_country (
          country_id UInt64,
          country String,
          iso_currency String,
          iso_country String,
          city_id UInt64,
          city String,
          city_population UInt64,
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (country_id)
      PARTITION BY intDiv(country_id, 1000)
  """,
    },
    {
        "table_name": "raw_customer_loyalty",
        "s3_path": "customer_loyalty",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_customer_loyalty (
          customer_id UInt64,
          first_name String,
          last_name String,
          city String,
          country String,
          postal_code String,
          preferred_language String,
          gender LowCardinality(String),
          favourite_brand String,
          marital_status LowCardinality(String),
          children_count UInt8,
          sign_up_date Date,
          birthday_date Date,
          e_mail String,
          phone_number String,
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (customer_id, sign_up_date)
      PARTITION BY toYYYYMM(sign_up_date)
  """,
    },
    {
        "table_name": "raw_truck",
        "s3_path": "truck",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_truck (
          truck_id UInt64,
          menu_type_id UInt32,
          primary_city String,
          region String,
          iso_region String,
          country String,
          iso_country_code String,
          franchise_flag UInt8,
          year UInt16,
          make String,
          model String,
          ev_flag UInt8,
          franchise_id UInt64,
          truck_opening_date Date,
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (truck_id, truck_opening_date)
      PARTITION BY toYYYYMM(truck_opening_date)
  """,
    },
    {
        "table_name": "raw_location",
        "s3_path": "location",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_location (
          location_id UInt64,
          placekey String,
          location String,
          city String,
          region String,
          iso_country_code String,
          country String,
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (location_id)
      PARTITION BY intDiv(location_id, 1000)
  """,
    },
    {
        "table_name": "raw_menu",
        "s3_path": "menu",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_menu (
          menu_id UInt64,
          menu_type_id UInt32,
          menu_type LowCardinality(String),
          truck_brand_name String,
          menu_item_id UInt64,
          menu_item_name String,
          item_category LowCardinality(String),
          item_subcategory LowCardinality(String),
          cost_of_goods_usd Decimal(10,2),
          sale_price_usd Decimal(10,2),
          menu_item_health_metrics_obj JSON,
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (menu_item_id)
      PARTITION BY intDiv(menu_item_id, 1000)
  """,
    },
    {
        "table_name": "raw_order_header",
        "s3_path": "order_header",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_order_header (
          order_id UInt64,
          truck_id UInt64,
          location_id UInt64,
          customer_id UInt64,
          discount_id String,
          shift_id UInt32,
          shift_start_time String,
          shift_end_time String,
          order_channel LowCardinality(String),
          order_ts DateTime,
          served_ts DateTime,
          order_currency String,
          order_amount Decimal(10,2),
          order_tax_amount Decimal(10,2),
          order_discount_amount Decimal(10,2),
          order_total Decimal(10,2),
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (order_id, order_ts)
      PARTITION BY toYYYYMM(order_ts)
  """,
    },
    {
        "table_name": "raw_order_detail",
        "s3_path": "order_detail",
        "file_format": "CSV",
        "schema": """
      CREATE TABLE IF NOT EXISTS raw_order_detail (
          order_detail_id UInt64,
          order_id UInt64,
          menu_item_id UInt64,
          discount_id String,
          line_number UInt16,
          quantity UInt8,
          unit_price Decimal(10,2),
          price Decimal(10,2),
          order_item_discount_amount Decimal(10,2),
          _ingestion_timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (order_detail_id, order_id)
      PARTITION BY intDiv(order_id, 1000)
  """,
    },
]
