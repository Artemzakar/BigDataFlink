package ru.bigdataflink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class FlinkStarSchemaJob {
    private static final DateTimeFormatter CSV_DATE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        String topic = env("KAFKA_TOPIC", "sales-json");
        String groupId = env("FLINK_CONSUMER_GROUP", "flink-star-schema");
        String postgresUrl = env("POSTGRES_URL", "jdbc:postgresql://localhost:5432/salesdb");
        String postgresUser = env("POSTGRES_USER", "postgres");
        String postgresPassword = env("POSTGRES_PASSWORD", "postgres");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<SaleEvent> sales = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka sales source")
                .map(new SaleEventJsonMapper())
                .name("JSON to SaleEvent");

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(2000)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(postgresUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(postgresUser)
                .withPassword(postgresPassword)
                .build();

        sales.addSink(JdbcSink.sink(
                "INSERT INTO dim_customer (customer_key, source_customer_id, first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (customer_key) DO UPDATE SET source_customer_id = EXCLUDED.source_customer_id, first_name = EXCLUDED.first_name, " +
                        "last_name = EXCLUDED.last_name, age = EXCLUDED.age, email = EXCLUDED.email, country = EXCLUDED.country, " +
                        "postal_code = EXCLUDED.postal_code, pet_type = EXCLUDED.pet_type, pet_name = EXCLUDED.pet_name, pet_breed = EXCLUDED.pet_breed",
                FlinkStarSchemaJob::setCustomer,
                executionOptions,
                connectionOptions
        )).name("dim_customer sink");

        sales.addSink(JdbcSink.sink(
                "INSERT INTO dim_seller (seller_key, source_seller_id, first_name, last_name, email, country, postal_code) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (seller_key) DO UPDATE SET source_seller_id = EXCLUDED.source_seller_id, first_name = EXCLUDED.first_name, " +
                        "last_name = EXCLUDED.last_name, email = EXCLUDED.email, country = EXCLUDED.country, postal_code = EXCLUDED.postal_code",
                FlinkStarSchemaJob::setSeller,
                executionOptions,
                connectionOptions
        )).name("dim_seller sink");

        sales.addSink(JdbcSink.sink(
                "INSERT INTO dim_product (product_key, source_product_id, name, category, price, quantity, pet_category, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (product_key) DO UPDATE SET source_product_id = EXCLUDED.source_product_id, name = EXCLUDED.name, " +
                        "category = EXCLUDED.category, price = EXCLUDED.price, quantity = EXCLUDED.quantity, pet_category = EXCLUDED.pet_category, " +
                        "weight = EXCLUDED.weight, color = EXCLUDED.color, size = EXCLUDED.size, brand = EXCLUDED.brand, " +
                        "material = EXCLUDED.material, description = EXCLUDED.description, rating = EXCLUDED.rating, reviews = EXCLUDED.reviews, " +
                        "release_date = EXCLUDED.release_date, expiry_date = EXCLUDED.expiry_date",
                FlinkStarSchemaJob::setProduct,
                executionOptions,
                connectionOptions
        )).name("dim_product sink");

        sales.addSink(JdbcSink.sink(
                "INSERT INTO dim_store (store_id, name, location, city, state, country, phone, email) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (store_id) DO UPDATE SET name = EXCLUDED.name, location = EXCLUDED.location, city = EXCLUDED.city, " +
                        "state = EXCLUDED.state, country = EXCLUDED.country, phone = EXCLUDED.phone, email = EXCLUDED.email",
                FlinkStarSchemaJob::setStore,
                executionOptions,
                connectionOptions
        )).name("dim_store sink");

        sales.addSink(JdbcSink.sink(
                "INSERT INTO dim_supplier (supplier_id, name, contact, email, phone, address, city, country) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (supplier_id) DO UPDATE SET name = EXCLUDED.name, contact = EXCLUDED.contact, email = EXCLUDED.email, " +
                        "phone = EXCLUDED.phone, address = EXCLUDED.address, city = EXCLUDED.city, country = EXCLUDED.country",
                FlinkStarSchemaJob::setSupplier,
                executionOptions,
                connectionOptions
        )).name("dim_supplier sink");

        sales.addSink(JdbcSink.sink(
                "INSERT INTO fact_sales (sale_key, source_sale_id, sale_date, customer_key, seller_key, product_key, store_id, supplier_id, sale_quantity, sale_total_price) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (sale_key) DO UPDATE SET source_sale_id = EXCLUDED.source_sale_id, sale_date = EXCLUDED.sale_date, " +
                        "customer_key = EXCLUDED.customer_key, seller_key = EXCLUDED.seller_key, product_key = EXCLUDED.product_key, " +
                        "store_id = EXCLUDED.store_id, supplier_id = EXCLUDED.supplier_id, sale_quantity = EXCLUDED.sale_quantity, " +
                        "sale_total_price = EXCLUDED.sale_total_price",
                FlinkStarSchemaJob::setFactSale,
                executionOptions,
                connectionOptions
        )).name("fact_sales sink");

        env.execute("Kafka to PostgreSQL Star Schema");
    }

    private static void setCustomer(PreparedStatement statement, SaleEvent event) throws SQLException {
        setString(statement, 1, customerKey(event));
        setInteger(statement, 2, event.saleCustomerId);
        setString(statement, 3, event.customerFirstName);
        setString(statement, 4, event.customerLastName);
        setInteger(statement, 5, event.customerAge);
        setString(statement, 6, event.customerEmail);
        setString(statement, 7, event.customerCountry);
        setString(statement, 8, event.customerPostalCode);
        setString(statement, 9, event.customerPetType);
        setString(statement, 10, event.customerPetName);
        setString(statement, 11, event.customerPetBreed);
    }

    private static void setSeller(PreparedStatement statement, SaleEvent event) throws SQLException {
        setString(statement, 1, sellerKey(event));
        setInteger(statement, 2, event.saleSellerId);
        setString(statement, 3, event.sellerFirstName);
        setString(statement, 4, event.sellerLastName);
        setString(statement, 5, event.sellerEmail);
        setString(statement, 6, event.sellerCountry);
        setString(statement, 7, event.sellerPostalCode);
    }

    private static void setProduct(PreparedStatement statement, SaleEvent event) throws SQLException {
        setString(statement, 1, productKey(event));
        setInteger(statement, 2, event.saleProductId);
        setString(statement, 3, event.productName);
        setString(statement, 4, event.productCategory);
        setDecimal(statement, 5, event.productPrice);
        setInteger(statement, 6, event.productQuantity);
        setString(statement, 7, event.petCategory);
        setDecimal(statement, 8, event.productWeight);
        setString(statement, 9, event.productColor);
        setString(statement, 10, event.productSize);
        setString(statement, 11, event.productBrand);
        setString(statement, 12, event.productMaterial);
        setString(statement, 13, event.productDescription);
        setDecimal(statement, 14, event.productRating);
        setInteger(statement, 15, event.productReviews);
        setDate(statement, 16, event.productReleaseDate);
        setDate(statement, 17, event.productExpiryDate);
    }

    private static void setStore(PreparedStatement statement, SaleEvent event) throws SQLException {
        setString(statement, 1, storeId(event));
        setString(statement, 2, event.storeName);
        setString(statement, 3, event.storeLocation);
        setString(statement, 4, event.storeCity);
        setString(statement, 5, event.storeState);
        setString(statement, 6, event.storeCountry);
        setString(statement, 7, event.storePhone);
        setString(statement, 8, event.storeEmail);
    }

    private static void setSupplier(PreparedStatement statement, SaleEvent event) throws SQLException {
        setString(statement, 1, supplierId(event));
        setString(statement, 2, event.supplierName);
        setString(statement, 3, event.supplierContact);
        setString(statement, 4, event.supplierEmail);
        setString(statement, 5, event.supplierPhone);
        setString(statement, 6, event.supplierAddress);
        setString(statement, 7, event.supplierCity);
        setString(statement, 8, event.supplierCountry);
    }

    private static void setFactSale(PreparedStatement statement, SaleEvent event) throws SQLException {
        setString(statement, 1, saleKey(event));
        setInteger(statement, 2, event.id);
        setDate(statement, 3, event.saleDate);
        setString(statement, 4, customerKey(event));
        setString(statement, 5, sellerKey(event));
        setString(statement, 6, productKey(event));
        setString(statement, 7, storeId(event));
        setString(statement, 8, supplierId(event));
        setInteger(statement, 9, event.saleQuantity);
        setDecimal(statement, 10, event.saleTotalPrice);
    }

    private static void setString(PreparedStatement statement, int index, String value) throws SQLException {
        if (isBlank(value)) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    private static void setInteger(PreparedStatement statement, int index, String value) throws SQLException {
        Integer integerValue = toInteger(value);
        if (integerValue == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, integerValue);
        }
    }

    private static void setDecimal(PreparedStatement statement, int index, String value) throws SQLException {
        BigDecimal decimalValue = toDecimal(value);
        if (decimalValue == null) {
            statement.setNull(index, Types.NUMERIC);
        } else {
            statement.setBigDecimal(index, decimalValue);
        }
    }

    private static void setDate(PreparedStatement statement, int index, String value) throws SQLException {
        Date dateValue = toDate(value);
        if (dateValue == null) {
            statement.setNull(index, Types.DATE);
        } else {
            statement.setDate(index, dateValue);
        }
    }

    private static Integer toInteger(String value) {
        return isBlank(value) ? null : Integer.valueOf(value.trim());
    }

    private static BigDecimal toDecimal(String value) {
        return isBlank(value) ? null : new BigDecimal(value.trim());
    }

    private static Date toDate(String value) {
        return isBlank(value) ? null : Date.valueOf(LocalDate.parse(value.trim(), CSV_DATE_FORMAT));
    }

    private static String saleKey(SaleEvent event) {
        return stableId(
                event.id,
                event.saleDate,
                customerKey(event),
                sellerKey(event),
                productKey(event),
                storeId(event),
                supplierId(event),
                event.saleQuantity,
                event.saleTotalPrice
        );
    }

    private static String customerKey(SaleEvent event) {
        return stableId(
                event.saleCustomerId,
                event.customerFirstName,
                event.customerLastName,
                event.customerAge,
                event.customerEmail,
                event.customerCountry,
                event.customerPostalCode,
                event.customerPetType,
                event.customerPetName,
                event.customerPetBreed
        );
    }

    private static String sellerKey(SaleEvent event) {
        return stableId(
                event.saleSellerId,
                event.sellerFirstName,
                event.sellerLastName,
                event.sellerEmail,
                event.sellerCountry,
                event.sellerPostalCode
        );
    }

    private static String productKey(SaleEvent event) {
        return stableId(
                event.saleProductId,
                event.productName,
                event.productCategory,
                event.productPrice,
                event.productQuantity,
                event.petCategory,
                event.productWeight,
                event.productColor,
                event.productSize,
                event.productBrand,
                event.productMaterial,
                event.productDescription,
                event.productRating,
                event.productReviews,
                event.productReleaseDate,
                event.productExpiryDate
        );
    }

    private static String storeId(SaleEvent event) {
        return stableId(event.storeName, event.storeLocation, event.storeCity, event.storeCountry);
    }

    private static String supplierId(SaleEvent event) {
        return stableId(event.supplierName, event.supplierEmail, event.supplierPhone, event.supplierCountry);
    }

    private static String stableId(String... values) {
        String joined = String.join("|", sanitize(values));
        return UUID.nameUUIDFromBytes(joined.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static String[] sanitize(String[] values) {
        String[] result = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] == null ? "" : values[i].trim();
        }
        return result;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return isBlank(value) ? defaultValue : value;
    }

    private static class SaleEventJsonMapper implements MapFunction<String, SaleEvent>, Serializable {
        private transient ObjectMapper objectMapper;

        @Override
        public SaleEvent map(String value) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            return objectMapper.readValue(value, SaleEvent.class);
        }
    }
}
