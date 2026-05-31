package ru.bigdataflink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SaleEvent implements Serializable {
    @JsonProperty("id")
    public String id;
    @JsonProperty("customer_first_name")
    public String customerFirstName;
    @JsonProperty("customer_last_name")
    public String customerLastName;
    @JsonProperty("customer_age")
    public String customerAge;
    @JsonProperty("customer_email")
    public String customerEmail;
    @JsonProperty("customer_country")
    public String customerCountry;
    @JsonProperty("customer_postal_code")
    public String customerPostalCode;
    @JsonProperty("customer_pet_type")
    public String customerPetType;
    @JsonProperty("customer_pet_name")
    public String customerPetName;
    @JsonProperty("customer_pet_breed")
    public String customerPetBreed;

    @JsonProperty("seller_first_name")
    public String sellerFirstName;
    @JsonProperty("seller_last_name")
    public String sellerLastName;
    @JsonProperty("seller_email")
    public String sellerEmail;
    @JsonProperty("seller_country")
    public String sellerCountry;
    @JsonProperty("seller_postal_code")
    public String sellerPostalCode;

    @JsonProperty("product_name")
    public String productName;
    @JsonProperty("product_category")
    public String productCategory;
    @JsonProperty("product_price")
    public String productPrice;
    @JsonProperty("product_quantity")
    public String productQuantity;
    @JsonProperty("pet_category")
    public String petCategory;
    @JsonProperty("product_weight")
    public String productWeight;
    @JsonProperty("product_color")
    public String productColor;
    @JsonProperty("product_size")
    public String productSize;
    @JsonProperty("product_brand")
    public String productBrand;
    @JsonProperty("product_material")
    public String productMaterial;
    @JsonProperty("product_description")
    public String productDescription;
    @JsonProperty("product_rating")
    public String productRating;
    @JsonProperty("product_reviews")
    public String productReviews;
    @JsonProperty("product_release_date")
    public String productReleaseDate;
    @JsonProperty("product_expiry_date")
    public String productExpiryDate;

    @JsonProperty("sale_date")
    public String saleDate;
    @JsonProperty("sale_customer_id")
    public String saleCustomerId;
    @JsonProperty("sale_seller_id")
    public String saleSellerId;
    @JsonProperty("sale_product_id")
    public String saleProductId;
    @JsonProperty("sale_quantity")
    public String saleQuantity;
    @JsonProperty("sale_total_price")
    public String saleTotalPrice;

    @JsonProperty("store_name")
    public String storeName;
    @JsonProperty("store_location")
    public String storeLocation;
    @JsonProperty("store_city")
    public String storeCity;
    @JsonProperty("store_state")
    public String storeState;
    @JsonProperty("store_country")
    public String storeCountry;
    @JsonProperty("store_phone")
    public String storePhone;
    @JsonProperty("store_email")
    public String storeEmail;

    @JsonProperty("supplier_name")
    public String supplierName;
    @JsonProperty("supplier_contact")
    public String supplierContact;
    @JsonProperty("supplier_email")
    public String supplierEmail;
    @JsonProperty("supplier_phone")
    public String supplierPhone;
    @JsonProperty("supplier_address")
    public String supplierAddress;
    @JsonProperty("supplier_city")
    public String supplierCity;
    @JsonProperty("supplier_country")
    public String supplierCountry;
}
