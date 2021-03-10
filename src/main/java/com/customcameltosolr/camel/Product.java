package com.customcameltosolr.camel;

import org.apache.solr.client.solrj.beans.Field;

public class Product
{
    @Field
    private String name;

    @Field
    private String productId;

    @Field
    private String sku;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Product{");
        sb.append("name='").append(name).append('\'');
        sb.append(", productId='").append(productId).append('\'');
        sb.append(", sku='").append(sku).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
