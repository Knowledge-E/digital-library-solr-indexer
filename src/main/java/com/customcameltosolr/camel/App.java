package com.customcameltosolr.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.gson.GsonDataFormat;
import org.apache.camel.component.solr.SolrConstants;
import org.apache.camel.impl.DefaultCamelContext;

public class App
{
    public static void main( String[] args ) throws Exception
    {
        CamelContext context = new DefaultCamelContext();

        final GsonDataFormat gsonDataFormat = new GsonDataFormat();
        gsonDataFormat.setUnmarshalType(Products.class);

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception
            {
                from("file:data/solr?noop=true")
                        .log(LoggingLevel.INFO, "Processing file ${file:name}")
                        .unmarshal(gsonDataFormat)
                        .setBody().simple("${body.products}")
                        .split().body()
                        .log(LoggingLevel.INFO, "${body}")
                        .setHeader(SolrConstants.OPERATION, constant(SolrConstants.OPERATION_ADD_BEAN))
                        .to("solr://localhost:8984/solr/newcore")
                        .to("direct:commit");

                from("direct:commit")
                        .setHeader(SolrConstants.OPERATION, constant(SolrConstants.OPERATION_COMMIT))
                        .to("solr://localhost:8984/solr/newcore");
            }
        });

        context.start();
        Thread.sleep(10000);
        context.stop();
    }
}
