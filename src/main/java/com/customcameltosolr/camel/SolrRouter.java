package com.customcameltosolr.camel;

import static java.util.stream.Collectors.toList;
import static org.apache.camel.Exchange.CONTENT_TYPE;
import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_QUERY;
import static org.apache.camel.Exchange.HTTP_URI;
import static org.apache.camel.builder.PredicateBuilder.and;
import static org.apache.camel.builder.PredicateBuilder.in;
import static org.apache.camel.builder.PredicateBuilder.not;
import static org.apache.camel.builder.PredicateBuilder.or;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_EVENT_TYPE;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_RESOURCE_TYPE;
import static org.fcrepo.camel.FcrepoHeaders.FCREPO_URI;
import static org.fcrepo.camel.processor.ProcessorUtils.tokenizePropertyPlaceholder;
import static org.slf4j.LoggerFactory.getLogger;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.fcrepo.camel.processor.EventProcessor;
import org.slf4j.Logger;

/**
 * A content router for handling JMS events.
 *
 * @author Aaron Coburn
 */
public class SolrRouter extends RouteBuilder {

    // In the event of failure, the maximum number of times a redelivery will be attempted.
    final int maxRedeliveries = 10;

    // The camel URI for the incoming message stream.
    final String inputStream = "jms:topic:fedora";

    // If would like the `indexing:hasIndexingTransformation` property to be checked
    // on a per-object basis, set this to true. Otherwise, the `fcrepo.defaultTransform`
    // is always used as transformation URL even if an object has the
    // `indexing:hasIndexingTransformation` property set.
    final Boolean fcrepoCheckHasIndexingTransformation = true;

    // If you would like to index only those objects with a type `indexing:Indexable`,
    // set this property to `true`
    final Boolean indexingPredicate = false;

    // The default `LDPath` transformation to use. This is overridden on a per-object
    // basis with the `indexing:hasIndexingTransformation` predicate unless
    // `fcrepo.checkHasIndexingTransformation` is false. This should be a public URL.
    final String fcrepoDefaultTransform = "http://0.0.0.0:8181/program";

    // The location of the LDPath service.
    final String ldpathServiceBaseUrl = "http://0.0.0.0:9086/ldpath";

    // The camel URI for handling reindexing events.
    final String solrReindexStream = "jms:queue:solr.reindex";

    // The baseUrl for the Solr server. If using Solr 4.x or better, the URL should include
    // the core name.
    final String solrBaseUrl = "http4://0.0.0.0:8983/solr/openaccess";

    // The timeframe (in milliseconds) within which new items should be committed to the solr index.
    final int solrCommitWithin = 10000;

    // A comma-delimited list of URIs to filter. That is, any Fedora resource that either
    // matches or is contained in one of the URIs listed will not be processed by the
    // fcrepo-indexing-solr application.
    final String filterContainers = "http://localhost:8080/rest/audit";

    final String fcRepoBaseUrl = "http://localhost:8080/rest";

    private static final Logger logger = getLogger(SolrRouter.class);

    private static final String hasIndexingTransformation =
            "(/rdf:RDF/rdf:Description/indexing:hasIndexingTransformation/@rdf:resource | " +
                    "/rdf:RDF/rdf:Description/indexing:hasIndexingTransformation/@rdf:about)[1]";

    private static final String RESOURCE_DELETION = "http://fedora.info/definitions/v4/event#ResourceDeletion";
    private static final String DELETE = "https://www.w3.org/ns/activitystreams#Delete";
    private static final String INDEXING_TRANSFORMATION = "CamelIndexingTransformation";
    private static final String INDEXABLE = "http://fedora.info/definitions/v4/indexing#Indexable";
    private static final String INDEXING_URI = "CamelIndexingUri";

    /**
     * Configure the message route workflow.
     */
    public void configure() throws Exception {
        final Namespaces ns = new Namespaces("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        ns.add("indexing", "http://fedora.info/definitions/v4/indexing#");
        ns.add("ldp", "http://www.w3.org/ns/ldp#");

        // A generic error handler (specific to this RouteBuilder)
        onException(Exception.class)
                .maximumRedeliveries(maxRedeliveries)
                .log("Index Routing Error: ${routeId}");

        // Starting route - getting data from activemq which fedora repo inserted
        from(inputStream)
                .routeId("FcrepoSolrRouter")
                .log("Getting data from the queue")
                .log("${body}")
                .log("-------------------------------------------------------------------------")
                .process(new EventProcessor())
                .choice()
                .when(
                    or(
                        header(FCREPO_EVENT_TYPE).contains(RESOURCE_DELETION),
                        header(FCREPO_EVENT_TYPE).contains(DELETE)
                    )
                )
                .to("direct:delete.solr")
                .otherwise()
                .to("direct:index.solr");


        // Based on an item's metadata, determine if it is indexable.
        from("direct:index.solr")
                .routeId("FcrepoSolrIndexer")
                .removeHeaders("CamelHttp*")
                .log("${headers}")
                .filter(not(in(tokenizePropertyPlaceholder(getContext(), filterContainers, ",").stream()
                        .map(uri -> or(
                                header(FCREPO_URI).startsWith(constant(uri + "/")),
                                header(FCREPO_URI).isEqualTo(constant(uri))))
                        .collect(toList()))))
                .choice()
                    .when(and(simple(String.valueOf(indexingPredicate != true)),
                            simple(String.valueOf(fcrepoCheckHasIndexingTransformation != true))))
                        .setHeader(INDEXING_TRANSFORMATION).simple(fcrepoDefaultTransform)
                        .to("direct:update.solr")
                    .otherwise()
                        .log("We are visiting fcrepo here")
                        .to("fcrepo:" + fcRepoBaseUrl + "?preferOmit=PreferContainment")
                        .setHeader(INDEXING_TRANSFORMATION).xpath(hasIndexingTransformation, String.class, ns)
                .choice()
                    .when(or(header(INDEXING_TRANSFORMATION).isNull(),
                        header(INDEXING_TRANSFORMATION).isEqualTo("")))
                        .setHeader(INDEXING_TRANSFORMATION).simple(fcrepoDefaultTransform).end()
                        .removeHeaders("CamelHttp*")
                .choice()
                    .when(or(simple(String.valueOf(indexingPredicate != true)),
                        header(FCREPO_RESOURCE_TYPE).contains(INDEXABLE)))
                        .to("direct:update.solr")
                    .otherwise()
                        .to("direct:delete.solr");

        // Handle update operations
        from("direct:update.solr").routeId("FcrepoSolrUpdater")
                .log(LoggingLevel.INFO, logger, "Indexing Solr Object ${header.CamelFcrepoUri}")
                .setBody(constant(null))
                .setHeader(INDEXING_URI).simple("${header.CamelFcrepoUri}")
                // Don't index the transformation itself
                .filter().simple("${header.CamelIndexingTransformation} != ${header.CamelIndexingUri}")
                .choice()
                .when(header(INDEXING_TRANSFORMATION).startsWith("http"))
                .log(LoggingLevel.INFO, logger,
                        "Fetching external LDPath program from ${header.CamelIndexingTransformation}")
                .to("direct:external.ldpath")
                .setHeader(HTTP_METHOD).constant("POST")
                .to("direct:transform.ldpath")
                .to("direct:send.to.solr")
                .when(or(header(INDEXING_TRANSFORMATION).isNull(), header(INDEXING_TRANSFORMATION).isEqualTo("")))
                .setHeader(HTTP_METHOD).constant("GET")
                .to("direct:transform.ldpath")
                .to("direct:send.to.solr")
                .otherwise()
                .log(LoggingLevel.INFO, logger, "Skipping ${header.CamelFcrepoUri}");

        // Handle re-index events
        from(solrReindexStream)
                .routeId("FcrepoSolrReindex")
                .to("direct:index.solr");

        // Remove an item from the solr index.
        from("direct:delete.solr").routeId("FcrepoSolrDeleter")
                .removeHeaders("CamelHttp*")
                .to("mustache:org/fcrepo/camel/indexing/solr/delete.mustache")
                .log(LoggingLevel.INFO, logger, "Deleting Solr Object ${headers[CamelFcrepoUri]}")
                .setHeader(HTTP_METHOD).constant("POST")
                .setHeader(CONTENT_TYPE).constant("application/json")
                .setHeader(HTTP_QUERY).simple("commitWithin=" + solrCommitWithin)
                .to(solrBaseUrl + "/update?useSystemProperties=true");

        from("direct:external.ldpath").routeId("FcrepoSolrLdpathFetch")
                .removeHeaders("CamelHttp*")
                .log("Some kind of external ldpath")
                .setHeader(HTTP_URI).header(INDEXING_TRANSFORMATION)
                .setHeader(HTTP_METHOD).constant("GET")
                .to("http4://localhost/ldpath");


        from("direct:transform.ldpath").routeId("FcrepoSolrTransform")
                .removeHeaders("CamelHttp*")
                .setHeader(HTTP_URI).simple(ldpathServiceBaseUrl)
                .setHeader(HTTP_QUERY).simple("context=${headers.CamelFcrepoUri}")
                .log("Sending request to ldpath here")
                .to("http4://localhost/ldpath")
                .process(exchange -> log.info("The response code is: {}", exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE)));

        // Send the transformed resource to Solr
        from("direct:send.to.solr").routeId("FcrepoSolrSend")
                .log(LoggingLevel.INFO, "Sending data to solr here")
                .streamCaching()
                .removeHeaders("CamelHttp*")
                .setHeader(HTTP_METHOD).constant("POST")
                .setHeader(HTTP_QUERY).simple("commitWithin=10000")
                .to(solrBaseUrl + "/update?useSystemProperties=true");
    }
}
