/**
 * 
 */
package com.github.ansell.spark;

import java.util.Map;
import java.util.Optional;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;

import scala.collection.JavaConversions;

/**
 * A source of information for Spark, using RDF4J SPARQL endpoints as sources.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class SparkRDF4JDefaultSource implements RelationProvider, SchemaRelationProvider, DataSourceRegister {
	@Override
	public String shortName() {
		return "spark-rdf4j-sparql";
	}

	@Override
	public SparkRDF4JSparqlRelation createRelation(SQLContext sqlContext,
			scala.collection.immutable.Map<String, String> scalaParameters, StructType schema) {
		Map<String, String> parameters = JavaConversions.asJavaMap(scalaParameters);
		String service = Optional.ofNullable(parameters.get("service")).orElseThrow(() -> new RuntimeException(
				"Spark RDF4J Sparql requires a SPARQL 'service' to be specified in the parameters"));

		try {
			String query = Optional.ofNullable(parameters.get("query")).orElseThrow(() -> new RuntimeException(
					"Spark RDF4J Sparql requires a 'query' to be specified in the parameters"));
			ParsedQuery parsedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null);
			if(!(parsedQuery instanceof ParsedTupleQuery)) {
				throw new RuntimeException("Spark RDF4J can only be used with Tuple (Select) queries right now.");
			}
			return new SparkRDF4JSparqlRelation(service, parsedQuery, schema, sqlContext);
		} catch (MalformedQueryException e) {
			throw new RuntimeException("Query was not valid SPARQL", e);
		}

	}

	@Override
	public SparkRDF4JSparqlRelation createRelation(SQLContext sqlContext,
			scala.collection.immutable.Map<String, String> parameters) {
		return createRelation(sqlContext, parameters, null);
	}

}
