/**
 * 
 */
package com.github.ansell.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.UnknownTransactionStateException;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.util.Repositories;

import scala.collection.JavaConversions;

/**
 * A method of accessing information for relations created by
 * {@link SparkRDF4JDefaultSource}.
 * 
 * This class is only public to expose both BaseRelation and TableScan together,
 * as the Scala Spark code has no common ancester for these two.
 * 
 * Instances of this class should only be created using
 * {@link SparkRDF4JDefaultSource#createRelation(SQLContext, scala.collection.immutable.Map)}
 * or
 * {@link SparkRDF4JDefaultSource#createRelation(SQLContext, scala.collection.immutable.Map, StructType)}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class SparkRDF4JSparqlRelation extends BaseRelation implements TableScan {

	private SQLContext sqlContextField;
	private String serviceField;
	private ParsedQuery queryField;
	private StructType schemaField;

	/**
	 * Constructor for a new {@link SparkRDF4JSparqlRelation} based on the given
	 * service, query, schema, and context.
	 * 
	 * @param service
	 *            The URL to the SPARQL service to be used for this query.
	 * @param parsedQuery
	 *            The preparsed SPARQL query.
	 * @param schema
	 *            The schema to use for the results of the query.
	 * @param sqlContext
	 *            The context for the query.
	 */
	SparkRDF4JSparqlRelation(String service, ParsedQuery parsedQuery, StructType schema, SQLContext sqlContext) {
		this.serviceField = Objects.requireNonNull(service);
		this.queryField = Objects.requireNonNull(parsedQuery);
		this.schemaField = Optional.ofNullable(schema).orElseGet(() -> {
			// These bindings are guaranteed to be present and are not nullable
			Set<String> assuredBindingNames = this.queryField.getTupleExpr().getAssuredBindingNames();
			// If bindings are only in the following they are nullable
			StructType result = new StructType();
			this.queryField.getTupleExpr().getBindingNames().forEach(binding -> {
				result.add(binding, DataTypes.StringType, !(assuredBindingNames.contains(binding)));
			});
			return result;
		});
		this.sqlContextField = sqlContext;
	}

	@Override
	public RDD<Row> buildScan() {
		SPARQLRepository repository = new SPARQLRepository(serviceField);
		try {
			repository.initialize();

			List<Row> rows = tupleQueryModifiedToWorkWithVirtuoso(repository, queryField.getSourceString(), tuple -> {
				List<Row> result = new ArrayList<>();
				while (tuple.hasNext()) {
					result.add(convertTupleResultToRow(tuple.getBindingNames(), tuple.next(), this.schemaField));
				}
				return result;
			});

			// The unmapped spark context must be closed by the owner of this
			// class when it isn't needed, this is just created to allow us to
			// use the scala code in java
			@SuppressWarnings("resource")
			JavaSparkContext sc = new JavaSparkContext(sqlContext().sparkContext());
			return sc.parallelize(rows).rdd();
		} finally {
			repository.shutDown();
		}
	}

	private static <T> T getModifiedToWorkWithVirtuoso(Repository repository,
			Function<RepositoryConnection, T> processFunction) throws RepositoryException {
		RepositoryConnection conn = null;

		try {
			conn = repository.getConnection();
			T result = processFunction.apply(conn);
			return result;
		} finally {
			if (conn != null && conn.isOpen()) {
				conn.close();
			}
		}
	}

	private static <T> T tupleQueryModifiedToWorkWithVirtuoso(Repository repository, String query,
			Function<TupleQueryResult, T> processFunction) throws RepositoryException, UnknownTransactionStateException,
			MalformedQueryException, QueryEvaluationException {
		return getModifiedToWorkWithVirtuoso(repository, conn -> {
			TupleQuery preparedQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
			try (TupleQueryResult queryResult = preparedQuery.evaluate();) {
				return processFunction.apply(queryResult);
			}
		});
	}

	private static Row convertTupleResultToRow(List<String> bindingNames, BindingSet tuple, StructType schema) {
		List<StructField> schemaAsList = JavaConversions.asJavaList(schema.toList());

		String[] resultArray = new String[schemaAsList.size()];
		for (int i = 0; i < schemaAsList.size(); i++) {
			resultArray[i] = tuple.getBinding(schemaAsList.get(i).name()).getValue().stringValue();
		}

		org.apache.spark.sql.catalyst.expressions.GenericRow testGeneric = new GenericRow(resultArray);
		return testGeneric;
	}

	@Override
	public StructType schema() {
		return this.schemaField;
	}

	@Override
	public SQLContext sqlContext() {
		return this.sqlContextField;
	}

}
