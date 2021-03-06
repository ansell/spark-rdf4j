package com.github.ansell.spark;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import scala.collection.JavaConversions;

public class SparkRDF4JDefaultSourceTest {

	private static SQLContext sqlContext;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		sqlContext = new SQLContext(new SparkContext("local[2]", "SparkRDF4JDefaultSourceTest"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		sqlContext.sparkContext().stop();
	}

	private String testQuery;
	private String testServer;

	@Before
	public void setUp() throws Exception {
		testQuery = "select DISTINCT ?s where { GRAPH <http://bio2rdf.org/hgnc_resource:bio2rdf.dataset.hgnc.R3> { ?s a <http://bio2rdf.org/hgnc.symbol_vocabulary:Resource> . } } LIMIT 1000";
		testServer = "http://hgnc.bio2rdf.org/sparql";
	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public final void testShortName() {
		assertEquals("spark-rdf4j-sparql", new SparkRDF4JDefaultSource().shortName());
	}

	@Test
	public final void testCreateRelationSQLContextMapOfStringStringStructType() {
		
		System.out.println("Starting test body");
		
		Map<String, String> parameters = new HashMap<>();

		parameters.put("service", testServer);
		parameters.put("query", testQuery);

		scala.collection.immutable.Map<String, String> scalaParameters = HelpScalaCope.getScalaImmutableMap(parameters);

		SparkRDF4JSparqlRelation createRelation = new SparkRDF4JDefaultSource().createRelation(sqlContext,
				scalaParameters);
		assertNotNull(createRelation);
		StructType schema = createRelation.schema();
		assertNotNull(schema);
		String[] fieldNames = schema.fieldNames();
		assertEquals(1, fieldNames.length);

		long startRDD = System.nanoTime();
		JavaRDD<Row> myRDD = createRelation.buildScan().toJavaRDD().persist(StorageLevel.MEMORY_ONLY());
		System.out.println("Time required to build RDD: " + (System.nanoTime() - startRDD)/1_000_000);
		long startCount = System.nanoTime();
		assertEquals(1000, myRDD.count());
		System.out.println("Time required to count: " + (System.nanoTime() - startCount)/1_000_000);

		myRDD.foreach(r -> {
			System.out.println(r);
			String uri = r.getString(0);
			assertFalse("Found an empty result", uri.trim().isEmpty());
			assertTrue("Found unexpected result: " + uri, uri.startsWith("http://bio2rdf.org/hgnc.symbol:"));
		});
		
		System.out.println("Returning from test body");
	}

	@Ignore("TODO: Implement me")
	@Test
	public final void testCreateRelationSQLContextMapOfStringString() {
		fail("Not yet implemented"); // TODO
	}

}
