package com.github.ansell.spark;

import java.util.Map;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Help Scala cope with Java collections since they don't provide any way to get
 * immutable maps from Java code.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class HelpScalaCope {

	public static <A, B> scala.collection.immutable.Map<A, B> getScalaImmutableMap(Map<A, B> m) {
		return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>> conforms());
	}
}
