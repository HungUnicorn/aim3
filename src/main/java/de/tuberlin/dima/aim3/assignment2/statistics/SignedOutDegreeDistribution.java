/**
 * Graph-Mining Tutorial for Ozone
 *
 * Copyright (C) 2013  Sebastian Schelter <ssc@apache.org>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment2.statistics;

import com.google.common.collect.Iterables;

import de.tuberlin.dima.aim3.assignment2.Config;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class SignedOutDegreeDistribution {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

		/* Convert the input to edges, consisting of (source, target, isFriend ) */
		DataSet<Tuple3<Long, Long, Boolean>> edges = input
				.flatMap(new EdgeReader());

		DataSet<Tuple3<Long, Long, Boolean>> friendEdges = edges
				.filter(new FriendFilter());
		DataSet<Tuple3<Long, Long, Boolean>> foeEdges = edges
				.filter(new FoeFilter());

		/* Create a dataset of all vertex ids and count them */
		DataSet<Long> friendVertices = friendEdges.project(0).types(Long.class)// source
																				// node
				.union(friendEdges.project(1).types(Long.class))// target node
				.distinct().reduceGroup(new CountVertices());

		DataSet<Long> foeVertices = foeEdges.project(0).types(Long.class)// source
																			// node
				.union(foeEdges.project(1).types(Long.class))// target node
				.distinct().reduceGroup(new CountVertices());

		DataSet<Tuple2<Long, Long>> verticesWithFriendDegree = friendEdges
				.project(0).types(Long.class)// source node
				.groupBy(0).reduceGroup(new DegreeOfVertex());

		DataSet<Tuple2<Long, Long>> verticesWithFoeDegree = foeEdges.project(0)
				.types(Long.class)// source node
				.groupBy(0).reduceGroup(new DegreeOfVertex());

		/* Compute the degree distribution */
		DataSet<Tuple2<Long, Double>> friendDegreeDistribution = verticesWithFriendDegree
				.groupBy(1).reduceGroup(new DistributionElement())
				.withBroadcastSet(friendVertices, "numVertices");

		DataSet<Tuple2<Long, Double>> foeDegreeDistribution = verticesWithFoeDegree
				.groupBy(1).reduceGroup(new DistributionElement())
				.withBroadcastSet(foeVertices, "numVertices");

		friendDegreeDistribution.print();
		foeDegreeDistribution.print();		
		env.execute();
	}

	public static class EdgeReader implements
			FlatMapFunction<String, Tuple3<Long, Long, Boolean>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple3<Long, Long, Boolean>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				boolean isFriend = "+1".equals(tokens[2]);

				collector.collect(new Tuple3<Long, Long, Boolean>(source,
						target, isFriend));
			}
		}
	}

	public static class CountVertices implements
			GroupReduceFunction<Tuple1<Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> edges,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(edges)));
		}
	}

	public static class DegreeOfVertex implements
			GroupReduceFunction<Tuple1<Long>, Tuple2<Long, Long>> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> tuples,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Iterator<Tuple1<Long>> iterator = tuples.iterator();
			Long vertexId = iterator.next().f0;

			long count = 1L;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}

			collector.collect(new Tuple2<Long, Long>(vertexId, count));
		}
	}

	public static class DistributionElement extends
			RichGroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

		private long numVertices;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numVertices = getRuntimeContext().<Long> getBroadcastVariable(
					"numVertices").get(0);
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> verticesWithDegree,
				Collector<Tuple2<Long, Double>> collector) throws Exception {

			Iterator<Tuple2<Long, Long>> iterator = verticesWithDegree
					.iterator();
			Long degree = iterator.next().f1;

			long count = 1L;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}

			collector.collect(new Tuple2<Long, Double>(degree, (double) count
					/ numVertices));
		}
	}

	// FilterFunction for friend edge
	public static class FriendFilter implements
			FilterFunction<Tuple3<Long, Long, Boolean>> {

		@Override
		public boolean filter(Tuple3<Long, Long, Boolean> value)
				throws Exception {
			// TODO Auto-generated method stub
			return value.f2 == true;

		}
	}

	public static class FoeFilter implements
			FilterFunction<Tuple3<Long, Long, Boolean>> {

		@Override
		public boolean filter(Tuple3<Long, Long, Boolean> value)
				throws Exception {
			// TODO Auto-generated method stub
			return value.f2 == false;

		}
	}
}
