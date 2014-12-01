package de.tuberlin.dima.aim3.assignment3;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.AbstractMap.SimpleEntry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

// Ref https://d396qusza40orc.cloudfront.net/datasci/lecture_slides/week3/021_map_reduce_matrix_multiply.pdf
// Ref http://importantfish.com/two-step-matrix-multiplication-with-hadoop/
public class MatrixMultiplication {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// Input: (Matrix, rowID, colID, value)
		DataSource<String> input = env
				.readTextFile("/home/hung/aim3/src/test/resources/assignment3/test.matrix");

		// For each element (i,j) of A, emit (j, A[i,j]) for k in 1..N
		// For each element (j,k) of B, emit ((j, B[j,k]) for i in 1..L
		DataSet<Tuple4<Integer, String, Integer, Double>> matrix = input
				.flatMap(new MatrixMapper());

		// Key = j, value = (A[i,j] * B[j,k]) = rowA[j] * colB[j]
		DataSet<Tuple3<Integer, Integer, Double>> vectorMultiply = matrix
				.groupBy(0).reduceGroup(new VectorMultiply());

		// Key = (i,k), value = sum([A[i,j] * B[j,k])
		DataSet<Tuple3<Integer, Integer, Double>> vectorSum = vectorMultiply
				.groupBy(0, 1).sum(2);

		vectorSum.print();

		env.execute();
	}

	public static class MatrixMapper implements
			FlatMapFunction<String, Tuple4<Integer, String, Integer, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple4<Integer, String, Integer, Double>> collector)
				throws Exception {

			String[] indicesAndValue = SEPARATOR.split(s);
			String matrix = indicesAndValue[0];
			int rowID = Integer.parseInt(indicesAndValue[1]);
			int colID = Integer.parseInt(indicesAndValue[2]);
			Double value = Double.parseDouble(indicesAndValue[3]);

			if (matrix.equals("A"))
				collector.collect(new Tuple4<Integer, String, Integer, Double>(
						colID, matrix, rowID, value));
			else
				collector.collect(new Tuple4<Integer, String, Integer, Double>(
						rowID, matrix, colID, value));
		}
	}

	public static class VectorMultiply
			extends
			RichGroupReduceFunction<Tuple4<Integer, String, Integer, Double>, Tuple3<Integer, Integer, Double>> {

		@Override
		public void reduce(
				Iterable<Tuple4<Integer, String, Integer, Double>> values,
				Collector<Tuple3<Integer, Integer, Double>> out)
				throws Exception {

			ArrayList<Entry<Integer, Double>> listA = new ArrayList<Entry<Integer, Double>>();
			ArrayList<Entry<Integer, Double>> listB = new ArrayList<Entry<Integer, Double>>();

			for (Tuple4<Integer, String, Integer, Double> value : values) {
				if (value.f1.equals("A"))
					listA.add(new SimpleEntry<Integer, Double>(value.f2,
							value.f3));
				else
					listB.add(new SimpleEntry<Integer, Double>(value.f2,
							value.f3));
			}

			Integer i;
			Double a_ij;
			Integer k;
			Double b_jk;

			for (Entry<Integer, Double> a : listA) {
				i = a.getKey();
				a_ij = a.getValue();
				for (Entry<Integer, Double> b : listB) {
					k = b.getKey();
					b_jk = b.getValue();
					// rowA[i] * colB[k]
					out.collect(new Tuple3<Integer, Integer, Double>(i, k, a_ij
							* b_jk));
				}
			}
		}
	}
}
