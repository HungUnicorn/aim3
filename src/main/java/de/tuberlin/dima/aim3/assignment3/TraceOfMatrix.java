package de.tuberlin.dima.aim3.assignment3;

import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.operators.ProjectOperator.Projection;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import de.tuberlin.dima.aim3.assignment3.MatrixMultiplication.MatrixMapper;
import de.tuberlin.dima.aim3.assignment3.MatrixMultiplication.VectorMultiply;

//The trace of an nxn matrix A is the sum of the main diagonal entries. 
//tr(A) = a11+ a22 + a33 + ... + ann
public class TraceOfMatrix {

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

		// Key = j, value = (A[i,j] * B[j,i])
		DataSet<Tuple3<Integer, Integer, Double>> vectorAMultiplyBDiagonal = matrix
				.groupBy(0).reduceGroup(new VectorAMultiplyBDiagonal());

		DataSet<Tuple3<Integer, Integer, Double>> vectorBMultiplyABDiagonal = matrix
				.groupBy(2).reduceGroup(new VectorBMultiplyADiagonal());

		ProjectOperator<Tuple3<Integer, Integer, Double>, Tuple1<Double>> trace_AB = vectorAMultiplyBDiagonal
				.sum(2).project(2).types(Double.class);
		
		ProjectOperator<Tuple3<Integer, Integer, Double>, Tuple1<Double>> trace_BA = vectorBMultiplyABDiagonal
				.sum(2).project(2).types(Double.class);		

		trace_AB.print();
		trace_BA.print();

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

	public static class VectorAMultiplyBDiagonal
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

			// A X B
			for (Entry<Integer, Double> a : listA) {
				i = a.getKey();
				a_ij = a.getValue();
				for (Entry<Integer, Double> b : listB) {
					k = b.getKey();
					// For diagonal
					if (i == k) {
						b_jk = b.getValue();
						out.collect(new Tuple3<Integer, Integer, Double>(i, k,
								a_ij * b_jk));
					}
				}
			}
		}
	}	

	public static class VectorBMultiplyADiagonal
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
					listA.add(new SimpleEntry<Integer, Double>(value.f0,
							value.f3));
				else
					listB.add(new SimpleEntry<Integer, Double>(value.f0,
							value.f3));
			}

			Integer i;
			Double a_ij;
			Integer k;
			Double b_jk;

			// B X A
			for (Entry<Integer, Double> b : listB) {
				i = b.getKey();
				b_jk = b.getValue();
				for (Entry<Integer, Double> a : listA) {
					k = a.getKey();
					// For diagonal
					if (i == k) {
						a_ij = a.getValue();
						out.collect(new Tuple3<Integer, Integer, Double>(i, k,
								a_ij * b_jk));
					}
				}
			}
		}
	}
}
