import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

	public static class MatrixMultiplicationMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outKey;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			StringTokenizer itr = new StringTokenizer(value.toString());
			String[] elements = new String[itr.countTokens()];
			int index = 0;
			/* Parsing String Tokens and storing them in a String Array */
			while (itr.hasMoreTokens()) {
				elements[index++] = itr.nextToken();
			}

			/* Fetch the max row and column values for the input Matrices */
			int Max_i = Integer.parseInt(conf.get("i"));
			int Max_j = Integer.parseInt(conf.get("j"));
			int Max_k = Integer.parseInt(conf.get("k"));

			/*
			 * Iterate through each String in array and send key-value pairs to
			 * reducer
			 * 
			 * For Matrix 1, Key (i, k), Value = (M, j, mij) -> k = 0,1,2,3,...
			 * For Matrix 2, Key (i, k), Value = (N, j, njk) -> i = 0,1,2,3,...
			 */

			if (elements[0].equals("M")) {
				String i_val = new String(elements[1]);
				String j_val = new String(elements[2]);
				String matrixValue = new String(elements[3]);
				for (int k = 0; k <= Max_k; k++) {
					outKey = new Text(i_val + "," + String.valueOf(k));
					context.write(outKey, new Text("M" + "," + j_val + ","
							+ matrixValue));
				}
			} else {
				String j_val = new String(elements[1]);
				String k_val = new String(elements[2]);
				String matrixValue = new String(elements[3]);
				for (int i = 0; i <= Max_i; i++) {
					outKey = new Text(String.valueOf(i) + "," + k_val);
					context.write(outKey, new Text("N" + "," + j_val + ","
							+ matrixValue));
				}
			}

		}

	}

	public static class MatrixMultiplicationReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int Max_j = Integer.parseInt(conf.get("j"));
			int MatArray1[] = new int[Max_j + 1];
			int MatArray2[] = new int[Max_j + 1];

			for (Text val : values) {

				String[] InpValues = val.toString().split(",");
				String matrixId = "";
				int ArrIdx = 0;
				int MatrixVal = 0;
//				try {
					matrixId = InpValues[0];
					ArrIdx = Integer.parseInt(InpValues[1]);
					MatrixVal = Integer.parseInt(InpValues[2]);
//				} catch (Exception e) {
					// continue;
//				}

				if (matrixId.equals("M")) {
					MatArray1[ArrIdx] = MatrixVal;
				} else if (matrixId.equals("N")) {
					MatArray2[ArrIdx] = MatrixVal;
				}

			}

			/* Multiply and Add the products */
			int sum = 0;
			for (int i = 0; i < MatArray1.length; i++) {
				sum += MatArray1[i] * MatArray2[i];
			}

			result.set(String.valueOf(sum));
			context.write(key, result);
		}
	}

	public static class MatrixConfiguration {
		private static int m1_row = Integer.MIN_VALUE;
		private static int m1_col = Integer.MIN_VALUE;
		private static int m2_row = Integer.MIN_VALUE;
		private static int m2_col = Integer.MIN_VALUE;

		public static void inputMatrixConfig() throws FileNotFoundException,
				IOException {

			BufferedReader bufferedReader = new BufferedReader(
					new FileReader(
							"/home/cloudera/workspace/CIS5570Assignment3MM/input/matrix"));
			String str;
			while ((str = bufferedReader.readLine()) != null) {
				String[] Matrix = str.split(" ");
				if (Matrix[0].equals("M")) {
					m1_row = Math.max(m1_row, Integer.parseInt(Matrix[1]));
					m1_col = Math.max(m1_col, Integer.parseInt(Matrix[2]));
				} else {
					m2_row = Math.max(m2_row, Integer.parseInt(Matrix[1]));
					m2_col = Math.max(m2_col, Integer.parseInt(Matrix[2]));
				}
				
			}
			if (m1_col != m2_row) {
				System.out
						.println("Mismatch in row and column values of Input Matrices");
				System.exit(1);
			}

		}

		public int getMaxI() {
			return m1_row;
		}

		public int getMaxJ() {
			return m1_col;
		}

		public int getMaxK() {
			return m2_col;
		}

	}

	public static void main(String[] args) throws Exception {
		MatrixConfiguration matrixConfiguration = new MatrixConfiguration();
		MatrixConfiguration.inputMatrixConfig();

		Configuration conf = new Configuration();
		conf.setInt("i", matrixConfiguration.getMaxI());
		conf.setInt("j", matrixConfiguration.getMaxJ());
		conf.setInt("k", matrixConfiguration.getMaxK());

		Job job = Job.getInstance(conf, "Matrix Multiplication");

		job.setJarByClass(MatrixMultiplication.class);

		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.setMapperClass(MatrixMultiplicationMapper.class);
		// job.setCombinerClass(MatrixMultiplicationReducer.class);
		job.setReducerClass(MatrixMultiplicationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}