import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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


public class MatrixAddition {

	public static class MatrixAdditionMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String[] elements = new String[itr.countTokens()];

			int index = 0;
			/* Parsing String Tokens and storing them in a String Array */
			while (itr.hasMoreTokens()) {
				elements[index++] = itr.nextToken();
			}

			/*
			 * Iterate through each String in array and send its index and value
			 * as key-value pairs to reducer
			 */
			for (int i = 0; i < elements.length; i++) {
				StringBuilder sb = new StringBuilder();
				sb.append(elements[++i]);
				sb.append(",");
				sb.append(elements[++i]);
				IntWritable matrixValue = new IntWritable(
						Integer.parseInt(elements[++i]));

				word.set(sb.toString().trim());
				context.write(word, matrixValue);
			}
		}

	}

	public static class MatrixAdditionReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			/* Add the values for a given key */
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
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

			BufferedReader br = new BufferedReader(
					new FileReader(
							"/home/cloudera/workspace/CIS5570Assignment3MA/input/MatrixAdd"));
			String line;
			while ((line = br.readLine()) != null) {
				String[] Matrix = line.split(" ");
				if (Matrix[0].equals("M")) {
					m1_row = Math.max(m1_row, Integer.parseInt(Matrix[1]));
					m1_col = Math.max(m1_col, Integer.parseInt(Matrix[2]));
				} else {
					m2_row = Math.max(m2_row, Integer.parseInt(Matrix[1]));
					m2_col = Math.max(m2_col, Integer.parseInt(Matrix[2]));
				}

			}
			/* Check if the two matrices are identical */
			if (m1_row != m2_row && m1_col != m2_col) {
				System.out
						.println("Mismatch in row and column values of Input Matrices");
				System.exit(1);
			}

		}

	}

	public static void main(String[] args) throws Exception {
		
		MatrixConfiguration.inputMatrixConfig();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Addition");

		job.setJarByClass(MatrixAddition.class);

		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.setMapperClass(MatrixAdditionMapper.class);
		job.setCombinerClass(MatrixAdditionReducer.class);
		job.setReducerClass(MatrixAdditionReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
