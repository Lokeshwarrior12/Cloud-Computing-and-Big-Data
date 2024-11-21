import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Trip {

    public static class TripMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("VendorID")) {
                return;
            }
            if (Character.isDigit(value.toString().charAt(0))) {
                String[] assign = value.toString().split(",");
                double totalAmount = Double.parseDouble(assign[assign.length - 1]);
                double distance = Double.parseDouble(assign[4]);
                int roundoffDistance = (int) Math.round(distance);
                if (roundoffDistance < 200) {
                    context.write(new IntWritable(roundoffDistance), new DoubleWritable(totalAmount));
                }
            }
        }
    }

    public static class TripReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0.0;
            double count = 0;

            // Calculate total and count
            for (DoubleWritable value : values) {
                total += value.get();
                count++;
            }

            if (key.get() < 0) { // Check for negative keys, though your logic may not need this
                return;
            }

            double avg = total / count;
            if (avg > 0) {
                context.write(key, new DoubleWritable(avg));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TripAnalysisJob");
        job.setJarByClass(Trip.class); // Corrected to Trip.class
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(TripMapper.class); // Corrected to TripMapper.class
        job.setReducerClass(TripReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
