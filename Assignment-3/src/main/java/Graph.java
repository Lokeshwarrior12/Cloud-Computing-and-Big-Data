import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;
    public long group;
    public long VertexID;
    public long[] adj;

    public Vertex() {}

    public Vertex(long group, long VertexID, long[] adj) {
        this.tag = 0;
        this.group = group;
        this.VertexID = VertexID;
        this.adj = adj;
    }

    public Vertex(long group) {
        this.tag = 1;
        this.group = group;
        this.VertexID = group;
        this.adj = new long[0];
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeShort(tag);
        output.writeLong(group);
        output.writeLong(VertexID);
        output.writeInt(adj.length);
        int i = 0;
    while (i < adj.length) {
        output.writeLong(adj[i++]);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tag = input.readShort();
        group = input.readLong();
        VertexID = input.readLong();
        int len = input.readInt();
        adj = new long[len];
        int i = 0;
    while (i < len) {
        adj[i++] = input.readLong();
        }
    }

    @Override
    public String toString() {
        return group + "\t" + VertexID;
    }
}

public class Graph {

    public static class MapperOne extends Mapper<LongWritable, Text, LongWritable, Vertex> {

        @Override
        public void map(LongWritable key, Text inputVal, Context context) throws IOException, InterruptedException {
            try {
                StringTokenizer strings = new StringTokenizer(inputVal.toString(), ",");
                long VertexID = Long.parseLong(strings.nextToken());
                long[] adj = new long[strings.countTokens()];
                int i = 0;
                while (strings.hasMoreTokens()) {
                    adj[i++] = Long.parseLong(strings.nextToken());
                }
                context.write(new LongWritable(VertexID), new Vertex(VertexID, VertexID, adj));
            } catch (IOException | InterruptedException e) {
                System.out.println("ERROR: Mapper for Job1");
                e.printStackTrace();
            }
        }
    }

    public static class ReducerOne extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void reduce(LongWritable value, Iterable<Vertex> vertex, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Vertex> iterator = vertex.iterator();
                while (iterator.hasNext()) {
                    Vertex vert = iterator.next();
                    context.write(value, vert);
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("ERROR: REDUCER for Job1");
                e.printStackTrace();
            }
        }
    }

    public static class MapperTwo extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException {
            try {
                context.write(new LongWritable(vertex.VertexID), vertex);
                
                Iterator<Long> iterator = Arrays.stream(vertex.adj).iterator();
                while (iterator.hasNext()) {
                    long vert = iterator.next();
                    context.write(new LongWritable(vert), new Vertex(vertex.group));
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("ERROR: MAPPER for Job2");
                e.printStackTrace();
            }
        }
    }

    public static class ReducerTwo extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void reduce(LongWritable value, Iterable<Vertex> vertex, Context context) throws IOException, InterruptedException {
            try {
                long maxValue = Long.MAX_VALUE;
                long[] adj = new long[0];
                
                Iterator<Vertex> iterator = vertex.iterator();
                while (iterator.hasNext()) {
                    Vertex vert = iterator.next();
                    if (vert.tag == 0) {
                        adj = vert.adj.clone();
                    }
                    maxValue = Math.min(maxValue, vert.group);
                }
                context.write(new LongWritable(maxValue), new Vertex(maxValue, value.get(), adj));
            } catch (IOException | InterruptedException e) {
                System.out.println("ERROR: REDUCER for Job2");
                e.printStackTrace();
            }
        }
    }

    public static class MapperThree extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {

        @Override
        public void map(LongWritable value, Vertex vertex, Context context) throws IOException, InterruptedException {
            try {
                context.write(new LongWritable(vertex.group), new IntWritable(1));
            } catch (IOException | InterruptedException e) {
                System.out.println("ERROR: MAPPER for Job3");
                e.printStackTrace();
            }
        }
    }

    public static class ReducerThree extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable> {

        @Override
        public void reduce(LongWritable value, Iterable<IntWritable> vertex, Context context) throws IOException, InterruptedException {
            try {
                long maxValue = 0;
                Iterator<IntWritable> iterator = vertex.iterator();
                while (iterator.hasNext()) {
                    IntWritable vert = iterator.next();
                    maxValue += vert.get();
                }
                context.write(value, new LongWritable(maxValue));
            } catch (IOException | InterruptedException e) {
                System.out.println("ERROR: REDUCER for Job3");
                e.printStackTrace();
            }
        }
    }

    public static void main ( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJarByClass(Graph.class);
        job1.setMapperClass(MapperOne.class);
        job1.setReducerClass(ReducerOne.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.waitForCompletion(true);

        for ( short i = 0; i < 5; i++ ) {
            Job job2 = Job.getInstance(conf, "Job2 " + (i + 1));
            job2.setJarByClass(Graph.class);
            job2.setMapperClass(MapperTwo.class);
            job2.setReducerClass(ReducerTwo.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));
            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance(conf, "Job3");
        job3.setJarByClass(Graph.class);
        job3.setMapperClass(MapperThree.class);
        job3.setReducerClass(ReducerThree.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.waitForCompletion(true);
    }
}