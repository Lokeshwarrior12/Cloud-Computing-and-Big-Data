import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Block implements Writable {
    int rows;
    int columns;
    public double[][] data;

    public Block() {}

    public Block(int rows, int columns) {
        this.rows = rows;
        this.columns = columns;
        data = new double[rows][columns];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(columns);
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                out.writeDouble(data[i][j]);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rows = in.readInt();
        columns = in.readInt();
        data = new double[rows][columns];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                data[i][j] = in.readDouble();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                sb.append(String.format("\t%.3f", data[i][j]));
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
    
    public Pair() {}
    
    public Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    @Override
    public int compareTo(Pair o) {
        int cmp = Integer.compare(i, o.i);
        if (cmp == 0) {
            cmp = Integer.compare(j, o.j);
        }
        return cmp;
    }

    @Override
    public String toString() {
        return ""+ i + "\t" + j;
    }
}

class Triple implements Writable {
    public int i;
    public int j;
    public double value;
    
    public Triple() {}
    
    public Triple(int i, int j, double v) {
        this.i = i;
        this.j = j;
        value = v;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
        value = in.readDouble();
    }
}


public class Add {
    public static class ConvertMapper extends Mapper<Object, Text, Pair, Triple> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int rows = context.getConfiguration().getInt("rows", 1);
            int columns = context.getConfiguration().getInt("columns", 1);
            String[] tokens = value.toString().split(",");
            int i = Integer.parseInt(tokens[0]);
            int j = Integer.parseInt(tokens[1]);
            double v = Double.parseDouble(tokens[2]);

            context.write(new Pair(i / rows, j / columns), new Triple(i % rows, j % columns, v));
        }
    }

    public static class ConvertReducer extends Reducer<Pair, Triple, Pair, Block> {
        @Override
        public void reduce(Pair key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
            
            int rows = context.getConfiguration().getInt("rows", 1);
            int columns = context.getConfiguration().getInt("columns", 1);
        
            Block block = new Block(rows, columns);
            for (Triple triple : values) {
                block.data[triple.i][triple.j] = triple.value;
            }
            context.write(key, block);
        }
    }


    public static class AddMapper extends Mapper<Pair, Block, Pair, Block> {
        @Override
        protected void map(Pair key, Block value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class AddReducer extends Reducer<Pair, Block, Pair, Block> {
        @Override
        protected void reduce(Pair key, Iterable<Block> values, Context context) throws IOException, InterruptedException {
            int rows = context.getConfiguration().getInt("rows", 1);
            int columns = context.getConfiguration().getInt("columns", 1);
            
            Block resultBlock = new Block(rows, columns);

            for (Block block : values) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < columns; j++) {
                        resultBlock.data[i][j] += block.data[i][j];
                    }
                }
            }

            context.write(key, resultBlock);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("rows", Integer.parseInt(args[0]));

        conf.setInt("columns", Integer.parseInt(args[1]));
        Job job1 = Job.getInstance(conf, "Convert to Block Matrix 1");
        job1.setJarByClass(Add.class);

        job1.setMapperClass(ConvertMapper.class);
        job1.setReducerClass(ConvertReducer.class);
        
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(Block.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(Triple.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[4]));
        
        if (!job1.waitForCompletion(true)) {
            System.err.println("First matric Convertion failed");
            System.exit(1);
        }

        
        Job job2 = Job.getInstance(conf, "Convert to Block Matrix 2");
        job2.setJarByClass(Add.class);
        job2.setMapperClass(ConvertMapper.class);
        job2.setReducerClass(ConvertReducer.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(Block.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Triple.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[5]));

        if (!job2.waitForCompletion(true)) {
            System.err.println("First matric Convertion failed");
            System.exit(1);
        }

        // Third Map-Reduce job for block matrix addition
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(Add.class);
        job3.setMapperClass(AddMapper.class);
        job3.setReducerClass(AddReducer.class);
        job3.setOutputKeyClass(Pair.class);
        job3.setOutputValueClass(Block.class);
        job3.setMapOutputKeyClass(Pair.class);
        job3.setMapOutputValueClass(Block.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job3, new Path(args[4]), SequenceFileInputFormat.class, AddMapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[5]), SequenceFileInputFormat.class, AddMapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[6]));

        if (!job3.waitForCompletion(true)) {
            System.err.println("Block Addition failed");
            System.exit(1);
        }

        System.exit(0);
    }
}
