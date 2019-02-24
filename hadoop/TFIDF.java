import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TFIDF {

    public static class Mapper1 extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable ONE = new LongWritable(1);
        private final static Pattern PUNCTUATIONS = Pattern.compile("\\p{Punct}");

        @Override
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer iterator = new StringTokenizer(value.toString());
            while(iterator.hasMoreTokens()) {
                String wrd = iterator.nextToken();
                Matcher punctRemoval = PUNCTUATIONS.matcher(wrd);
                wrd = punctRemoval.replaceAll("");
                if(!wrd.isEmpty()) {
                    context.write(new Text(wrd.toString()+"#"+fileName),ONE);
                }
            }
        }
    }

    public static class Reducer1 extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> value, Context context)throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable x:value) {
                sum += x.get();
            }
            context.write(key,new LongWritable(sum));
        }

    }

    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
            StringTokenizer itrerator = new StringTokenizer(value.toString());
            String[] keys = itrerator.nextToken().split("#");
            String t = keys[0] + "#" + itrerator.nextToken();
            context.write(new Text(keys[1]), new Text(t));
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
        public static enum count {TOTAL};

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
            long sum = 0;
            String docName = key.toString();
            context.getCounter(count.TOTAL).increment(1);
            Map<String, Long> words = new HashMap<>();
            for(Text x:value) {
                String[] parts = x.toString().split("#");
                long n = Long.parseLong(parts[1]);
                sum += n;
                words.put(docName + "#" + parts[0], n);
            }
            for ( String val : words.keySet() ) {
                long n = words.get(val);
                context.write(new Text(val), new Text(Long.toString(n) + "#" + Long.toString(sum)));
            }
        }

    }

    public static class Mapper3 extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
            StringTokenizer iterator = new StringTokenizer(value.toString());
            String[] keys = iterator.nextToken().split("#");
            String[] vals = iterator.nextToken().split("#");
            context.write(new Text(keys[1]), new Text(keys[0]+"#"+vals[0]+"#"+vals[1]+"#"+1));
        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, DoubleWritable> {

        private int numOfDocs = 0;

        @Override
        public void setup(Context context)throws IOException,InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            this.numOfDocs = configuration.getInt("TOTAL_DOCS",-1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
            int sum = 0;
            Map<String, Vector<Long>> docCount = new HashMap<>();
            for ( Text val : value ) {
                String[] parts = val.toString().split("#");
                if ( 4 != parts.length )
                    continue;
                long n = Long.parseLong(parts[1]);
                long N = Long.parseLong(parts[2]);
                long m = Long.parseLong(parts[3]);
                sum += m;
                Vector<Long> l = new Vector<>();
                l.add(n);
                l.add(N);
                docCount.put(key.toString() + "#" + parts[0], l);
            }
            Map<String, Double> tfidf = new HashMap<>();
            for (String val : docCount.keySet()) {
                double tf, idf, result;
                if (docCount.get(val).get(1) == 0) {
                    continue;
                }
                tf = ((double)docCount.get(val).get(0) / (double)docCount.get(val).get(1));
                if ( sum == 0 ) {
                    idf = 0.0;
                } else {
                    idf = java.lang.Math.log( (double)( numOfDocs/((double)(sum)) ) );
                }
                result = tf * idf;
                tfidf.put(val, result);
            }
            for ( String val : tfidf.keySet() ) {
                context.write(new Text(val), new DoubleWritable(tfidf.get(val)));
            }
        }
    }

    public static void main(String[] args)throws Exception {
        Configuration configuration = new Configuration();

        Job j1 = Job.getInstance(configuration, "word count");
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TDIDF <in> [<in>...] <out>");
            System.exit(2);
        }
        j1.setJarByClass(TFIDF.class);
        j1.setMapperClass(Mapper1.class);
        j1.setCombinerClass(Reducer1.class);
        j1.setReducerClass(Reducer1.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(j1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(j1,new Path(otherArgs[otherArgs.length - 1] + "_1"));
        j1.waitForCompletion(true);

        Job j2 = Job.getInstance(configuration, "inv index");
        j2.setJarByClass(TFIDF.class);
        j2.setMapperClass(Mapper2.class);
        j2.setReducerClass(Reducer2.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(Text.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2, new Path(otherArgs[otherArgs.length - 1] + "_1/part-r-00000"));
        FileOutputFormat.setOutputPath(j2, new Path(otherArgs[otherArgs.length - 1] + "_2"));
        j2.waitForCompletion(true);
        configuration.setInt("TOTAL_DOCS", (int)j2.getCounters().findCounter(Reducer2.count.TOTAL).getValue());

        Job j3 = Job.getInstance(configuration, "tfidf");
        j3.setJarByClass(TFIDF.class);
        j3.setMapperClass(Mapper3.class);
        j3.setReducerClass(Reducer3.class);
        j3.setMapOutputKeyClass(Text.class);
        j3.setMapOutputValueClass(Text.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j3, new Path(otherArgs[otherArgs.length - 1] + "_2/part-r-00000"));
        FileOutputFormat.setOutputPath(j3, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(j3.waitForCompletion(true) ? 0 : 1);
    }
}
