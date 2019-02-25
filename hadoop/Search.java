import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class sortComparator extends WritableComparator {

    protected sortComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable x, WritableComparable y) {
        DoubleWritable a = (DoubleWritable)x;
        DoubleWritable b = (DoubleWritable)y;

        return -1*a.compareTo(b);
    }
}

public class Search {

    public static class Mapper1 extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static Pattern PUNCTUATIONS = Pattern.compile("\\p{Punct}");

        @Override
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
            String query = context.getConfiguration().get("TERM");
            StringTokenizer qitr = new StringTokenizer(query.toString());

            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] keys = itr.nextToken().split("#");
            double tfidf = Double.parseDouble(itr.nextToken().toString());

            while (qitr.hasMoreTokens()) {
                String t = qitr.nextToken();
                Matcher unwantedMatcher = PUNCTUATIONS.matcher(t);
                t = unwantedMatcher.replaceAll("");
                if (t.equalsIgnoreCase(keys[0])) {
                    context.write(new Text(keys[1]), new DoubleWritable(tfidf));
                }
            }
        }
    }

    public static class Reducer1 extends Reducer<Text,DoubleWritable,DoubleWritable,Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> value, Context context)throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable x:value) {
                sum += x.get();
            }
            context.write(new DoubleWritable(sum) ,new Text(key.toString()));
        }

    }


    public static void main(String[] args)throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: search <search term> <in> [<in>...] <out>");
            System.exit(2);
        }
        configuration.set("TERM",otherArgs[0]);

        Job job = Job.getInstance(configuration, "search");
        job.setJarByClass(Search.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(sortComparator.class);
        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
