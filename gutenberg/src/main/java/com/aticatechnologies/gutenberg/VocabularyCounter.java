package com.aticatechnologies.gutenberg;

/**
 * Created by JuanManuel on 29/07/2015.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * @author Juan Manuel Musacchio
 */
public class VocabularyCounter extends Configured implements Tool {

    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORDS = new Text("words");

    /**
     * It maps each word as a key and puts a one for each occurrence as a value
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final transient Text word = new Text();
        private static final BreakIterator wordIterator = BreakIterator.getWordInstance(new Locale("en","US"));

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final String line = value.toString();
            wordIterator.setText(line);

            int start = wordIterator.first();
            int end = wordIterator.next();
            while (end != BreakIterator.DONE) {
                String w = line.substring(start,end);
                if (Character.isLetterOrDigit(w.charAt(0))) {
                    word.set(w);
                    context.write(word, ONE);
                }
                start = end;
                end = wordIterator.next();
            }
        }
    }

    /**
     * It creates a single key called words and put a one per key/word
     */
    public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            context.write(WORDS, ONE);
        }
    }

    /**
     * It counts the number of ones for the key called words to get the vocabulary
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(WORDS, new IntWritable(sum));
        }
    }

    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(VocabularyCounter.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new Configuration(), new VocabularyCounter(), args);
        System.exit(returnCode);
    }
}