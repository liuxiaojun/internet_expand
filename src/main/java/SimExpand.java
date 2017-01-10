import java.io.*;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by liuxiaojun on 2017/1/5.
 */
public class SimExpand {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.95F);
        conf.set("mapreduce.reduce.speculative","false");

        DistributedCache.addCacheFile(new URI("hdfs://nameservice1/data/user/ap_city.txt#ap_city.txt"), conf);
        DistributedCache.addCacheFile(new URI("hdfs://nameservice1/data/user/apmac_orgId.txt#apmac_orgId.txt"), conf);

        Job job = Job.getInstance(conf, "expand_internet_test");
        String out_put = "/tmp/expand_internet_" + System.currentTimeMillis();
        Path out = new Path(out_put);
        // use test
        FileInputFormat.addInputPaths(job,"hdfs://nameservice1/data/internet/2017/01/10/internet_2017011000.log.bz2");
        Integer reduceTasks = 1;

        FileOutputFormat.setOutputPath(job, out);
        job.setJarByClass(SimExpand.class);
        job.setMapperClass(SimMap.class);
        job.setReducerClass(SimReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(reduceTasks*500);
        job.waitForCompletion(true);
    }
}