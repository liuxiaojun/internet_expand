import java.io.*;
import java.net.*;
import java.util.Arrays;
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
        if (args.length >= 1 && args[0].length()==10) {
            Configuration conf = new Configuration();
            conf.set("mapreduce.map.memory.mb","9999");
            conf.set("mapreduce.reduce.memory.mb","9999");
            conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.95F);
            conf.set("mapreduce.reduce.speculative", "false");

            DistributedCache.addCacheFile(new URI("hdfs://nameservice1/data/user/ap_city.txt#ap_city.txt"), conf);
            DistributedCache.addCacheFile(new URI("hdfs://nameservice1/data/user/apmac_orgId.txt#apmac_orgId.txt"), conf);

            String timeHour = args[0];
            Job job = Job.getInstance(conf, "expand_internet_"+timeHour);
            job.getConfiguration().setStrings("timeHour", timeHour);

            String out_put = "/tmp/expand_internet_" + System.currentTimeMillis();
            Path out = new Path(out_put);

            //one hour
            FileInputFormat.addInputPaths(job, "hdfs://nameservice1/data/internet/"+timeHour.substring(0,4)+"/"+timeHour.substring(4,6)+"/"+timeHour.substring(6,8)+"/internet_"+timeHour+".log.bz2");
            Integer reduceTasks = 1;

            FileOutputFormat.setOutputPath(job, out);
            job.setJarByClass(SimExpand.class);
            job.setMapperClass(SimMap.class);
            job.setReducerClass(SimReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(reduceTasks * 500);
            job.waitForCompletion(true);
        }else{
            System.out.println(Arrays.toString(args));
        }
    }
}