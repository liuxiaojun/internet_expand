import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

/**
 * Created by liuxiaojun on 2017/1/10.
 */
public class SimReduce extends Reducer<Text, Text, Text, NullWritable> {
    public static FileSystem dstFs;
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        dstFs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
        long i = 0L;
        Path toWrite = new Path(key.toString());
        if (!dstFs.exists(toWrite.getParent())){
            dstFs.mkdirs(toWrite.getParent());
        }

        String writePath = key.toString();
        Path hdfsWrite = new Path(writePath);
        BufferedOutputStream bs = new BufferedOutputStream(dstFs.create(hdfsWrite));

        for(Text v: values){
            i = i +1;
            bs.write((v + "\n").getBytes());
        }
        bs.flush();
        bs.close();
    }
}