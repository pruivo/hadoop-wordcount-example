package com.gustavonalle.hadoop;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputFormat;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputFormat;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf(Main.class);
        jobConf.setJobName("wordcount");

        jobConf.setOutputKeyClass(String.class);
        jobConf.setOutputValueClass(Integer.class);
        jobConf.setOutputKeyComparatorClass(StringComparator.class);
        jobConf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.io.serializer.JavaSerialization");

        jobConf.setMapperClass(MapClass.class);
        jobConf.setReducerClass(ReduceClass.class);

        jobConf.set("mapreduce.ispn.output.cache.name", "namedCache");

        //FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        //FileOutputFormat.setOutputPath(jobConf, new Path(args[0]));
        jobConf.setInputFormat(InfinispanInputFormat.class);
        jobConf.setOutputFormat(InfinispanOutputFormat.class);
        System.out.println("About to run the job!!!!");
        JobClient.runJob(jobConf);

        System.out.println("Finished executing job.");
        System.out.println("CACHE CONTENT:");

        RemoteCacheManager remoteCacheManager = new RemoteCacheManager();
        RemoteCache cache = remoteCacheManager.getCache("namedCache");
        System.out.println(cache.getBulk());

    }

    public static class StringComparator implements RawComparator<String> {

        @Override
        public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
            return WritableComparator.compareBytes(bytes, i, i2, bytes2, i3, i4);
        }

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }
}
