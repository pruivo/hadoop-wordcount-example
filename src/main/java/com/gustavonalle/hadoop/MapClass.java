package com.gustavonalle.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.infinispan.hadoopintegration.InfinispanObject;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * The mapper
 */
public class MapClass extends MapReduceBase implements Mapper<InfinispanObject<Integer>, InfinispanObject<String>, String, Integer> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(InfinispanObject<Integer> key, InfinispanObject<String> value,
                    OutputCollector<String, Integer> output,
                    Reporter reporter) throws IOException {

        StringTokenizer itr = new StringTokenizer(value.get());
        while (itr.hasMoreTokens()) {
            output.collect(itr.nextToken(), 1);
        }
    }

}
