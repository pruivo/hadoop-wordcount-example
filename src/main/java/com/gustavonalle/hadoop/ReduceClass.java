package com.gustavonalle.hadoop;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ReduceClass extends MapReduceBase implements Reducer<String, Integer, String, Integer> {

    public void reduce(String key, Iterator<Integer> values,
                       OutputCollector<String, Integer> output,
                       Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next();
        }
        output.collect(key, sum);
    }
}
