package com.aliyun.odps.examples.flink.transform.function;

import org.apache.flink.api.common.functions.FilterFunction;

public class MyFilter implements FilterFunction<String> {

    private String arg1;

    public MyFilter() {
    }

    public MyFilter(String arg1) {
        this.arg1 = arg1;
    }

    @Override
    public boolean filter(String value) throws Exception {
        return value.contains(this.arg1);
    }
}
