package com.sina;

import org.apache.hadoop.hive.ql.exec.UDF;

public class AddOne extends UDF {

    public String[] evaluate(String text) {

        return text.split(" ");
    }



}
