package udf;

import com.aliyun.odps.udf.UDF;
import com.google.gson.JsonObject;

public class test1 extends UDF {

    public String evaluate(String a, String b){
        String tmp1 = a + ":" + b;
        return "{" + tmp1 + "}";
    }
}
