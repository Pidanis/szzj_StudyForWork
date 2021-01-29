import com.aliyun.odps.udf.UDF;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.udf.UDF;
public class MyConcat extends UDF {
//    private Text ret = new Text();
    public String evaluate(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        double c = Double.parseDouble(a);
        if (b.equals("age")) {
            if (c < 18){
                String d = "未成年";
                return d;
            }else{
                return "已成年";
            }
        }
        return b;
    }

//    public static void main(String[] args) {
//        System.out.println(new MyConcat().evaluate("20", "age"));
//    }
}