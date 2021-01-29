import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string -> string"})
public class test2 extends UDTF {
    @Override
    public void process(Object[] args) throws UDFException {
        // TODO
        String a = (String) args[0];
        String b = (String) args[1];
        String c;
//        forward(b);
        if(b.equals("age")){
            double tmp = Double.parseDouble(a);
            if(tmp < 18){
                c = "未成年";
//                //                System.out.println("未成年");
            }else{
                c = "已成年";
//                //                System.out.println("已成年");
            }
            forward(c);
        }
    }

//    public static void main(String[] args) throws UDFException {
//        test2 st = new test2();
//        Object[] arr = new Object[]{"20", "age"};
////        ArrayList<String> arr = new ArrayList<String>();
////        arr.add(0, "20");
////        arr.add(1, "age");
//        System.out.println(arr[0]);
//        System.out.println(arr[1]);
////        arr[0].
////        arrarr[0].add(0,);
//
////        arr[0].add("20");
////        arr[1].add("age");
////        st.process(arr);
////        Object[] arr = new ArrayList[8];
////        arr.
////        arr.add("age");
//        new test2().process(arr);
//    }



}