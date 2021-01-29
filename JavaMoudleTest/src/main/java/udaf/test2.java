package udaf;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

@Resolve({"string,string,string -> string"})
public class test2 extends Aggregator {

    class MyBuffer implements Writable{

        String a = "";
        String b = "";
//        String c;

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(a);
//            dataOutput.writeUTF(b);
//            dataOutput.writeUTF(c);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            a = dataInput.readUTF();
//            b = dataInput.readUTF();
//            c = dataInput.readUTF();
        }
    }

    @Override
    public Writable newBuffer() {
        return new MyBuffer();
    }

    HashMap<String, HashMap> map = new HashMap();
    HashMap<String, String> map1 = new HashMap<>();
    int i = 1;
    @Override
    public void iterate(Writable writable, Writable[] testWritables) throws UDFException {
//        List<String> strs = new ArrayList<>();
        String a1 = String.valueOf(testWritables[0]);
        String a2 = String.valueOf(testWritables[1]);
        String a3 = String.valueOf(testWritables[2]);
        map1.put(a1+a2, a3);
//
//        int len = testWritables.length;
//        String tmp = "";
//        for(Writable item:testWritables){
//            tmp += String.valueOf(item) + "-";
//        }
////        String a3 = String.valueOf(testWritables[2]);
//
//
//        map1.put(a2,a3);
//        if(map.containsKey(a1)){
////            map1.put(a2,a3);
//            map.get(a1).put(a2,a3);
//        }else{
//            map1.put(a2,a3);
//            map.put(a1,map1);
//        }
        String mapString = "";
//        LinkedHashMap<String, String> x = map.get(a1);
//        for (Map.Entry map2: x.entrySet()){
//            mapString = (String)map2.getKey() + "-" + (String)map2.getValue();
////            String mapvalue = (String)map2.getValue();
//        }


//        map1.put("1","1");
//        map1.put("2","2");
        for (Map.Entry map2: map1.entrySet()){
            mapString += "<" + i + ">" + map2.getKey() + "-" + map2.getValue() + " ";
        }
        MyBuffer result = (MyBuffer) writable;
//        result.a = a1;
        result.a = mapString;
        i++;

//        result.c = a3;

    }



    @Override
    public Writable terminate(Writable writable) throws UDFException {
        MyBuffer ret = (MyBuffer) writable;
        return new Text(ret.a);
    }

    @Override
    public void merge(Writable writable, Writable writable1) throws UDFException {
        MyBuffer result = (MyBuffer) writable;
        MyBuffer result2 = (MyBuffer) writable1;
        result.a = result2.a;
//        result.b = result2.b;
//        result.c = result2.c;
    }
}
