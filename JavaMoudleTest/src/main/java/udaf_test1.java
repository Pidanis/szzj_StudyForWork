import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
//
//// TODO define input and output types, e.g. "double->double".
@Resolve({"string,string -> string"})
//public class udaf_test1 extends Aggregator {
//
//    private class MyBuffer implements Writable {
//
//        private String key;
//        private String value;
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            dataOutput.writeUTF(key);
//            dataOutput.writeUTF(value);
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//            key = dataInput.readUTF();
//            value = dataInput.readUTF();
//        }
//    }
//
//    @Override
//    public void setup(ExecutionContext ctx) throws UDFException {
//
//    }
//
//    /**
//     * 创建聚合Buffer
//     *
//     * @return Writable聚合buffer
//     */
//    @Override
//    public Writable newBuffer() {
//        // TODO
//        return new MyBuffer();
//    }
//    LinkedHashMap<String, String> map = new LinkedHashMap<>();
//    /**
//     * @param buffer 聚合buffer
//     * @param args   SQL中调用UDAF时指定的参数，不能为null，但是args里面的元素可以为null，代表对应的输入数据是null
//     * @throws UDFException
//     */
//    @Override //这一个用于map阶段，产生各片区的map结果
//    public void iterate(Writable buffer, Writable[] args) throws UDFException {
//        // TODO
//        Long old_count = 0L;
//        String key1 =  String.valueOf(args[0]);
//        Long value1 = Long.parseLong(String.valueOf(args[1]));//String.valueOf(args[1])
//        MyBuffer buffer1 =(MyBuffer) buffer;
//
//        if (key1 != null){
//            if(map.containsKey(key1)){
//                Long old_value = Long.parseLong(map.get(key1));
//                value1 += old_value;
//            }
//            map.put(key1, String.valueOf(value1));
//        }
//
//        buffer1.key = key1;
//        buffer1.value = map.get(key1);
//
//
//    }
//
//    /**
//     * @param buffer  聚合buffer
//     * @param partial 分片聚合结果 //reduce之后的结果，直接赋值给buffer，因为整个UDAF是依靠buffer去传递值
//     * @throws UDFException
//     */
//    Long tmp = 0L;
//    @Override
//    public void merge(Writable buffer, Writable partial) throws UDFException {
//        // TODO
//        MyBuffer buffer1 = (MyBuffer) buffer;
//        MyBuffer buffer2 = (MyBuffer) partial;
//        if(buffer2.value != null){
//            tmp = Long.parseLong(buffer1.value);
//        }else{
//            tmp += Long.parseLong(buffer2.value);
//        }
////        buffer1.key = buffer2.key;
//        buffer1.value = String.valueOf(tmp);
//    }
//
//    /**
//     * 生成最终结果
//     *
//     * @param buffer
//     * @return Object UDAF的最终结果
//     * @throws UDFException
//     */
//    private LongWritable ret = new LongWritable(0L);
//    @Override
//    public Writable terminate(Writable buffer) throws UDFException {
//        // TODO
//        MyBuffer buffer1 = (MyBuffer) buffer;
//        ret.set(Long.parseLong(buffer1.value));
//        return new Text(String.valueOf(ret));
//    }
//
//    @Override
//    public void close() throws UDFException {
//
//    }
//
////    public static void main(String[] args) throws UDFException {
////        Writable[] a1 = new Writable[]{new Text("Jan"), new Text("1")};
////        MyBuffer1 buffer1 = new MyBuffer1();
////        new udaf_test1().iterate(buffer1, a1);
////    }
//}
////class MyBuffer1 implements Writable {
////
////    private String key;
////    private String value;
////
////    @Override
////    public void write(DataOutput dataOutput) throws IOException {
////        dataOutput.writeUTF(key);
////        dataOutput.writeChars(value);
////    }
////
////    @Override
////    public void readFields(DataInput dataInput) throws IOException {
////        key = dataInput.readUTF();
////        value = dataInput.readUTF();
////    }
////}

public class udaf_test1 extends Aggregator {
    private static class MyBuffer implements Writable {
        private String  name;
        private Long count;
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(name);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            name =  in.readUTF();
            count = in.readLong();
        }
    }
    @Override
    public Writable newBuffer() {
        // TODO
        return new MyBuffer();
    }
    LinkedHashMap<String,Long> map = new LinkedHashMap<>();
    Long old_count = 0L;
    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
        //TODO
        String arg = String.valueOf(args[0]);
        Long cnt = Long.parseLong(String.valueOf(args[1]));
        MyBuffer buf = (MyBuffer) buffer;

        if (arg != null) {
            if(map.containsKey(arg)){
                Long newcnt = map.get(arg);
                old_count = cnt + newcnt;
                map.put(arg,old_count);
            }else {
                map.put(arg,old_count+cnt);
            }
        }
        buf.name = arg;
        buf.count = map.get(arg);

    }
    private LongWritable ret = new LongWritable();
    @Override
    public Writable terminate(Writable arg0) throws UDFException {
        //TODO
        MyBuffer buffer = (MyBuffer) arg0;
        ret.set(buffer.count);
        return new Text(String.valueOf(ret));
    }

    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        //TODO
        MyBuffer buf = (MyBuffer) buffer;
        MyBuffer p = (MyBuffer) partial;
        buf.name = p.name;
        buf.count = p.count;
    }


}