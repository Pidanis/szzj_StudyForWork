package udaf;

//import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
//import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;
//import com.aliyun.odps.udf.annotation.Resolve;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

@Resolve("string,*->string")
public class distinct_col extends Aggregator {
    private static class MyBuffer implements Writable {
        private String  zj_ls = "";
        private String dz = "";
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(zj_ls);
            out.writeUTF(dz);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            zj_ls =  in.readUTF();
            dz = in.readUTF();
        }
    }



    @Override
    public Writable newBuffer() {
        return new MyBuffer();
    }
    Map<String,String> map = new LinkedHashMap<String, String>();
    String old_dz = "";
    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
        String zj_ls = String.valueOf(args[0]) + "," + String.valueOf(args[1]);
//        String ls = String.valueOf(args[1]);
//        Long cnt = Long.parseLong(String.valueOf(args[1]));
        String dz = String.valueOf(args[2]);
        MyBuffer buf = (MyBuffer) buffer;

        if (zj_ls != null) {
            if(map.containsKey(zj_ls)){
                String[] dz_arr = map.get(zj_ls).split(",");
//                old_count = cnt+newcnt;
                if(!Arrays.asList(dz_arr).contains(dz)){
                    map.put(zj_ls, map.get(zj_ls) + "," + dz);
                }

            }else {
                map.put(zj_ls, dz);
            }
        }
        buf.zj_ls = zj_ls;
        buf.dz = map.get(zj_ls);

    }
    private Text ret = new Text();
    @Override
    public Writable terminate(Writable arg0) throws UDFException {
        MyBuffer buffer = (MyBuffer) arg0;
        ret.set(buffer.dz);
        return ret;
    }

    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        MyBuffer buf = (MyBuffer) buffer;
        MyBuffer p = (MyBuffer) partial;
        buf.zj_ls = p.zj_ls;
        for(String dz_tmp : buf.dz.split(",")){
            if(dz_tmp != null && dz_tmp != ""){
                if(!Arrays.asList(p.dz.split(",")).contains(dz_tmp)){
                    p.dz = p.dz + "," + dz_tmp;
                }
            }
        }
        buf.dz = p.dz;
    }


}