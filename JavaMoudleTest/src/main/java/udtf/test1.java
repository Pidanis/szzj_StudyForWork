package udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.IOException;
@Resolve({"string,string -> string"})
public class test1 extends UDTF {
    @Override
    public void process(Object[] objects) throws UDFException, IOException {

    }


}
