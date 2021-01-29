import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.IOException;

@Resolve("string->string,string")
public class cjzParse extends UDTF {

    @Override
    public void process(Object[] objects) throws UDFException, IOException {
        String a = (String) objects[0];
        String a1 = a.substring(0, a.length()-6);
        String a2 = a.substring(a.length()-6, a.length());
        forward(a1, a2);
    }
}
