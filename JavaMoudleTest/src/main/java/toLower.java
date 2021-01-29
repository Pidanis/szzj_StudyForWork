import com.aliyun.odps.io.Text;
import com.aliyun.odps.udf.UDF;

public class toLower extends UDF {
    public String evaluate(String s){
        if(s == null){
            return null;
        };

        return s.toLowerCase();
    }
}
