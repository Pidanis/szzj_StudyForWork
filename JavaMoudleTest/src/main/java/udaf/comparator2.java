package udaf;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.WritableComparable;

public class comparator2 extends Text.Comparator {
    public comparator2(Class<? extends WritableComparable> keyClass) {
        super(keyClass);
    }
}
