package udf;

import java.util.Comparator;

public class comparaterTest1 implements Comparator {
    @Override
    public int compare(Object o1, Object o2) {
        if((Integer)o1 > (Integer) o2){
            return 1; //1升序输出
        }else if((Integer)o1 < (Integer)o2){
            return -1;
        }else{
            return 0;
        }
    }
}
