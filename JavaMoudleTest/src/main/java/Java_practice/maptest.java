package Java_practice;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class maptest {
    LinkedHashMap<String, String> a1 = new LinkedHashMap();
//    HashMap<String, String> a1 = new HashMap();
    public static void main(String[] args) {
        maptest test1 = new maptest();
        test1.a1.put("pi","dan");
        test1.a1.put("dan", "pi");
        test1.a1.put("dan", "PI");
        for (Map.Entry entry:test1.a1.entrySet()){
            System.out.println(entry.getKey() +"-" + entry.getValue());
        }
    }
}
