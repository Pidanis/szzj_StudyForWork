import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.security.SecurityManager;

import javax.swing.*;
import java.lang.reflect.Array;
import java.util.*;

public class test {
    public static void main(String[] args) {
//        test t1 = new test();
//        for(int i=0; i<50; i++ ){
//            System.out.print("fib1 i-"+ i + " sum-" + t1.fib1(i));
//            System.out.println(" / fib2 i-"+ i + " sum-" + t1.fib2(i));
//        }
//        String s = "{({})}";
//        System.out.println(t1.containsPattern(new int[]{3,2,2,1,2,2,1,1,1,2,3,2,2}, 2, 2));
        System.out.println("浙浙浙66666".replaceAll("^浙+", "浙"));
        System.out.println("1,2,3".split(","));
        
//        System.out.println(t1.reverseWords("a good   example"));
    }

    public boolean containsPattern(int[] arr, int m, int k) {
        int len = arr.length;
        String preStr = "";
        String cur_str = "";
        int count = 0;
        String arrStr = "";
        for(int arr_:arr){
            System.out.println(Integer.toString(arr_));
            arrStr = arrStr.concat(Integer.toString(arr_));
        }
//        System.out.println(arrStr);
        for(int j=0; j<len-m; j++){
            preStr = arrStr.substring(j, j+m);
            for(int i=j+m; i<= len - m; i+=m){
                System.out.println("cur_str:" + cur_str);
                System.out.println("count:" + count);
                cur_str = arrStr.substring(i, i+m);
                if(cur_str.equals(preStr)){
                    count ++;
                }else{
                    if(count >= k){
                        return true;
                    }
//                    preStr = cur_str;
                    count = 1;
                    break;

                }
            }
//            if(count >= k){
//                return true;
//            }else{return false;}
        }
//        for(int i=0; i<= len - m; i+=m){
//            System.out.println("cur_str:" + cur_str);
//            System.out.println("count:" + count);
//            cur_str = arrStr.substring(i, i+m);
//            if(cur_str.equals(preStr)){
//                count ++;
//            }else{
//                if(count >= k){
//                    return true;
//                }
//                preStr = cur_str;
//                count = 1;
//
//            }
//        }
        if(count >= k){
            return true;
        }else{return false;}

    }



    public String reverseWords(String s) {
        String s_new = "";
        String[] Strs = s.split(" ");
        for(String Str:Strs){
            s_new = Str.concat(" ").concat(s_new);
            System.out.println(s_new);
        }
        return s_new.trim();
    }

    public String convertToBase7(int num) {
        String a = "";
        String temp = "";
        if(num < 0){
            temp = "-";
            num = -num;
        }
        while(num != 0){
            System.out.println(a);
            a += String.valueOf(num % 2);
            num /= 2;
        }
        // a += String.valueOf(num);
        a += temp;
        return new StringBuffer(a).reverse().toString();
    }

    public boolean hasAlternatingBits(int n) {
        String a = "";
        String b = "";
        while(n != 0){
            a += n % 2;
            n /= 2;
        }
        // a += n;
        b = new StringBuffer(a).reverse().toString();
        return a.equals(b);
    }



    public int fib1(int n) {
    if(n == 0 || n == 1){
        return n;
    }
    else if(n == 2){
        return 1;
    }
    int temp0 = 1;
    int temp1 = 1;
    int tmp = 0;
//    int[] temp = new int[]{0, 1};
    for(int i = 2; i < n; i++){
//        temp0 = temp[0];
//        temp1 = temp[1];
//            System.out.print("temp0--" + temp0 + " / " + "temp1--" + temp1 + "......");

        tmp = temp0 + temp1;
        temp0 = temp1;
        temp1 = tmp>1000000007 ? tmp % 1000000007:tmp;
    }
//    System.out.print("temp[0]--" + temp[0] + " / " + "temp[1]--" + temp[1] + "......");
    return temp1;
}

    public int fib2(int n) {
        if(n==0 || n==1){
            return n;
        }
        int a=0; int b=0; int c=1;
        for(int i=1;i<n;i++){
            a=b;
            b=c;
            c=a+b;
            if(c>1000000007){
                c=c%1000000007;
            }
        }
        return c;
    }

    public int numWays(int n) {
        LinkedHashMap<Integer, Integer> map = new LinkedHashMap<>();
        List A = new ArrayList();
        Iterator b = A.iterator();
        if(n == 0 || n == 1){
            return n;
        }
        int temp0 = 0;
        int temp1 = 1;
        int temp = 0;
        for(int i=2; i<=n; i++){
            temp = temp0 + temp1;
            temp0 = temp1;
            temp1 = temp<(1e9+7)?temp:temp%(1000000007);
        }
        return temp1;
    }
}
