package Java_practice;

public class Demo3 {
    public static void main(String[] args){
        int[] arr = {1, 2, 3, 4, 5};
        int tmp;
        for (int i = 0; i < arr.length / 2; i++){
            tmp = arr[i];
            arr[i] = arr[arr.length - 1 - i];
            arr[arr.length - 1 - i] = tmp;
        }
        for (int i = 0; i < arr.length; i++){
            System.out.println(arr[i]);
        }
    }
}
