package test;

public class TestClass2 {
    // "static void main" must be defined in a public class.

    public static void main(String[] args) {
        System.out.println(largestNumber(268));
        System.out.println(largestNumber(670));
        System.out.println(largestNumber(0));
        System.out.println(largestNumber(-999));
        System.out.println(largestNumber(-945));
        System.out.println(largestNumber(-439));

    }

    private static int largestNumber(int num) {
        boolean isNegative = num < 0;
        String resStr = "";
        if (!isNegative) {
            String temp = String.valueOf(num);
            for (int i = 0; i < temp.length(); i++) {
                int n = Integer.parseInt(String.valueOf(temp.charAt(i)));
                if (5 > n) {
                    resStr = temp.substring(0, i) + "5" + temp.substring(i);
                    break;
                }
            }
            if (resStr.length() == 0) {
                resStr += "5";
            }
        } else {
            String temp = String.valueOf(num * -1);
            for (int i = 0; i < temp.length(); i++) {
                int n = Integer.parseInt(String.valueOf(temp.charAt(i)));
                if (5 < n) {
                    resStr = temp.substring(0, i) + "5" + temp.substring(i);
                    break;
                }
            }

        }

        return isNegative ? Integer.parseInt(resStr) * -1 : Integer.parseInt(resStr);
    }

}