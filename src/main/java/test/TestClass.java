package test;

public class TestClass {
    public static void main(String[] args) {
//        int[] A = new int[]{1, 3, 6, 4, 1, 2};
//        int[] A = new int[]{1, 2, 3};
        int[] A = new int[]{-1, -3};

        System.out.println(new TestClass().sol(-999));
    }


    public int solution(int N) {
        boolean isNeg = false;
        if (N == 0) {
            return 50;
        } else if (N < 0) {
            N = Math.abs(N);
            isNeg = true;
        }

        int arr[] = new int[4];
        int i = 0;
        int number = N;
        while (number > 0) {
            arr[i++] = number % 10;
            number = number / 10;
        }
        return 0;
    }

    public int sol(int N) {
        int digit = 5;
        if (N == 0) {
            return digit * 10;
        }
        int neg = N < 0 ? -1 : 1;

        N = Math.abs(N);
        int n = N;
        int count = 0;
        while (n > 0) {
            count++;
            n = n / 10;
        }
        int max = Integer.MIN_VALUE;
        int pos = 1;
        for (int i = 0; i <= count; i++) {
            int newValue = ((N / pos) * (pos * 10)) + (digit * pos) + (N % pos);
            System.out.println("pos = " + pos);
            System.out.println("N = " + N);
            System.out.println("N % pos = " + N % pos);
            System.out.println("(N / pos) * (pos * 10) = " + ((N / pos) * (pos * 10)));
            System.out.println("newValue = " + newValue);
            System.out.println("----------------------------------");
            if (newValue * neg > max) {
                max = newValue * neg;
            }
            pos = pos * 10;
        }
        return max;
    }

}
