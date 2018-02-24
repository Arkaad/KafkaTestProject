package test;

public class PatternTest {
    private static int maxRow;
    private static int maxColumn;

    public static void main(String[] args) {
        String arr[][] = {
                {"#", ".", "#", "#", "#"},
                {"#", ".", "#", ".", "#"},
                {"#", ".", "#", "#", "#"},
                {"#", ".", ".", ".", "#"},
                {"#", "#", "#", "#", "#"}
        };
        maxRow = arr[0].length - 1;
        maxColumn = arr.length - 1;
        System.out.println("maxRow = " + maxRow);
        System.out.println("maxColumn = " + maxColumn);

        int count = 0;
        int boundaryCount = 0;
        for (int row = 0; row <= maxRow; row++) {
            for (int col = 0; col <= maxColumn; col++) {
                if (arr[row][col].equals("#")) {
                    boundaryCount = getBoundaryCount(arr, row, col);
//                    System.out.println("boundaryCount = " + boundaryCount);
                    count += boundaryCount;
                }
            }
        }
        System.out.println("No. of Boundaries = " + count);
    }

    private static int getBoundaryCount(String[][] arr, int row, int col) {
        int boundaryCount = 0;
        if (maxRow == maxColumn && maxRow == 0) {
            return 4;
        }
        //for 0th row
        if (row == 0) {
            if (col == 0) {
                boundaryCount += 2;
                if (isOk(arr, row, col + 1)) {
                    boundaryCount += 1;
                }
            } else {
                boundaryCount += 1;
                if (col == maxColumn) {
                    boundaryCount += 1;
                    if (isOk(arr, row, col - 1)) {
                        boundaryCount += 1;
                    }
                }
            }
            if (isOk(arr, row + 1, col)) {
                boundaryCount += 1;
            }
        }

        //for maxRow
        else if (row == maxRow && col == 0) {
            boundaryCount += 2;
            if (isOk(arr, row, col + 1)) {
                boundaryCount += 1;
            }
            if (isOk(arr, row - 1, col)) {
                boundaryCount += 1;
            }
        } else if (row == maxRow && col > 0) {
            boundaryCount += 1;
            if (col == maxColumn) {
                boundaryCount += 1;
                if (isOk(arr, row, col - 1)) {
                    boundaryCount += 1;
                }
            }
            if (isOk(arr, row - 1, col)) {
                boundaryCount += 1;
            }
        }
        //rest
        else if (col == 0 || col == maxColumn) {
            boundaryCount += 1;
            if (isOk(arr, row - 1, col)) {
                boundaryCount++;
            }
            if (col == 0) {
                if (isOk(arr, row, col + 1)) {
                    boundaryCount++;
                }
            } else {
                if (isOk(arr, row, col - 1)) {
                    boundaryCount++;
                }
            }
            if (isOk(arr, row + 1, col)) {
                boundaryCount++;
            }
        } else {
            if (isOk(arr, row, col - 1)) {
                boundaryCount++;
            }
            if (isOk(arr, row - 1, col)) {
                boundaryCount++;
            }
            if (isOk(arr, row, col + 1)) {
                boundaryCount++;
            }
            if (isOk(arr, row + 1, col)) {
                boundaryCount++;
            }
        }

        return boundaryCount;
    }

    private static boolean isOk(String arr[][], int row, int col) {
        boolean isOk = false;
        if (arr[row][col].equals(".")) {
            if (row > 0 && row < maxRow && col > 0 && col < maxColumn) {
                if (arr[row][col - 1].equals(".")) {
                    isOk = true;
                }
                if (arr[row - 1][col].equals(".")) {
                    isOk = true;
                }
                if (arr[row][col + 1].equals(".")) {
                    isOk = true;
                }
                if (arr[row + 1][col].equals(".")) {
                    isOk = true;
                }
            } else {
                isOk = true;
            }
//            isOk = isNotEnclosed(arr, row, col);
        }
        return isOk;
    }

    private static boolean isNotEnclosed(String arr[][], int row, int col) {
        if (arr[row][col].equals("#")) {
            return false;
        } else if (row <= 0 || row >= maxRow || col <= 0 || col >= maxColumn) {
            return true;
        } else {
            if (isNotEnclosed(arr, row, col - 1)) {
                return true;
            }
//            if (isNotEnclosed(arr, row - 1, col)) {
//                return true;
//            }
//            if (isNotEnclosed(arr, row, col + 1)) {
//                return true;
//            }
//            if (isNotEnclosed(arr, row + 1, col)) {
//                return true;
//            }
        }
        return false;
    }

}
