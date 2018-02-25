package test;

public class PatternTestV2 {
    private static int maxRow;
    private static int maxColumn;

    public static void main(String[] args) {
        String arr[][] = {
                {"#", ".", "#", "#", "#", "#"},
                {"#", ".", "#", ".", ".", "#"},
                {"#", ".", "#", "#", ".", "#"},
                {"#", ".", "#", "#", "#", "#"},
                {"#", ".", ".", ".", "#", "#"},
                {"#", "#", "#", "#", "#", "#"}
        };
        maxRow = arr[0].length - 1;
        maxColumn = arr.length - 1;
        System.out.println("maxRow = " + maxRow);
        System.out.println("maxColumn = " + maxColumn);

        int count = 0;
        int boundaryCount;
        for (int row = 0; row <= maxRow; row++) {
            for (int col = 0; col <= maxColumn; col++) {
                if (arr[row][col].equals("#")) {
                    boundaryCount = getBoundaryCount(arr, row, col);
//                    System.out.println("row : " + row + " col : " + col + " boundaryCount = " + boundaryCount);
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
        //for 0th and maxRow
        if (row == 0 || row == maxRow) {
            if (col == 0) {
                boundaryCount += 2;
                if (isOkAndNotEnclosed(arr, row, col + 1)) {
                    boundaryCount += 1;
                }
            } else {
                boundaryCount += 1;
                if (col == maxColumn) {
                    boundaryCount += 1;
                }
                if (isOkAndNotEnclosed(arr, row, col - 1)) {
                    boundaryCount += 1;
                }
            }
            if (row == 0 && isOkAndNotEnclosed(arr, row + 1, col)) {
                boundaryCount += 1;
            } else if (row == maxRow && isOkAndNotEnclosed(arr, row - 1, col)) {
                boundaryCount += 1;
            }
        } else {
            if (col != 0) {
                if (isOkAndNotEnclosed(arr, row, col - 1)) {
                    boundaryCount++;
                }
            }
            if (isOkAndNotEnclosed(arr, row + 1, col)) {
                boundaryCount++;
            }
            if (col != maxColumn) {
                if (isOkAndNotEnclosed(arr, row, col + 1)) {
                    boundaryCount++;
                }
            }
            if (isOkAndNotEnclosed(arr, row - 1, col)) {
                boundaryCount++;
            }
            if (col == 0 || col == maxColumn) {
                boundaryCount++;
            }
        }

        return boundaryCount;
    }

    private static boolean isOkAndNotEnclosed(String arr[][], int row, int col) {
        if (arr[row][col].equals("#")) {
            return false;
        } else if (row <= 0 || row >= maxRow || col <= 0 || col >= maxColumn) {
            return true;
        } else {
            if (reduceLeft(arr, row, col)) {
                return true;
            } else if (reduceTop(arr, row, col)) {
                return true;
            } else if (reduceRight(arr, row, col)) {
                return true;
            } else if (reduceDown(arr, row, col)) {
                return true;
            }
        }
        return false;
    }

    private static boolean reduceLeft(String arr[][], int row, int col) {
        if (arr[row][col - 1].equals("#"))
            return false;
        for (int i = col; i >= 0; i--) {
            if (arr[row][i].equals("#")) {
                if (reduceTop(arr, row, i + 1))
                    return true;
                else if (reduceDown(arr, row, i + 1))
                    return true;
                return false;
            }
        }
        return true;
    }

    private static boolean reduceTop(String arr[][], int row, int col) {
        if (arr[row - 1][col].equals("#"))
            return false;
        for (int i = row; i >= 0; i--) {
            if (arr[i][col].equals("#")) {
                if (reduceLeft(arr, i + 1, col))
                    return true;
                else if (reduceRight(arr, i + 1, col))
                    return true;
                return false;
            }
        }
        return true;
    }

    private static boolean reduceRight(String arr[][], int row, int col) {
        if (arr[row][col + 1].equals("#"))
            return false;
        for (int i = col; i <= maxColumn; i++) {
            if (arr[row][i].equals("#")) {
                if (reduceTop(arr, row, i - 1))
                    return true;
                else if (reduceDown(arr, row, i - 1))
                    return true;
                return false;
            }
        }
        return true;
    }

    private static boolean reduceDown(String arr[][], int row, int col) {
        if (arr[row + 1][col].equals("#"))
            return false;
        for (int i = row; i <= maxRow; i++) {
            if (arr[i][col].equals("#")) {
                if (reduceLeft(arr, i - 1, col))
                    return true;
                else if (reduceRight(arr, i - 1, col))
                    return true;
                return false;
            }
        }
        return true;
    }

}
