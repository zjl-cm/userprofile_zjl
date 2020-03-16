package interview;

/**
 * Author itcast
 * Date 2020/2/28 16:50
 * Desc 演示使用递归+回溯完成走迷宫算法
 */
public class ZouMiGongTest {
    public static void main(String[] args) {
        //1.创建一个二维数组表示准备迷宫,默认都是0,表示都可以走
        int[][] map = new int[8][7]; //8行7列

        //2.将数组中的一些值赋值为1,表示准备一些墙
        //上下左右全部赋值为1,表示都是墙
        //上下设置为1
        for (int i = 0; i < 7; i++) {
            map[0][i] = 1;
            map[7][i] = 1;
        }
        //左右设置为1
        for (int i = 0; i < 8; i++) {
            map[i][0] = 1;
            map[i][6] = 1;
        }
        //迷宫中间设置一些墙
        map[3][1] = 1;
        map[3][2] = 1;
        map[3][3] = 1;
        map[3][4] = 1;
        map[2][2] = 1;
        //map[1][2] = 1;

        //3.查看一下迷宫长什么样子
        System.out.println("迷宫地图如下:");
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 7; j++) {
                System.out.print(map[i][j] + " ");
            }
            System.out.println();
        }
        /*
        迷宫地图如下:
        1 1 1 1 1 1 1
        1 0 0 0 0 0 1
        1 0 1 0 0 0 1
        1 1 1 1 1 0 1
        1 0 0 0 0 0 1
        1 0 0 0 0 0 1
        1 0 0 0 0 0 1
        1 1 1 1 1 1 1
         */
        //调用方法获取走迷宫的路径,map表示走哪个迷宫,1,1表示从左上角的0开始走
        getWay(map, 1, 1);

        System.out.println("走迷宫的路径为:");
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 7; j++) {
                System.out.print(map[i][j] + " ");
            }
            System.out.println();
        }
    }

    /**
     * 获取走迷宫路径的方法
     *
     * @param map map表示走哪个迷宫
     * @param i   i和j表示起点位置  i 是行
     * @param j   j是列
     * 约定:
     * 右下角0的位置即[6][5]为出口
     * 1表示墙,0表示可以走且没有走,2表示标记为可以行走路径,3表示走过但是走不通
     * 自定义一个走迷宫的策略:下->右->上->左
     */
    private static boolean getWay(int[][] map, int i, int j) {
        //使用回溯思想,也就是从后往前看,如果[6][5],也就是出口已经被标记为2了,那么走迷宫的路径标记就已经完成了
        if (map[6][5] == 2) {
            return true;
        } else {
            if (map[i][j] == 0) {//0表示可以走且没有走过
                //先假设该点就是走迷宫的路径,也就是应该标记为2
                map[i][j] = 2;
                //按照 策略:下->右->上->左去走
                if (getWay(map, i + 1, j)) {//往下走
                    return true;
                } else if (getWay(map, i, j + 1)) {//往右走
                    return true;
                } else if (getWay(map, i - 1, j)) {//往上
                    return true;
                } else if (getWay(map, i, j - 1)) {//往左
                    return true;
                } else{
                    map[i][j] = 3;
                    return false;
                }
            }else{//map[i][j] != 0 ,表示1或2或3
                return false;
            }
        }
    }
}
