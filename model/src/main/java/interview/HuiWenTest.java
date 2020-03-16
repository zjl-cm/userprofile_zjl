package interview;

/**
 * Author itcast
 * Date 2020/2/28 16:32
 * Desc 编写代码完成回文串判断
 * 编写一个程序用来判断某一个字符串是否是回文串
 * 如传入: "20200202"就是一个好日子,是回文串,返回true
 * "上海自来水来自海上" 返回true
 * "13666666631"  返回true
 * 如传入:"20200228",就不是一个回文串,返回false
 * 简单一点不考虑标点,即给定的字符串中没有标点
 */
public class HuiWenTest {
    public static void main(String[] args) {
        String text = "20200202";
        boolean flag = isHuiWen(text);
        System.out.println(flag);//true
        System.out.println(isHuiWen("20200228"));//false
        System.out.println(isHuiWen("上海自来水来自海上"));//true
    }

    /**
     * 判断参数是否是回文串
     * @param text
     * @return 判断结果,是为true
     */
    private static boolean isHuiWen(String text) {
        //不论字符串是奇数长度还是偶数长度,只要是回文串都满足:
        //第n位==倒数第n位
        //121
        //1221
        //那么现在只需要将字符串的第n位和倒数第n位比较即可,如果不相等直接返回false
        //如果所有位比完了都相等,返回true
        char[] chars = text.toCharArray();//将字符串转为字符数组
        for(int i= 0; i < chars.length / 2; i++) {
            //第i位和倒数第i位比较
            if (chars[i] != chars[chars.length-1-i]){
                return  false; //如果第i位和倒数第i位不相等则直接返回false
            }
        }
        return true;//for循环走完了,也就是比完了,都相等,那就返回true
    }
}
