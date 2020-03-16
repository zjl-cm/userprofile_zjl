/**
 * Author itcast
 * Date 2019/12/7 11:55
 * Desc 演示Java8中接口新特性的思想
 */
public class TestInterface {
    public static void main(String[] args) {
        new HuaWei().call();
        new HuaWei().video();

        new C().m();

        Phone.m();
        //HuaWei.m();//没有

    }
}

interface Phone{
    void call();
    //void video();//接口升级添加新功能,导致所有的实现类都需要实现该抽象方法
    //在Java8之后接口增加了新特性,支持接口默认方法
    //那么在接口升级的时候就很方便了!
    default void video(){
        System.out.println("video");
    }

    //在Java8之后接口中还支持静态方法
    static void m(){
        System.out.println("static m");
    }

    //我们之前编写一些工具类的时候,让别人可以用类名.静态方法名调用
    //为了避免别人去new这个工具类,可能需要私有化构造方法/将该工具类置为abstract,不让别人new
    //而在Java8中就可以直接使用接口搞定
    //也就是说在Java8之后,更应该优先使用接口了,而不是使用抽象类
}

class IPhone implements Phone{
    @Override
    public void call() {
        System.out.println("IPone call");
    }
}
class HuaWei implements Phone{
    @Override
    public void call() {
        System.out.println("HuaWei call");
    }
}


interface A{
    default void m(){
        System.out.println("Am");
    }
}
interface B{
    default void m(){
        System.out.println("Bm");
    }
}

class C implements A,B{
    @Override
    public void m() {
        //System.out.println("Cm");
        A.super.m();
    }
}