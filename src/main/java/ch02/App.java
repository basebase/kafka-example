package ch02;

/**
 * @Author xiaomoyu
 * @Date: 2022/1/13 14:10:41
 * @Description:    泛型基础学习使用
 */
public class App {
    public static void main(String[] args) {
        // 无法编译, 需要传入一个Fruit实例
//        Plate<Fruit> plate = new Plate<Apple>(new Apple());


        /***
         *    <? extends Class> 使用
         */

        // 上界在Fruit, 可以是Fruit及其子类
//        Plate<? extends Fruit> plate = new Plate<Apple>(new Apple());
//        Plate<? extends Fruit> plate = new Plate<GreenApple>(new GreenApple());

        // 上界在Meat, 无法编译
//        Plate<? extends Meat> plate = new Plate<GreenApple>(new GreenApple());

        // 虽然Pork和Beef都是Meat的子类, 但是如果泛型中写明了类型则需要传入具体的实例, 否则无法编译
//        Plate<? extends Meat> plate = new Plate<Pork>( /* new Pork() */ new Beef() );

        // 在不指定具体类型时, 可以随意写入Meat及其子类
//        Plate<? extends Meat> plate = new Plate<>(new Meat() {});
//        Plate<? extends Meat> plate = new Plate<>(new Beef());


//        Plate<? extends Food> plate = new Plate<>(new Meat() {});
//        Plate<? extends Food> plate = new Plate<>(new GreenApple());


        /***
         *  <? super Class>使用
         */

        // 传入的类型只能是Apple或者其父类, 不允许是其子类, 编译出错
//        Plate<? super Apple> plate = new Plate<GreenApple>(new GreenApple());

        Plate<? super Apple> plate = new Plate<>(new Food());
//        Plate<? super Apple> plate = new Plate<Food>(new Food());


        /****
         * <? extends Class> 不能存储数据, 只能读取数据
         */
//        Plate<? extends Fruit> plate1 = new Plate<Apple>(new Apple());
        Plate<? extends Fruit> plate1 = new Plate<>(new Banana());

        // 下面的set方法全都会报错, 可以简单理解我么无法知道用户究竟会插入什么对象, 可以是Apple也可以是Banana或者是其子类
        // 插入之后, 我们如何进行一个转换呢, 所以不让插入数据
//        plate1.setItem(new GreenApple());
//        plate1.setItem(new Apple());
//        plate1.setItem(new Fruit() {});

        // 但是可以使用get方法, 接收的类型可以是Fruit或者Object类型, 毕竟限制了类的上界
        Fruit item = plate1.getItem();
        // 编译出错
//        Apple item1 =  plate1.getItem();

        System.out.println(plate1);


        /***
         *
         * <? super Class>不能读取数据, 适合存入数据
         */

        Plate<? super Apple> plate2 = new Plate<>(new Apple());
        plate2.setItem(new RedApple());
//        plate2.setItem();

        System.out.println(plate2);
    }
}

// lev.1

/***
 * 食物基类
 */
class Food {

}

//lev.2

/***
 * 水果类
 */
class Fruit extends Food {

}

/***
 * 肉类
 */
class Meat extends Food {

}

// lev.3

/***
 * 香蕉类
 */
class Banana extends Fruit {

}


/***
 * 苹果类
 */
class Apple extends Fruit {

}

/***
 * 猪肉类
 */
class Pork extends Meat {

}

/**
 * 牛肉类
 */
class Beef extends Meat {

}


// lev.4

/***
 * 红苹果类
 */
class RedApple extends Apple {

}

/***
 * 青苹果类
 */
class GreenApple extends Apple {

}

/***
 * 盘子类
 * @param <T>
 */
class Plate<T> {
    private T item ;

    public Plate(T item) {
        this.item = item;
    }

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }
}