package model;

import com.google.gson.Gson;

/**
 * @Author xiaomoyu
 * @Date: 2022/3/2 18:03:46
 * @Description:   购物实体类
 */
public class Shop {
    // 用户ID
    private String id;
    // 用户姓名
    private String name;
    // 用户购买商品
    private String goods;
    // 用户购买商品金额
    private Double amount;
    // 用户购买店铺地址
    private String address;
    // 收银员
    private String cashier;
    // 支付类型
    private String type;
    // 订单时间
    private String createtime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGoods() {
        return goods;
    }

    public void setGoods(String goods) {
        this.goods = goods;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCashier() {
        return cashier;
    }

    public void setCashier(String cashier) {
        this.cashier = cashier;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCreatetime() {
        return createtime;
    }

    public void setCreatetime(String createtime) {
        this.createtime = createtime;
    }

    @Override
    public String toString() {
        return "Shop{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", goods='" + goods + '\'' +
                ", amount=" + amount +
                ", address='" + address + '\'' +
                ", cashier='" + cashier + '\'' +
                ", type='" + type + '\'' +
                ", createtime='" + createtime + '\'' +
                '}';
    }

    public static Shop build(String json) {
        Gson gson = new Gson();
        Shop shop = gson.fromJson(json, Shop.class);
        return shop;
    }

    public Shop make() {
        this.id = "****" + this.id.substring(5, this.id.length() - 1) + "****";
        return this;
    }


}
