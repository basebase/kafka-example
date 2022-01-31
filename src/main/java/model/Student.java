package model;

/**
 * @Author: xiaomoyu
 * @Date: 2022/01/31 10:54:38
 * @Description:
 */
public class Student {

    private String name;
    private Integer age;
    private String city;
    private Integer room;

    public Student(String student) {
        String[] tokens = student.split(",");
        this.name = tokens[0];
        this.age = Integer.parseInt(tokens[1]);
        this.city = tokens[2];
        this.room = Integer.parseInt(tokens[3]);
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", city='" + city + '\'' +
                ", room=" + room +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Integer getRoom() {
        return room;
    }

    public void setRoom(Integer room) {
        this.room = room;
    }
}
