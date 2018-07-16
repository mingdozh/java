package net.winter.spring.boot.example;

public class GreetingResponse {
    private long id;

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    private String desc;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
