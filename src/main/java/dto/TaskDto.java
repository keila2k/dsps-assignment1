package dto;

public class TaskDto {
    private String type;
    private String data;

    @Override
    public String toString() {
        return "TaskDto{" +
                "type='" + type + '\'' +
                ", data='" + data + '\'' +
                '}';
    }

    public TaskDto(String data, String queueUrl) {
        this.type = "task";
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

}
