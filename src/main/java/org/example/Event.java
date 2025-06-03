package org.example;

public class Event{
    private final String eventType;
    private final String data;
    private final String partition;

    public Event(String eventType, String data, String partition) {
        this.eventType = eventType;
        this.data = data;
        this.partition = partition;
    }

    public String getEventType() {
        return eventType;
    }

    public String getData() {
        return data;
    }

    public String getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "Event [type=" + eventType + ", data=" + data + ", partition=" + partition + "]";
    }
}