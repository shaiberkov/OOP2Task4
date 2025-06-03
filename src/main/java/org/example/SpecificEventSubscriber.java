package org.example;

import java.util.function.BiConsumer;

class SpecificEventSubscriber implements EventSubscriber {
    private final String name;
    private final BiConsumer<String, String> callback;
    private final String partition;

    public SpecificEventSubscriber(String name, String partition, BiConsumer<String, String> cb) {
        this.name = name;
        this.partition = partition;
        this.callback = cb;
    }

    @Override
    public void handleEvent(Event event) {
        if (event.getPartition().equals(this.partition)) {
            System.out.println("Subscriber " + name + " received event: " + event.getEventType() + " from partition: " + event.getPartition());
            callback.accept(event.getEventType(), event.getData());
        }
    }

    public String getPartition() {
        return partition;
    }
}