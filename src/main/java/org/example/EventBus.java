package org.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EventBus {
    private static final Map<String, Map<String, List<EventSubscriber>>> subscribers = new HashMap<>();
    private static final Map<String, List<Event>> eventLog = new HashMap<>();
    private static final Map<String, Integer> lastUnconsumedEventPointer = new HashMap<>();
    private static final int MAX_EVENT_LOG_SIZE = 10; // Example size

    public static void subscribe(String eventType, String partition, EventSubscriber subscriber) {
        subscribers.computeIfAbsent(partition, k -> new HashMap<>())
                .computeIfAbsent(eventType, k -> new ArrayList<>())
                .add(subscriber);
        eventLog.computeIfAbsent(partition, k -> new ArrayList<>());
        lastUnconsumedEventPointer.putIfAbsent(partition, -1);
        System.out.println("Subscriber subscribed to eventType: " + eventType + " in partition: " + partition);
    }

    public static void publish(String eventType, String data, String partition) {
        Event event = new Event(eventType, data, partition);
        System.out.println("Publishing event: " + event);

        List<Event> partitionLog = eventLog.computeIfAbsent(partition, k -> new ArrayList<>());
        if (partitionLog.size() >= MAX_EVENT_LOG_SIZE) {
            partitionLog.remove(0);
        }
        partitionLog.add(event);
        lastUnconsumedEventPointer.put(partition, (partitionLog.size() - 1 + MAX_EVENT_LOG_SIZE) % MAX_EVENT_LOG_SIZE);


        Map<String, List<EventSubscriber>> partitionSubscribers = subscribers.get(partition);
        if (partitionSubscribers != null) {
            List<EventSubscriber> eventTypeSubscribers = partitionSubscribers.get(eventType);
            if (eventTypeSubscribers != null) {
                for (EventSubscriber sub : eventTypeSubscribers) {
                    sub.handleEvent(event);
                }
            }
        }
    }

    public static void rollback(String partition, int steps) {
        List<Event> partitionLog = eventLog.get(partition);
        if (partitionLog == null || partitionLog.isEmpty()) {
            System.out.println("No events to rollback in partition: " + partition);
            return;
        }

        int currentPointer = lastUnconsumedEventPointer.getOrDefault(partition, -1);
        if (currentPointer == -1) {
            System.out.println("No events consumed yet in partition: " + partition);
            return;
        }

        int newPointer = (currentPointer - steps + partitionLog.size()) % partitionLog.size();
        lastUnconsumedEventPointer.put(partition, newPointer);
        System.out.println("Rolled back partition '" + partition + "' by " + steps + " steps. New pointer at index: " + newPointer);
        System.out.println("Last unconsumed event for partition '" + partition + "' is now: " + partitionLog.get(newPointer));
    }

    public static Event getLastUnconsumedEvent(String partition) {
        List<Event> partitionLog = eventLog.get(partition);
        if (partitionLog == null || partitionLog.isEmpty()) {
            return null;
        }
        int pointer = lastUnconsumedEventPointer.getOrDefault(partition, -1);
        if (pointer == -1 || pointer >= partitionLog.size()) {
            return null;
        }
        return partitionLog.get(pointer);
    }
}