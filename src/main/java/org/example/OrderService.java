package org.example;

class OrderService {
    public void placeOrder(int orderId, String partition) {
        String data = String.valueOf(orderId);
        System.out.println("Order " + orderId + " has been placed in partition: " + partition);

        EventBus.publish("OrderPlaced", data, partition);
        EventBus.publish("OrderConfirmed", data, partition);
    }
}
