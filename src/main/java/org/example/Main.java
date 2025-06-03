package org.example;
public class Main {
    public static void main(String[] args) {
        OrderService service = new OrderService();

        String partitionA = "WarehouseA";
        String partitionB = "WarehouseB";

        SpecificEventSubscriber sub1_A = new SpecificEventSubscriber("Sub1_A", partitionA, (event, data) -> {
            if (event.equals("OrderPlaced")) {
                System.out.println("Sub1_A: Processing new order " + data + " from " + partitionA);
            }
        });

        SpecificEventSubscriber sub2_A = new SpecificEventSubscriber("Sub2_A", partitionA, (event, data) -> {
            if (event.equals("OrderConfirmed")) {
                System.out.println("Sub2_A: Confirming order " + data + " from " + partitionA);
            }
        });

        SpecificEventSubscriber sub3_A = new SpecificEventSubscriber("Sub3_A", partitionA, (event, data) -> {
            System.out.println("Sub3_A: General handler for " + event + " of order " + data + " from " + partitionA);
        });

        SpecificEventSubscriber sub1_B = new SpecificEventSubscriber("Sub1_B", partitionB, (event, data) -> {
            if (event.equals("OrderPlaced")) {
                System.out.println("Sub1_B: Processing new order " + data + " from " + partitionB);
            }
        });

        SpecificEventSubscriber sub2_B = new SpecificEventSubscriber("Sub2_B", partitionB, (event, data) -> {
            if (event.equals("OrderConfirmed")) {
                System.out.println("Sub2_B: Confirming order " + data + " from " + partitionB);
            }
        });

        EventBus.subscribe("OrderPlaced", partitionA, sub1_A);
        EventBus.subscribe("OrderConfirmed", partitionA, sub2_A);
        EventBus.subscribe("OrderPlaced", partitionA, sub3_A);
        EventBus.subscribe("OrderConfirmed", partitionA, sub3_A);

        EventBus.subscribe("OrderPlaced", partitionB, sub1_B);
        EventBus.subscribe("OrderConfirmed", partitionB, sub2_B);

        System.out.println("\n--- Placing Orders ---");
        service.placeOrder(101, partitionA);
        service.placeOrder(202, partitionB);
        service.placeOrder(103, partitionA);
        service.placeOrder(204, partitionB);
        service.placeOrder(105, partitionA);


        System.out.println("\n--- Current Last Unconsumed Event ---");
        System.out.println("Partition A: " + EventBus.getLastUnconsumedEvent(partitionA));
        System.out.println("Partition B: " + EventBus.getLastUnconsumedEvent(partitionB));


        System.out.println("\n--- Performing Rollback ---");
        EventBus.rollback(partitionA, 1);
        System.out.println("After rollback (Partition A): " + EventBus.getLastUnconsumedEvent(partitionA));

        EventBus.rollback(partitionB, 1);
        System.out.println("After rollback (Partition B): " + EventBus.getLastUnconsumedEvent(partitionB));

        System.out.println("\n--- Placing more orders after rollback ---");
        service.placeOrder(106, partitionA);
        service.placeOrder(208, partitionB);

        System.out.println("\n--- Final Last Unconsumed Event ---");
        System.out.println("Partition A: " + EventBus.getLastUnconsumedEvent(partitionA));
        System.out.println("Partition B: " + EventBus.getLastUnconsumedEvent(partitionB));

    }
}