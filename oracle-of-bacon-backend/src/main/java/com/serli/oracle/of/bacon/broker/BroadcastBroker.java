package com.serli.oracle.of.bacon.broker;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class BroadcastBroker {

    private List<Consumer<Event>> consumerList = new LinkedList<>();

    public void addConsumer(Consumer<Event> eventConsumer){
        consumerList.add(eventConsumer);
    }

    public void publishEvent(Event event){
        consumerList.forEach(eventConsumer -> eventConsumer.accept(event));
    }

    public static class Event {
        public final String name;
        public final String param;

        public Event(String name, String param) {
            this.name = name;
            this.param = param;
        }
    }


}
