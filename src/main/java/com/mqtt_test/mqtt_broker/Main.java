package com.mqtt_test.mqtt_broker;

import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttTopicSubscription;

import java.util.ArrayList;
import java.util.List;
/**
 *  This is a simple implementation of a mqtt broker using
 *  http://vertx.io/docs/vertx-mqtt-server/java/
 */
public class Main {

    public static void main(String[] args) {
        MqttServer mqttServer = MqttServer.create(Vertx.vertx());
        init(mqttServer);
    }

    private static void init(MqttServer mqttServer) {
        mqttServer.endpointHandler(endpoint -> {
                System.out.println("MQTT client [" + endpoint.clientIdentifier() + "] request to connect, clean session = " + endpoint.isCleanSession());
                endpoint.accept(false);
                handleSubscription(endpoint);
                handleUnsubscription(endpoint);
                publishHandler(endpoint);
                handleClientDisconnect(endpoint);
            }).listen(ar -> {
                if (ar.succeeded()) {
                    System.out.println("MQTT server is listening on port " + ar.result().actualPort());
                } else {
                    System.out.println("Error on starting the server");
                    ar.cause().printStackTrace();
                }
            });
    }
    private static void handleSubscription(MqttEndpoint endpoint) {
        endpoint.subscribeHandler(subscribe -> {
            List grantedQosLevels = new ArrayList < > ();
            for (MqttTopicSubscription s: subscribe.topicSubscriptions()) {
                System.out.println("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());
                grantedQosLevels.add(s.qualityOfService());
            }
                endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
            });
    }
    private static void handleUnsubscription(MqttEndpoint endpoint) {
        endpoint.unsubscribeHandler(unsubscribe -> {
            for (String t: unsubscribe.topics()) {
                System.out.println("Unsubscription for " + t);
            }
                endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
            });
    }
    private static void publishHandler(MqttEndpoint endpoint) {
        endpoint.publishHandler(message -> {
                System.out.println("Message published by : "+message.payload());
                endpoint.publish(message.topicName(),message.payload(),message.qosLevel(),message.isDup(),message.isRetain());
                endpoint.publishAcknowledge(message.messageId());
            }).publishReleaseHandler(messageId -> {
                endpoint.publishComplete(messageId);
            });
    }
    private static void handleClientDisconnect(MqttEndpoint endpoint) {
        endpoint.disconnectHandler(h -> {
            System.out.println("The remote client has closed the connection.");
            });
    }
}
