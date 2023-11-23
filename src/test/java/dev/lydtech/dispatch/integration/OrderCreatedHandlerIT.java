package dev.lydtech.dispatch.integration;

import dev.lydtech.dispatch.DispatchConfiguration;
import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class OrderCreatedHandlerIT {
    private final static String ORDER_CREATED_TOPIC = "order.created";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private  KafkaTestOCListener testOCListener;

      @Autowired
      private  KafkaTestOCListener1 testOCListener1;

    @Configuration
    static class TestConfigForOrderCreated {

        @Bean
        public  KafkaTestOCListener testOCListener() {
            return new KafkaTestOCListener();
        }
      @Bean
        public KafkaTestOCListener1 testOCListener1() { return new KafkaTestOCListener1();}
    }

    @KafkaListener(groupId = "OrderCreateTest", topics = { ORDER_CREATED_TOPIC})
    public static class KafkaTestOCListener {
        AtomicInteger orderCreatedCounter = new AtomicInteger(0);


        @KafkaHandler
        void receiveOrderCreated(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
            log.info("Received Order Created in partition "+ partition, " key: " + key + " - payload: " + payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            orderCreatedCounter.incrementAndGet();
        }


    }

    @KafkaListener(groupId = "OrderCreateTest", topics = { ORDER_CREATED_TOPIC})
    public static class KafkaTestOCListener1 {
        AtomicInteger orderCreatedCounter = new AtomicInteger(0);


       @KafkaHandler
        void receiveOrderCreated(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
           log.info("Received Order Created in partition "+ partition, " key: " + key + " - payload: " + payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            orderCreatedCounter.incrementAndGet();
        }


    }
    @BeforeEach
    public void setUp() {
        testOCListener.orderCreatedCounter.set(0);
      // testOCListener1.orderCreatedCounter.set(0);

        // Wait until the partitions are assigned.  The application listener container has one topic and the test
        // listener container has multiple topics, so take that into account when awaiting for topic assignment.
      registry.getListenerContainers().stream()
               .forEach(container -> {
                   System.out.println("topics details :::: " + container.getContainerProperties() );
                   ContainerTestUtils.waitForAssignment(container,
                            container.getContainerProperties().getTopics().length* embeddedKafkaBroker.getPartitionsPerTopic());});

    }


    @Test
    public void testOrderCreatedFlow() throws Exception {
        UUID uuid = randomUUID();
        UUID uuid1 = randomUUID();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(uuid, "my-item-"+ uuid.toString());
        sendMessage(ORDER_CREATED_TOPIC, uuid.toString(), orderCreated);

        OrderCreated orderCreated1 = TestEventData.buildOrderCreatedEvent(uuid1, "my-item-"+ uuid1.toString());
        sendMessage(ORDER_CREATED_TOPIC, uuid1.toString(), orderCreated1);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testOCListener.orderCreatedCounter::get, equalTo(1));
      // await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
          //    .until(testOCListener1.orderCreatedCounter::get, equalTo(1));

    }

    private void sendMessage(String topic, String key, Object data) throws Exception {
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }

}
