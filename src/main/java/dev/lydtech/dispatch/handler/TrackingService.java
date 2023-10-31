package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.message.*;
import dev.lydtech.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class TrackingService {

    private static final String TRACKING_STATUS_TOPIC = "tracking.status";
    private final KafkaTemplate<String, Object> kafkaProducer;
    private final DispatchService dispatchService;
    @KafkaListener(
            id = "orderDispatchClient",
            topics = "dispatch.tracking",
            groupId = "dispatch.order.dispatch.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(DispatchPreparing dispatchPreparing) throws Exception {

        log.info("Dispatch message: " + dispatchPreparing);
        TrackingStatusUpdated status = TrackingStatusUpdated.builder()
                        .orderId(dispatchPreparing.getOrderId())
                        .status(Status.PREPARING)
                          .build();
        kafkaProducer.send(TRACKING_STATUS_TOPIC ,status).get();

    }


}
