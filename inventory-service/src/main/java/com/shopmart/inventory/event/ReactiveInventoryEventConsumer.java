package com.shopmart.inventory.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shopmart.inventory.service.ProductService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveInventoryEventConsumer {

    private final ProductService productService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;
    private Disposable disposable;

    @PostConstruct
    public void init() {
        ReceiverOptions<String, OrderEvent> receiverOptions = ReceiverOptions.<String, OrderEvent>create(kafkaProperties.buildConsumerProperties(null))
                .subscription(Collections.singletonList(KafkaTopics.ORDER));

        disposable = KafkaReceiver.create(receiverOptions)
                .receive()
                .flatMap(this::processRecord)
                .subscribe();
    }

    private Mono<Void> processRecord(ReceiverRecord<String, OrderEvent> record) {
        return Mono.fromRunnable(() -> {
            try {
                OrderEvent event = record.value();
                log.info("Reactive Inventory received event: {}", event);

                if (event.getType() == SagaEventType.ORDER_CREATED) {
                    try {
                        productService.decreaseStock(event.getProductId(), event.getQuantity());
                        
                        OrderEvent nextEvent = OrderEvent.builder()
                                .orderId(event.getOrderId())
                                .productId(event.getProductId())
                                .quantity(event.getQuantity())
                                .amount(event.getAmount())
                                .type(SagaEventType.INVENTORY_RESERVED)
                                .message("Inventory reserved successfully")
                                .build();
                        kafkaTemplate.send(KafkaTopics.ORDER, nextEvent);
                    } catch (Exception e) {
                        log.error("Failed to reserve inventory: {}", e.getMessage());
                        OrderEvent nextEvent = OrderEvent.builder()
                                .orderId(event.getOrderId())
                                .productId(event.getProductId())
                                .quantity(event.getQuantity())
                                .amount(event.getAmount())
                                .type(SagaEventType.INVENTORY_FAILED)
                                .message(e.getMessage())
                                .build();
                        kafkaTemplate.send(KafkaTopics.ORDER, nextEvent);
                    }
                } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
                    try {
                        productService.increaseStock(event.getProductId(), event.getQuantity());
                        log.info("Inventory rolled back for orderId: {}", event.getOrderId());
                    } catch (Exception e) {
                        log.error("Failed to rollback inventory: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("Error processing record: {}", e.getMessage());
            } finally {
                record.receiverOffset().acknowledge();
            }
        });
    }

    @PreDestroy
    public void destroy() {
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }
}
