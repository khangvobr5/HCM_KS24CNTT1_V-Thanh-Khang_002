package com.shopmart.payment.event;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-group")
    public void consume(OrderEvent event) {
        log.info("Payment received event: {}", event);

        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            try {
                PaymentRequest request = PaymentRequest.builder()
                        .orderId(event.getOrderId())
                        .amount(event.getAmount())
                        .build();
                PaymentResponse response = paymentService.processPayment(request);

                SagaEventType nextEventType = response.getStatus() == PaymentStatus.SUCCESS 
                        ? SagaEventType.PAYMENT_COMPLETED 
                        : SagaEventType.PAYMENT_FAILED;

                OrderEvent nextEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(nextEventType)
                        .message(response.getMessage())
                        .build();
                
                kafkaTemplate.send(KafkaTopics.ORDER, nextEvent);

            } catch (Exception e) {
                log.error("Failed to process payment: {}", e.getMessage());
                OrderEvent nextEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_FAILED)
                        .message(e.getMessage())
                        .build();
                kafkaTemplate.send(KafkaTopics.ORDER, nextEvent);
            }
        }
    }
}
