package dinhnguyen.techs.order.event;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dinhnguyen.techs.commons.forms.OrderStatus;
import dinhnguyen.techs.commons.forms.Payment;
import dinhnguyen.techs.commons.forms.PaymentStatus;
import dinhnguyen.techs.commons.forms.SagaTransaction;
import dinhnguyen.techs.commons.kafka.EventHandler;

@Service
public class OrderProcessing implements EventHandler<SagaTransaction> {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	ObjectMapper objectMapper;

	@Override
	@KafkaListener(topics = "order", groupId = "ms-microservice")
	public void handleEvent(String sagaJson) {

		System.out.println(" order process  ");
		SagaTransaction saga;
		try {
			// handle publisher
			saga = this.objectMapper.readValue(sagaJson, SagaTransaction.class);
			OrderStatus status = saga.getOrder().getStatus();
			if (OrderStatus.New_Order.equals(status)) {
				System.out.println(" Save data into DB and send object to Kafka to process next step  ");
				Payment payment = new Payment();
				payment.setPaymentId(UUID.randomUUID().toString());
				payment.setStatus(PaymentStatus.PAYMENT_NEW);
				saga.setPayment(payment);
				this.send(saga);
			} else {
				
				// Handle subscribe: Send feedback to previous step to commit or rollback data
				this.feedback(sagaJson);
			}
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	// Handle publisher
	@Override
	public void send(SagaTransaction saga) {
		try {
			kafkaTemplate.send("payment", this.objectMapper.writeValueAsString(saga));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	// Handle subscribe
	@Override
	public void feedback(String jsonSaga) {
		SagaTransaction saga = null;
		try {
			saga = this.objectMapper.readValue(jsonSaga, SagaTransaction.class);
			if (OrderStatus.Reject_Order.equals(saga.getOrder().getStatus())) {
				System.out.println(" Rollback DB in Order Service  ");

			} else {
				if (OrderStatus.Accept_Order.equals(saga.getOrder().getStatus())) {
					System.out.println(" Udate DB in Order Service , transaction finish success:");
				}
			}
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

}
