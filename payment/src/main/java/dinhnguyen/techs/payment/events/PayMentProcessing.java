package dinhnguyen.techs.payment.events;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dinhnguyen.techs.commons.forms.OrderStatus;
import dinhnguyen.techs.commons.forms.PaymentStatus;
import dinhnguyen.techs.commons.forms.SagaTransaction;
import dinhnguyen.techs.commons.forms.StockStatus;
import dinhnguyen.techs.commons.kafka.EventHandler;

@Service
public class PayMentProcessing implements EventHandler<SagaTransaction> {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	ObjectMapper objectMapper;

	@Override
	@KafkaListener(topics = "payment", groupId = "ms-microservice")
	public void handleEvent(String jsonSaga) {

		System.out.println(" Payment Process ");
		SagaTransaction saga;
		try {
			// Handle publisher:
			saga = this.objectMapper.readValue(jsonSaga, SagaTransaction.class);
			if (saga.getPayment().getStatus().equals(PaymentStatus.PAYMENT_NEW)) {
				System.out.println(" Save data into DB and send object to Kafka to process next step  ");
				dinhnguyen.techs.commons.forms.Stock stock = new dinhnguyen.techs.commons.forms.Stock();
				stock.setStatus(StockStatus.NEW_STOCK);
				saga.setStock(stock);
				
				this.send(saga);
			} else {
				// Handle subscribe: Send feedback to previous step to commit or rollback data
				this.feedback(jsonSaga);
			}

		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	// Handle publisher:
	@Override
	public void send(SagaTransaction saga) {
		try {
			kafkaTemplate.send("stock", this.objectMapper.writeValueAsString(saga));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	// Handle subscribe
	@Override
	public void feedback(String jsonSaga) {
		// Check order condition to accept or reject
		SagaTransaction saga = null;
		try {
			saga = this.objectMapper.readValue(jsonSaga, SagaTransaction.class);
			if (saga.getPayment().getStatus().equals(PaymentStatus.PAYMENT_FAIL)) {
				System.out.println(" Rollback DB in Payment Service  ");
				saga.getOrder().setStatus(OrderStatus.Reject_Order);
			} else {
				System.out.println(" udate db in Payment Service , save ok thanh cong ");
				saga.getOrder().setStatus(OrderStatus.Accept_Order);
			}
			kafkaTemplate.send("order", this.objectMapper.writeValueAsString(saga));

		} catch (JsonMappingException e1) {
			e1.printStackTrace();
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();
		}

	}

}
