package dinhnguyen.techs.stock.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dinhnguyen.techs.commons.forms.PaymentStatus;
import dinhnguyen.techs.commons.forms.SagaTransaction;
import dinhnguyen.techs.commons.kafka.EventHandler;
import dinhnguyen.techs.stock.repository.StockRepository;

@Service
public class StockProcessing implements EventHandler<SagaTransaction> {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private StockRepository stockRepository;
	@Autowired
	ObjectMapper objectMapper;

	@Override
	@KafkaListener(topics = "stock", groupId = "ms-microservice")
	public void handleEvent(String jsonSaga) {

		System.out.println(" Stock Process ");
		SagaTransaction saga;
		try {
			saga = this.objectMapper.readValue(jsonSaga, SagaTransaction.class);
			// Handle publisher: Call this.send(object) to process next step it you have next step:
			// Handle subscribe:
			dinhnguyen.techs.stock.entity.Stock stock = this.stockRepository.findById(1l).get();
			if (stock.getRemain().equals("YES")) {
				System.out.println(" Transaction finish success - Send feedback to previous step ");
				saga.getPayment().setStatus(PaymentStatus.PAYMENT_SUCCESS);
				this.feedback(this.objectMapper.writeValueAsString(saga));
			} else {
				System.out.println(" Transaction fail - Send feedback to previous step to rollback data :");
				saga.getPayment().setStatus(PaymentStatus.PAYMENT_FAIL);
				this.feedback(this.objectMapper.writeValueAsString(saga));
			}

		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	// Handle publisher: Nothing
	@Override
	public void send(SagaTransaction saga) {
		System.out.println(" Do it when you have next step of transaction !");

	}

	// Handle subscribe:
	@Override
	public void feedback(String jsonSaga) {
		kafkaTemplate.send("payment", jsonSaga);
	}

}
