package dinhnguyen.techs.shopping.controller;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dinhnguyen.techs.commons.forms.Order;
import dinhnguyen.techs.commons.forms.OrderStatus;
import dinhnguyen.techs.commons.forms.SagaTransaction;

@RestController
public class ShoppingController {

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@PostMapping("/order/create")
	public void createOrder() {
		SagaTransaction saga = new SagaTransaction();
		saga.setId(UUID.randomUUID().toString());
		Order order = new Order();
		order.setStatus(OrderStatus.New_Order);
		saga.setOrder(order);
		try {
			kafkaTemplate.send("order", objectMapper.writeValueAsString(saga));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

}
