package dinhnguyen.techs.commons.forms;

public class SagaTransaction {

	private String id;
	private Order order;
	private Payment payment;
	private Stock stock;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Order getOrder() {
		return order;
	}
	public void setOrder(Order order) {
		this.order = order;
	}
	public Payment getPayment() {
		return payment;
	}
	public void setPayment(Payment payment) {
		this.payment = payment;
	}
	public Stock getStock() {
		return stock;
	}
	public void setStock(Stock stock) {
		this.stock = stock;
	}
	

}
