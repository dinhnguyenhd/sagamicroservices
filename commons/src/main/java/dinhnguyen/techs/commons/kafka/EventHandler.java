package dinhnguyen.techs.commons.kafka;

public interface EventHandler<V> {

	public void handleEvent(String order);

	public void send(V v);

	public void feedback(String json);
}
