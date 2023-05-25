package dinhnguyen.techs.stock.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock")
public class Stock {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "remain")
	private String remain;

	public Stock() {
		super();
	}

	public Stock(Long id, String remain) {
		super();
		this.id = id;
		this.remain = remain;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getRemain() {
		return remain;
	}

	public void setRemain(String remain) {
		this.remain = remain;
	}

}
