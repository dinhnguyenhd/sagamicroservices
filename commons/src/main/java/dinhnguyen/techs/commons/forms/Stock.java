package dinhnguyen.techs.commons.forms;

public class Stock {
	private String stockId;
	private String account;
	private StockStatus status;
	private int test;

	public String getStockId() {
		return stockId;
	}

	public void setStockId(String stockId) {
		this.stockId = stockId;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public int getTest() {
		return test;
	}

	public void setTest(int test) {
		this.test = test;
	}

	public StockStatus getStatus() {
		return status;
	}

	public void setStatus(StockStatus status) {
		this.status = status;
	}
	

}
