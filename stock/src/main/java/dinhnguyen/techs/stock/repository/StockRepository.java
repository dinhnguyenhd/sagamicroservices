package dinhnguyen.techs.stock.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import dinhnguyen.techs.stock.entity.Stock;

@Repository
public interface StockRepository extends JpaRepository<Stock, Long> {

}
