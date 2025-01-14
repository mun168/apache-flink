package Dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class Transaction {
    private String transactionId;
    private String producerId;
    private String productName;
    private Double price;
    private int productQuantity;
    private String productCategory;
    private String productBrand;
    private double totalAmount;
    private String currency;
    private String customerId;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private Timestamp transactionDate;
    private String paymentMethod;
}
