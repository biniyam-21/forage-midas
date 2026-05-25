package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaConsumer(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId());

        if (sender == null) {
            return;
        }

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        if (recipient == null) {
            return;
        }

        float amount = transaction.getAmount();

        if (sender.getBalance() < amount) {
            return;
        }

        RestTemplate restTemplate = new RestTemplate();

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = incentive != null
                ? incentive.getAmount()
                : 0;

        sender.setBalance(sender.getBalance() - amount);

        recipient.setBalance(
                recipient.getBalance() + amount + incentiveAmount
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord =
                new TransactionRecord(
                        sender,
                        recipient,
                        amount,
                        incentiveAmount
                );

        transactionRecordRepository.save(transactionRecord);
    }
}