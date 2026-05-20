package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

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

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-group"
    )
    public void listen(Transaction transaction) {

        if (transaction == null) return;

        // Fetch sender
        UserRecord sender = userRepository
                .findById(transaction.getSenderId())
                .orElse(null);

        if (sender == null) return;

        // Fetch recipient
        UserRecord recipient = userRepository
                .findById(transaction.getRecipientId())
                .orElse(null);

        if (recipient == null) return;

        float amount = transaction.getAmount();

        // Validate balance
        if (sender.getBalance() < amount) return;

        // Update balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        // Persist users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Save transaction record (ONLY ONCE)
        TransactionRecord transactionRecord = new TransactionRecord(
                sender.getId(),
                recipient.getId(),
                amount
        );


        if (sender.getName().equals("waldorf")) {
            System.out.println("Waldorf sender balance: " + sender.getBalance());
        }

        if (recipient.getName().equals("waldorf")) {
            System.out.println("Waldorf recipient balance: " + recipient.getBalance());
        }

        transactionRecordRepository.save(transactionRecord);
    }
}