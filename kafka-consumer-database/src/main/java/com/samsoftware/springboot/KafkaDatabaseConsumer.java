package com.samsoftware.springboot;

import com.samsoftware.springboot.entity.WikimediaData;
import com.samsoftware.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    private final WikimediaDataRepository repository;

    public KafkaDatabaseConsumer(WikimediaDataRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            topics = "${spring.kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(String eventMessage) {
        LOGGER.info("Message received -> {}", eventMessage);

        WikimediaData wikimediaData = new WikimediaData();

        wikimediaData.setWikiEventData(eventMessage);
        repository.save(wikimediaData);
    }
}
