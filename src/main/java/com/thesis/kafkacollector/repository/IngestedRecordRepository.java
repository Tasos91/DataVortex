package com.thesis.kafkacollector.repository;

import com.thesis.kafkacollector.entity.IngestedRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IngestedRecordRepository extends JpaRepository<IngestedRecord, Long> {
}