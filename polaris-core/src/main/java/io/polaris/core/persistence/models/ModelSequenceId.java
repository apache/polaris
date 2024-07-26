package io.polaris.core.persistence.models;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

@Entity
@Table(name = "POLARIS_SEQUENCE")
public class ModelSequenceId {
  @Id
  @SequenceGenerator(
      name = "sequenceGen",
      sequenceName = "POLARIS_SEQ",
      initialValue = 1000,
      allocationSize = 25)
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sequenceGen")
  private Long id;
}
