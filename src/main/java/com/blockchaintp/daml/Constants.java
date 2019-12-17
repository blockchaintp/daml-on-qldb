package com.blockchaintp.daml;

import com.amazon.ion.system.IonSystemBuilder;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import com.fasterxml.jackson.dataformat.ion.ionvalue.IonValueMapper;

public final class Constants {

  public static final long DEFAULT_POLL_INTERVAL_MS = 10_000L;
  public static final int RETRY_LIMIT = 10;
  public static final IonObjectMapper MAPPER = new IonValueMapper(IonSystemBuilder.standard().build());
  public static final String DAML_LOG_TABLE_NAME = "daml_log";

}
