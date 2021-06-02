package com.blockchaintp.daml;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.blockchaintp.daml.exception.NonRecoverableErrorException;
import com.blockchaintp.daml.model.QldbDamlLogEntry;
import com.daml.ledger.participant.state.kvutils.KeyValueConsumption;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.Update;
import com.daml.ledger.participant.state.v1.Update.Heartbeat;
import com.digitalasset.daml.lf.data.Time.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import io.reactivex.processors.UnicastProcessor;
import scala.Tuple2;
import scala.collection.JavaConverters;
import software.amazon.qldb.QldbSession;
import software.amazon.qldb.TransactionExecutor;

public class QldbUpdateWatcher implements Runnable {

  private static final int MAX_LOG_ENTRIES_PER_FETCH = 40;
  private static final int DEFAULT_POLL_INTERVAL_SECONDS = 1;
  private static final Logger LOG = LoggerFactory.getLogger(QldbUpdateWatcher.class);
  private long offset;
  private final UnicastProcessor<Tuple2<Offset, Update>> processor;

  private final ScheduledExecutorService executorPool;
  private final DamlLedger ledger;
  private long hbCount;

  public QldbUpdateWatcher(final long startingOffset, final DamlLedger ledger,
      final ScheduledExecutorService executorPool) {
    this.offset = startingOffset;
    this.ledger = ledger;
    this.processor = UnicastProcessor.create();
    this.executorPool = executorPool;
  }

  public List<QldbDamlLogEntry> fetchNextLogEntries(final TransactionExecutor txn) throws IOException {
    final List<QldbDamlLogEntry> retList = new ArrayList<>();
    QldbDamlLogEntry log = QldbDamlLogEntry.getNextLogEntry(txn, this.ledger, this.offset);
    int countOfLogEntries = 0;
    while (log != null) {
      retList.add(log);
      this.offset = log.getOffset();
      countOfLogEntries++;
      if (countOfLogEntries < MAX_LOG_ENTRIES_PER_FETCH) {
        log = QldbDamlLogEntry.getNextLogEntry(txn, this.ledger, this.offset);
      } else {
        log = null;
      }
    }
    return retList;
  }

  public Source<Tuple2<Offset, Update>, NotUsed> toSource() {
    return Source.fromPublisher(this.processor);
  }

  private Timestamp getCurrentRecordTime() {
    return new Timestamp(Clock.systemUTC().instant().toEpochMilli() * 1_000_000L);
  }

  @Override
  public void run() {
    final QldbSession session = this.ledger.connect();
    try {
      final List<QldbDamlLogEntry> newEntries = new ArrayList<>();
      session.execute(txn -> {
        try {
          List<QldbDamlLogEntry> fetchedEntries = this.fetchNextLogEntries(txn);
          newEntries.addAll(fetchedEntries);
        } catch (IOException e1) {
          throw new NonRecoverableErrorException("Error fetching entries", e1);
        }
      }, (retryAttempts) -> LOG.info("Retrying due to OCC Failure"));
      boolean updatesSent = false;
      final long startOffset = this.offset;
      for (final QldbDamlLogEntry e : newEntries) {
        final Collection<Update> updates = JavaConverters
            .asJavaCollection(KeyValueConsumption.logEntryToUpdate(e.damlLogEntryId(), e.damlLogEntry()));
        long updateInLogEntryCount = 1;
        for (final Update u : updates) {
          final Offset thisOffset = Offset.apply(new long[] { e.getOffset(), updateInLogEntryCount++ });
          this.processor.onNext(Tuple2.apply(thisOffset, u));
          updatesSent = true;
        }
        this.offset = e.getOffset();
      }
      if (this.offset != startOffset) {
        this.hbCount = 0;
      }
      if (!updatesSent) {
        long effectiveOffset = this.offset;
        if (effectiveOffset < 0) {
          effectiveOffset = 0;
        }
        final Offset thisOffset = Offset.apply(new long[] { effectiveOffset, this.hbCount++ });
        LOG.info("Sending heartbeat at offset {}", thisOffset);
        Tuple2.apply(thisOffset, new Heartbeat(this.getCurrentRecordTime()));
      }
    } catch (final Exception ioe) {
      throw new NonRecoverableErrorException("IOException watching logs", ioe);
    }
    session.close();
    this.executorPool.schedule(this, DEFAULT_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }
}
