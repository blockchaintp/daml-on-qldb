package com.blockchaintp.daml;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

  private static final int DEFAULT_POLL_INTERVAL_SECONDS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(QldbUpdateWatcher.class);
  private long offset;
  private UnicastProcessor<Tuple2<Offset, Update>> processor;

  private ScheduledExecutorService executorPool;
  private QldbSession session;
  private long hbCount;

  public QldbUpdateWatcher(long startingOffset, QldbSession session, ScheduledExecutorService executorPool) {
    this.offset = startingOffset;
    this.session = session;
    this.processor = UnicastProcessor.create();
    this.executorPool = executorPool;
  }

  public List<QldbDamlLogEntry> fetchNextLogEntries(TransactionExecutor txn) throws IOException {
    List<QldbDamlLogEntry> retList = new ArrayList<>();
    QldbDamlLogEntry log = QldbDamlLogEntry.getNextLogEntry(txn, this.offset);
    while (log != null) {
      retList.add(log);
      this.offset = log.getOffset();
      log = QldbDamlLogEntry.getNextLogEntry(txn, this.offset);
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
    this.session.execute(txn -> {
      try {
        List<QldbDamlLogEntry> newEntries = this.fetchNextLogEntries(txn);
        boolean updatesSent = false;
        for (QldbDamlLogEntry e : newEntries) {
          Collection<Update> updates = JavaConverters
              .asJavaCollection(KeyValueConsumption.logEntryToUpdate(e.damlLogEntryId(), e.damlLogEntry()));
          long updateInLogEntryCount = 1;
          for (Update u : updates) {
            Offset thisOffset = Offset.apply(new long[] { e.getOffset(), updateInLogEntryCount++ });
            this.processor.onNext(Tuple2.apply(thisOffset, u));
            updatesSent = true;
          }
          this.offset = e.getOffset();
          this.hbCount = 0;
        }
        if (!updatesSent) {
          Offset thisOffset = Offset.apply(new long[] { this.offset, this.hbCount++ });
          LOG.info("Sending heartbeat at offset {}", thisOffset);
          Tuple2.apply(thisOffset,new Heartbeat(this.getCurrentRecordTime()));
        }
      } catch (IOException ioe) {
        LOG.error("Error fetching log entries at {}", this.offset);
      }
    }, (retryAttempt) -> LOG.info("Retrying due to OCC conflict"));

    this.executorPool.schedule(this, DEFAULT_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
  }
}
