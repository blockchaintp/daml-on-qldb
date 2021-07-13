import com.blockchaintp.daml.serviceinterface.Key;
import com.blockchaintp.daml.serviceinterface.Value;
import com.blockchaintp.daml.serviceinterface.exception.StoreReadException;
import com.blockchaintp.daml.serviceinterface.exception.StoreWriteException;
import com.blockchaintp.daml.stores.s3.S3Store;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3StoreTest {

  @Test
  void get_operations_compose_s3_errors_in_store_errors() throws StoreReadException {
    var s3 = mock(S3AsyncClient.class);
    when(s3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
      .thenReturn(CompletableFuture.supplyAsync(() -> {
        throw ApiCallTimeoutException.create(1000);
      }));

    var builder = mock(S3AsyncClientBuilder.class);

    when(builder.build())
      .thenReturn(s3);

    var store = new S3Store("", "", builder, x -> x, x -> x);

    var ex = Assertions.assertThrows(
      StoreReadException.class,
      () -> store.get(Collections.singletonList(new Key<>("")))
    );

    Assertions.assertInstanceOf(
      ApiCallTimeoutException.class,
      ex.getCause()
    );
  }

  @Test
  void pet_operations_compose_s3_errors_in_store_errors() throws StoreReadException {
    var s3 = mock(S3AsyncClient.class);
    when(s3.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
      .thenReturn(CompletableFuture.supplyAsync(() -> {
        throw ApiCallTimeoutException.create(1000);
      }));

    var builder = mock(S3AsyncClientBuilder.class);

    when(builder.build())
      .thenReturn(s3);

    var store = new S3Store("", "", builder, x -> x, x -> x);

    var ex = Assertions.assertThrows(
      StoreWriteException.class,
      () -> store.put(Collections.singletonList(new AbstractMap.SimpleEntry<>(
          new Key<>(""),
          new Value<>(new byte[]{})
        ))
      ));

    Assertions.assertInstanceOf(
      ApiCallTimeoutException.class,
      ex.getCause()
    );
  }
}
