import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3StoreTest {


  public class Stub {

    public S3AsyncClient get() {
      final var client = mock(S3AsyncClient.class);

      when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(
          CompletableFuture.completedFuture(PutObjectResponse.builder().build())
        );

      return client;
    }
  }
}
