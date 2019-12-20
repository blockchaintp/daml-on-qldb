# daml-on-qldb

## Build

### Using the docker based toolchain
The basic build uses docker to pull down its toolchain and compile.
1. install Docker [https://docs.docker.com/install/]
1. install docker-compose [https://docs.docker.com/compose/install/]

1. Clone the repository.

```$ git clone git@github.com:blockchaintp/daml-on-sawtooth.git```

4. set export the build identifier environment variable.  This is used to distinguish different variations of builds on the same machine.

```$ export ISOLATION_ID=my-local-build```

5. Execute the local build script. This will compile and package all of the java, as well as prepare docker images for local execution.

```$ bin/build.sh```


### Building in an IDE

The docker based toolchain is our standard way of building, but fundamentally this project is a standard maven build with no unusual elements.  So the jar artifacts may be built simply by

```mvn clean package```

See your IDE's particular instructions for opening maven based projects.

## Test

To run the test suite:

1. build the docker images locally as in the build instructions above
1. set your AWS credential environment variables appropriately and similarly to

```bash
  export AWS_ACCESS_KEY=your_access_key_id     # Note that AWS_ACCESS_KEY and
  export AWS_ACCESS_KEY_ID=your_access_key_id  # AWS_ACCESS_KEY_ID have the same value

  export AWS_REGION=us-east-1                  # the region you want the QLDB in,
                                               # NOTE: not all regions are valid
  export AWS_SECRET_ACCESS_KEY=your_secret_access_key
  docker-compose -f docker/daml-test.yaml up
```

By default `daml-on-qldb` will create a ledger named `daml-on-qldb` and an S3 bucket named `value-daml-on-qldb`.  The bucket name is based off of the ledger name.  To use a different ledger name update the daml-test.yaml to add the argument `--ledger my-ledger-name` to the startup of the daml-on-qldb process.


## Run

A local instance of the `daml-on-qldb` may be run up according to the following instructions.

1. build the docker images locally as in the build instructions above
1. set your AWS credential environment variables appropriately and similarly to

```bash
  export AWS_ACCESS_KEY=your_access_key_id     # Note that AWS_ACCESS_KEY and
  export AWS_ACCESS_KEY_ID=your_access_key_id  # AWS_ACCESS_KEY_ID have the same value

  export AWS_REGION=us-east-1                  # the region you want the QLDB in,
                                               # NOTE: not all regions are valid
  export AWS_SECRET_ACCESS_KEY=your_secret_access_key
  docker-compose -f docker/daml-local.yaml up
```
