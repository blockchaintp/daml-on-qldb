# daml-on-qldb

## Build

### Using the docker based toolchain

The basic build uses docker to pull down its toolchain and compile.

1. install Docker <https://docs.docker.com/install/>
1. install docker-compose <https://docs.docker.com/compose/install/>

1. Clone the repository.

```$ git clone git@github.com:blockchaintp/daml-on-sawtooth.git```

1. set export the build identifier environment variable. This is used to
   distinguish different variations of builds on the same machine.
   ISOLATION_ID defaults to `local`

```$ export ISOLATION_ID=my-local-build```

1. Build the package. This will compile,test and package all of the java, as
   well as prepare docker images for local execution.  In order for this to
   complete access to AWS credentials are required via environment variables.

```bash
export AWS_ACCESS_KEY=your_access_key_id     # Note that AWS_ACCESS_KEY and
export AWS_ACCESS_KEY_ID=your_access_key_id  # AWS_ACCESS_KEY_ID have the same value
make package
```

### Resetting permissions

If permissions/ownership of any files are incorrectly set after a build is
aborted, they may be corrected via

```bash
make fix_permissions
```

### Building in an IDE

The docker based toolchain is our standard way of building, but fundamentally
this project is a standard maven build with no unusual elements. So the jar
artifacts may be built simply by

```mvn clean package```

See your IDE's particular instructions for opening maven based projects.

## Test

To run the test suite, either use maven directly or if executing via `make` AWS
credentials are required.

```bash

export AWS_ACCESS_KEY=your_access_key_id     # Note that AWS_ACCESS_KEY and
export AWS_ACCESS_KEY_ID=your_access_key_id  # AWS_ACCESS_KEY_ID have the same value
make test

```

Enjoy!
