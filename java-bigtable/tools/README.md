# Command line tools for interacting with Jetstream protocol

## Usage

* To build, invoke `mvn install`.
* To list available commands `./tools/target/app/run.sh`.
* To get available options for a command `./tools/target/app/run.sh <command> --help`.

## Common Options

* `--table-name` the full table name to send RPCs to. Currently this
  defaults to a table in the test environment.
* `--app-profile-id` the app profile id to use. Defaults to `default`
* `--mode` configures the underlying transport. Can be one of
  * `CloudPath` - Uses traditional CFE path
  * `CloudPathTd` - Use CFE, but uses TrafficDirector to route to the CFE
  * `DirectPath` - Uses TrafficDirector + DirectPath
  * `RawDirectPath` - Uses DirectPath using raw ip as obtain from 
    [Borg Task Manager Summary](https://playbooks.corp.google.com/cloud-bigtable/procedures/DirectPathEndpointSetup.md?cl=head#:~:text=The%20Borg%20Task%20Manager%20Summary)
  * `--endpoint` - The endpoint of the target.
    * For CloudPath, this should include both the hostname and port. ie.
    `test-bigtable.sandbox.googleapis.com:443`.
    * For `CloudPathTd`, it should be just be the c2p target without a port. ie
    `test-bigtable.sandbox.googleapis.com`.
    * For `RawDirectPath`, it should be IP:port from the Borg summary page.
    * For `DirectPath`, it should be just be the c2p target without a port ie.
    `test-bigtable.sandbox.googleapis.com`.

## Commands

* `generate-load` - generates steady traffic using Jetstream. This will use
  the same row key for all requests. This is meant to test the transport.
  The load shape can be configured via the cli options
* `generate-unary-load` - similar to `generate-load`, but uses the classic
  unary protocol. Mainly used for comparison and antagonistic traffic.
* `readrow` - reads a single row using Jetstream.
* `mutaterow` - mutates a single row using Jetstream.
* `ycsb` - wraps the ycsb benchmark. It exposes the ycsb configuration using
  normal java cli options. Latency histograms will be stored by default in `results/`.
* `ycsb-summarize` - Summarizes a histogram timeseries file. 

## GKE

* A docker image of the tools lives at gcr.io/google.com/cloud-bigtable-dev/jetstream-tools
* Prerequisite: install kubectl: https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_kubectl
* Setup
  ```shell
    # Download credentials
    gcloud auth configure-docker  us-docker.pkg.dev
    gcloud container clusters get-credentials jetstream-gke \
      --region europe-west1 --project google.com:cloud-bigtable-dev
    # Install jq
    brew install jq
  ```
* A deployment in gke was created with:
  ```shell
  mvn clean install -P docker-publish \
      && kubectl apply -f tools/gke/ycsb.yaml
  ```
* To update the image:
  ```shell
    # Build, upload docker image and update the deployment
    mvn clean install -P docker-publish \
      && kubectl set image deployment/jetstream-tools-ycsb jetstream-tools-ycsb=$(cat tools/target/jib-image.json | jq -r '.image')
  ``` 
* To change parameters:
  ```shell
  kubectl edit deployment/jetstream-tools-ycsb
  # or to reset to the default config
  kubectl apply -f tools/gke/ycsb.yaml 
  ```

* To summarise YCSB results
  ```shell
  kubectl exec $POD_NAME -- /bin/sh -c "java -cp /app/resources:/app/classes:/app/libs/* com.google.cloud.bigtable.jetstream.tools.Main ycsb-summarize"
  ```

## Maintenance

### ycsb

Unfortunately ycsb doesn't publish artifacts to maven central, so the jars
are vendored as a local maven repository under tools/vendored-libs. To
update the jars:

```sh
git clone https://github.com/brianfrankcooper/YCSB.git /tmp/YCSB
git -C /tmp/YCSB reset --hard 1e62880552fc95fa4b012846c0f3887420e676a8
YCSB_VERSION="0.18.0-SNAPSHOT"

# Build YCSB parts we care about
mvn -f /tmp/YCSB/pom.xml \
  clean package -DskipTests -am -pl core,googlebigtable2

# copy the vendored jars
cp /tmp/YCSB/core/target/core-${YCSB_VERSION}.jar \
  /tmp/YCSB/googlebigtable2/target/googlebigtable2-binding-${YCSB_VERSION}.jar \

  $PWD/../tools/vendored-libs
```



Creating a new GKE cluster
```shell
gcloud container --project "google.com:cloud-bigtable-dev" clusters create "jetstream-gke" \
  --region "europe-west1" --tier "standard" \
  --machine-type "e2-standard-32" --image-type "COS_CONTAINERD" \
  --scopes "https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/bigtable.data","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" \
  --num-nodes "1" \
  --network "projects/google.com:cloud-bigtable-dev/global/networks/igor-default" \
  --subnetwork "projects/google.com:cloud-bigtable-dev/regions/europe-west1/subnetworks/igor-default" \
  --enable-autoscaling --min-nodes "0" --max-nodes "5" --location-policy "ANY" 
```
