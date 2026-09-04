#!/bin/bash
set -e

# We use stubs to trick protoc into compiling without internal dependencies.
# This avoids modifying the source files and risking syntax errors.

cd $(dirname "$0")

# Backup original pom.xml
cp pom.xml pom.xml.bak

# Create a temporary stubs directory
mkdir -p proto-stubs/google/api proto-stubs/storage/datapol/annotations/proto

cat << 'EOF' > proto-stubs/google/api/auditing.proto
syntax = "proto2";
package google.api;
import "google/protobuf/descriptor.proto";
message FieldAuditing {
  optional string directive = 1;
}
message ResourceContainer {
  optional string type = 1;
}
extend google.protobuf.FieldOptions {
  optional FieldAuditing field_auditing = 55555;
  // Stub for missing resource_container
  optional ResourceContainer resource_container = 55559;
}
EOF

cat << 'EOF' > proto-stubs/google/api/authz.proto
syntax = "proto2";
package google.api;
import "google/protobuf/descriptor.proto";
message Authz {
  optional string permissions = 1;
}
extend google.protobuf.FieldOptions {
  optional Authz authz = 55556;
}
EOF

cat << 'EOF' > proto-stubs/storage/datapol/annotations/proto/semantic_annotations.proto
syntax = "proto2";
package datapol;
import "google/protobuf/descriptor.proto";
enum SemanticType {
  ST_UNSPECIFIED = 0;
  ST_IDENTIFYING_ID = 1;
  ST_NOT_REQUIRED = 2;
  ST_SESSION_ID = 3;
  ST_LOCATION = 4;
  ST_USER_CONTENT = 5;
}
message Qualifier {
  optional bool is_access_target = 1;
}
extend google.protobuf.FieldOptions {
  optional Qualifier qualifier = 55557;
  optional SemanticType semantic_type = 55558;
}
extend google.protobuf.FileOptions {
  optional string file_vetting_status = 55560;
}
EOF

# Inject protobuf-maven-plugin into a temporary POM to handle extraction of common protos
cat << 'EOF' > pom.xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.google.api.grpc</groupId>
  <artifactId>proto-google-cloud-bigtable-v2</artifactId>
  <version>2.82.0-SNAPSHOT</version>
  <parent>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-bigtable-parent</artifactId>
    <version>2.82.0-SNAPSHOT</version>
  </parent>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-cloud-bigtable-deps-bom</artifactId>
        <version>2.82.0-SNAPSHOT</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-cloud-bigtable-bom</artifactId>
        <version>2.82.0-SNAPSHOT</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.google.api</groupId>
      <artifactId>api-common</artifactId>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>com.google.api.grpc</groupId>
      <artifactId>proto-google-common-protos</artifactId>
      <scope>compile</scope>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.xolstice.maven.plugins</groupId>
        <artifactId>protobuf-maven-plugin</artifactId>
        <version>0.6.1</version>
        <configuration>
          <protocArtifact>com.google.protobuf:protoc:4.33.6:exe:linux-x86_64</protocArtifact>
          <outputDirectory>${project.basedir}/src/main/java</outputDirectory>
          <clearOutputDirectory>false</clearOutputDirectory>
          <additionalProtoPathElements>
            <additionalProtoPathElement>${project.basedir}/proto-stubs</additionalProtoPathElement>
          </additionalProtoPathElements>
        </configuration>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
EOF

# Strip internal go_api_flag which protoc cannot parse
find src/main/proto -name "*.proto" -exec sed -i -E '/option go_api_flag/d' {} +
find src/main/proto -name "*.proto" -exec sed -i -E 's/extend proto2\.M/extend google.protobuf.M/g' {} +
find src/main/proto -name "*.proto" -exec sed -i -E 's/import "net\/proto2\/proto\/descriptor.proto";/import "google\/protobuf\/descriptor.proto";/g' {} +

echo "Compiling protos using protobuf-maven-plugin..."
mvn generate-sources -Dcheckstyle.skip=true || { echo "Compilation failed"; mv pom.xml.bak pom.xml; git restore src/main/proto; exit 1; }

echo "Cleaning up..."
mv pom.xml.bak pom.xml
git restore src/main/proto
rm -rf proto-stubs
echo "Done! Generated Java classes are in src/main/java"
