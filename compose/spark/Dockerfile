FROM apache/spark:3.5.9-java17-python3@sha256:2e5ef20d330184760e3a52639c03bcf1173dbbb1acfbb293451b02eb1a9799d1

USER root

ARG ICEBERG_VERSION=1.12.0
ARG ICEBERG_SPARK_RUNTIME_SHA256=8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc
ARG ICEBERG_AWS_BUNDLE_SHA256=dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957

RUN python3 -c "\
import hashlib, pathlib, urllib.request; \
jars = [ \
  ('https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar', '/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar', '${ICEBERG_SPARK_RUNTIME_SHA256}'), \
  ('https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar', '/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar', '${ICEBERG_AWS_BUNDLE_SHA256}'), \
]; \
[ \
  (urllib.request.urlretrieve(u, p), \
   h := hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest(), \
   (_ for _ in ()).throw(SystemExit(f'SHA256 mismatch for {p}: got {h}, expected {e}')) if h != e else print(f'OK {p} sha256={h}')) \
  for u, p, e in jars \
]"

ENV TZ=UTC

USER spark
