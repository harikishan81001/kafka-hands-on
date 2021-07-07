#
# Copyright 2017 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds a docker image for Confluent's example applications for the Kafka Streams API
#ARG DOCKER_UPSTREAM_REGISTRY=registry.hub.docker.com
#ARG DOCKER_UPSTREAM_TAG=latest

FROM confluentinc/cp-base

ARG STREAMS_VERSION=3.3.2
ARG ARTIFACT_ID=main
ARG STREAMS_BOOTSTRAP_SERVERS=127.0.0.1

MAINTAINER partner-support@confluent.io
LABEL io.confluent.docker=true
ARG COMMIT_ID=unknown
LABEL io.confluent.docker.git.id=$COMMIT_ID
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER

WORKDIR /build
ENV COMPONENT="${ARTIFACT_ID}"

# We run the Kafka Streams demo application as a non-priviledged user.
ENV STREAMS_USER="streams"
ENV STREAMS_GROUP=$STREAMS_USER

ENV STREAMS_EXAMPLES_BRANCH="${CONFLUENT_MAJOR_VERSION}.${CONFLUENT_MINOR_VERSION}.x"
ENV STREAMS_EXAMPLES_FATJAR="main-${STREAMS_VERSION}-standalone.jar"
ENV STREAMS_APP_DIRECTORY="/usr/share/java/kafka-streams"
ENV STREAMS_EXAMPLES_FATJAR_DEPLOYED="$STREAMS_APP_DIRECTORY/$STREAMS_EXAMPLES_FATJAR"
ENV KAFKA_MUSIC_APP_CLASS="com.delhivery.stream.KafkaKStreamsMain"
ENV KAFKA_MUSIC_APP_REST_HOST=localhost
ENV KAFKA_MUSIC_APP_REST_PORT=7070

#EXPOSE $KAFKA_MUSIC_APP_REST_PORT

# This affects how strings in Java class files are interpreted.  We want UTF-8, and this is the only locale in the
# base image that supports it
ENV LANG="C.UTF-8"

RUN groupadd $STREAMS_GROUP && useradd -r -g $STREAMS_GROUP $STREAMS_USER

# logging directory
RUN mkdir -p /var/log/kafka-streams && \
    chown $STREAMS_USER:$STREAMS_GROUP /var/log/kafka-streams

RUN wget https://archive.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz && \
    tar -zxf apache-maven-3.2.5-bin.tar.gz && \
    cp -R apache-maven-3.2.5 /usr/local && \
    ln -s /usr/local/apache-maven-3.2.5/bin/mvn /usr/bin/mvn && \
    mvn -version

RUN mkdir -p ~/.m2/repository
COPY pom.xml ./
COPY ./ ./

RUN mvn -T 2C clean install --fail-never
RUN mvn -T 2C clean dependency:go-offline --fail-never
RUN -T 2C mvn clean package -Dcheckstyle.skip=true
