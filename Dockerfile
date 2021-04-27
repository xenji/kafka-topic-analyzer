FROM fedora:33
RUN dnf upgrade -y && dnf clean all
COPY target/release/kafka-topic-analyzer /usr/bin/
ENTRYPOINT "/usr/bin/bash"