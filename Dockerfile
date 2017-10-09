FROM google/cloud-sdk

RUN apt-get update
RUN apt-get install -y git
RUN wget -O /tmp/go.tar.gz https://storage.googleapis.com/golang/go1.8.4.linux-amd64.tar.gz
RUN mkdir -p /usr/local
RUN tar -C /usr/local -xf /tmp/go.tar.gz
RUN rm -f /tmp/go.tar.gz

ENV GOPATH /go
ENV PATH $PATH:/usr/local/go/bin

CMD ["/bin/bash"]
