FROM ubuntu:latest
RUN apt-get update && \
    apt-get install -y \
    wget unzip bash
COPY count.sh /count.sh
RUN chmod +x /count.sh
CMD ["/count.sh"]

