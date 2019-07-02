FROM       alpine:3.9
RUN        apk add --update --no-cache ca-certificates
COPY       bigtable-backup /bin/bigtable-backup
EXPOSE     80
ENTRYPOINT [ "/bin/bigtable-backup" ]
