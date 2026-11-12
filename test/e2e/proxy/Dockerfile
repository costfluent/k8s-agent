# The e2e tests build the static binary on the host and copy it in, so this image needs no builder
# stage and pulls nothing.
FROM scratch
COPY proxy /proxy
USER 1000:1000
EXPOSE 3128
ENTRYPOINT ["/proxy"]
