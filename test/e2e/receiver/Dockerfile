# The smoke test builds the static binary on the host and copies it in, so this image needs no
# builder stage and pulls nothing.
FROM scratch
COPY receiver /receiver
USER 1000:1000
EXPOSE 8080
ENTRYPOINT ["/receiver"]
