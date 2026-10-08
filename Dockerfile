# Assembled from a published release: `container-image.yml` stages the verified Linux archives as
# `dist/<arch>/cassette`, so nothing is compiled here and the image carries the exact bytes users
# download. Not buildable from the repository root.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:54df941ed0d06a1bd95ef5e0ce391fd8d9f94b64782dc9a60062727849ee3f97

ARG TARGETARCH

COPY --chmod=0755 dist/${TARGETARCH}/cassette /usr/local/bin/
COPY LICENSE-APACHE LICENSE-MIT /usr/share/doc/cassette/

USER 65532:65532
WORKDIR /tmp
ENV CASSETTE_LISTEN=0.0.0.0:8787
EXPOSE 8787

ENTRYPOINT ["cassette"]
CMD ["--help"]

LABEL org.opencontainers.image.title="cassette" \
      org.opencontainers.image.description="Record/replay HTTP server for Dekopon provider baseUrl" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      org.opencontainers.image.source="https://github.com/dekopon-agents/cassette"
