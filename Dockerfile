FROM debian:12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fonts-lato fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -u 1000 -m audit

WORKDIR /app

COPY dist/ng-audit-checker ./
RUN chown audit:audit /app/ng-audit-checker

USER audit

CMD ["./ng-audit-checker"]
