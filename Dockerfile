FROM golang:alpine as go
WORKDIR /telekom
COPY go.mod go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLEd=0 go build .

FROM alpine
COPY --from=go /telekom/telekom_exporter /usr/local/bin/telekom_exporter
ENTRYPOINT ["/usr/local/bin/telekom_exporter"]
