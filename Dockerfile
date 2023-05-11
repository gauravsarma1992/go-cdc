FROM golang:1.19

WORKDIR /app

# Copy the source code from the host machine to the container
COPY . .

WORKDIR /app/src/run

# Build the executable
RUN go build -o /app/oplogreplay

WORKDIR /app/src/oplog

# Set the command to run the executable
CMD ["/app/oplogreplay"]
