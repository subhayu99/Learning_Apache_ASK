# Required connection configs for Kafka producer, consumer, and admin
bootstrap_servers = [
    # "pkc-2396y.us-east-1.aws.confluent.cloud:9092",
    "localhost:9092",
]
security_protocol = "SASL_SSL"
sasl_mechanisms = "PLAIN"
sasl_username = "ASFZHZWJGJL5IYR7"
sasl_password = "yFUlaYI9DJO8QcA5MLzvZHqWpoSk9C6HQZWwrcLTgUnNbD1+JEVtM1REaWlWRaji"

# Best practice for higher availability in librdkafka clients prior to 1.7
session_timeout_ms = 45000

# topic name
topic = "location"