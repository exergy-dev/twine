//! librdkafka ThreadedProducer wrapper.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientContext;

use crate::Stats;

pub struct Ctx {
    st: Arc<Stats>,
}

impl ClientContext for Ctx {}

impl ProducerContext for Ctx {
    type DeliveryOpaque = ();
    fn delivery(&self, r: &DeliveryResult<'_>, _: ()) {
        match r {
            Ok(_) => {
                self.st.kafka_ok.fetch_add(1, Relaxed);
            }
            Err((e, _)) => {
                self.st.kafka_err.fetch_add(1, Relaxed);
                self.st
                    .first_err
                    .call_once(|| log::info!("kafka: first produce error: {e}"));
            }
        }
    }
}

pub struct Kafka {
    producer: ThreadedProducer<Ctx>,
    topic: String,
    drop_on_full: bool,
    st: Arc<Stats>,
}

impl Kafka {
    pub fn new(
        brokers: &str,
        topic: &str,
        drop_on_full: bool,
        st: Arc<Stats>,
    ) -> KafkaResult<Self> {
        let producer: ThreadedProducer<Ctx> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "1")
            .set("enable.idempotence", "false")
            .set("linger.ms", "5")
            .set("compression.codec", "snappy")
            .set("queue.buffering.max.messages", "1048576")
            .set("queue.buffering.max.kbytes", "2097151")
            .create_with_context(Ctx { st: st.clone() })?;
        Ok(Kafka {
            producer,
            topic: topic.to_string(),
            drop_on_full,
            st,
        })
    }

    /// Queues one record. Blocking mode (the equivalent of franz-go `Produce`)
    /// retries on a full local queue so backpressure reaches the senders;
    /// drop mode (`TryProduce`) counts kafka_err and drops.
    pub fn produce(&self, pkt: &[u8]) {
        loop {
            match self
                .producer
                .send(BaseRecord::<(), [u8]>::to(&self.topic).payload(pkt))
            {
                Ok(()) => return,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _))
                    if !self.drop_on_full =>
                {
                    std::thread::sleep(Duration::from_micros(100));
                }
                Err((e, _)) => {
                    self.st.kafka_err.fetch_add(1, Relaxed);
                    self.st
                        .first_err
                        .call_once(|| log::info!("kafka: first produce error: {e}"));
                    return;
                }
            }
        }
    }

    pub fn flush(&self, d: Duration) {
        if let Err(e) = self.producer.flush(d) {
            log::info!("kafka flush: {e}");
        }
    }
}
