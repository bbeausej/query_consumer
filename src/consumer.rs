use log::info;
use rdkafka::{
    consumer::{ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    ClientContext, TopicPartitionList,
};

pub struct CustomConsumerContext;
impl ClientContext for CustomConsumerContext {}
impl ConsumerContext for CustomConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

pub type LoggingConsumer = StreamConsumer<CustomConsumerContext>;
