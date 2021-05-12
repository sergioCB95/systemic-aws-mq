const sqsConsumerComponent = require('./sqsConsumer')();

module.exports = () => {
  let config;
  let logger;
  let sns;
  let sqs;
  let sqsConsumer;

  const _setUpConfig = (inputConfig) => ({
    noStart: inputConfig.noStart || false,
    queueParams: {
      attributeNames: inputConfig.queueParams && inputConfig.queueParams.attributeNames || ['SentTimestamp'],
      batchSize: inputConfig.queueParams && inputConfig.queueParams.batchSize || 10,
      messageAttributeNames: inputConfig.queueParams && inputConfig.queueParams.messageAttributeNames || ['All'],
      visibilityTimeout: inputConfig.queueParams && inputConfig.queueParams.visibilityTimeout || 20,
      waitTimeSeconds: inputConfig.queueParams && inputConfig.queueParams.waitTimeSeconds || 20,
      ...inputConfig.queueParams,
    }
  });

  const start = async (dependencies) => {

    config = _setUpConfig(dependencies.config || {});
    logger = dependencies.logger || console;
    sns = dependencies.sns;
    sqs = dependencies.sqs;
    sqsConsumer = await sqsConsumerComponent.start({ config, logger, sqs });

    const publish = ({ topic, message }) => sns.publish({ topic, message });

    const subscribe = (queueName, handleMessage) => {
      const handleMessageWrapper = async (message) => {
        logger.info(
          `${queueName} MQ (messageId: ${message.MessageId})\n ${JSON.stringify(
            message.Body,
            null,
            2
          )}`
        );
        try {
          return handleMessage(JSON.parse(JSON.parse(message.Body).Message))
            .then((res) => {
              logger.info(
                `Successful response (messageId: ${message.MessageId}) \n ${res}`
              );
            })
            .catch((err) => {
              logger.error(
                `Error processing data (messageId: ${message.MessageId}) \n ${err}`
              );
              throw err;
            });
        } catch (err) {
          logger.error(
            `Error parsing data. JSON.parse fails with error \n ${err}`
          );
          throw err;
        }
      };
      return sqsConsumer.subscribe(queueName, handleMessageWrapper);
    };

    return {
      publish,
      subscribe,
      listeners: sqsConsumer.listeners,
    };
  };

  const stop = async () => {
    await sqsConsumerComponent.stop();
  }

  return { start, stop };
};
