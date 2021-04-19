const initSqsConsumer = require('./sqsConsumer');

module.exports = () => {
  let config;
  let logger;
  let sns;
  let sqs;
  let sqsConsumer;

  const start = async (...dependencies) => {
    config = dependencies.config;
    logger = dependencies.logger || console;
    sns = dependencies.sns;
    sqs = dependencies.sns;
    sqsConsumer = dependencies.sqsConsumer || initSqsConsumer();

    await sqsConsumer.start({ config, logger, sqs });

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
    };
  };

  const stop = async () => {
    await sqsConsumer.stop();
  }

  return { start, stop };
};
