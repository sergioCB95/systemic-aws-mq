const { Consumer } = require('sqs-consumer');

module.exports = () => {
  let logger;
  const listeners = {};

  const start = async ({
    config,
    sqs,
    logger: log,
  }) => {
    logger = log;

    const _buildQueueParams = (queueName, handleMessage) => ({
      queueUrl: queueName,
      handleMessage,
      sqs,
      ...config.queueParams,
    });

    const subscribe = (queueName, handleMessage) => {
      const listener = Consumer.create(
        _buildQueueParams(queueName, handleMessage)
      );
      listeners[queueName] = listener;

      listener.on('error', (err) => {
        logger.error(err.message);
      });

      listener.on('processing_error', (err) => {
        logger.error(err.message);
      });

      listener.on('timeout_error', (err) => {
        logger.error(err.message);
      });

      listener.on('stopped', () => {
        listeners[queueName].stopped = true;
      });

      if (!config.noStart) listener.start();
      return listener;
    };

    return {
      listeners,
      subscribe,
    };
  };

  const stop = async () => {
    logger.info('Disconnecting from AWS SQS queues...');
    Object.values(listeners).forEach((listener) => listener.stop());
  };

  return { start, stop };
};
