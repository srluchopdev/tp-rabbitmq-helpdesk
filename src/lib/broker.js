
const amqp = require('amqplib');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://guest:guest@localhost:5672';

const EXCHANGE = 'helpdesk.events';
const QUEUE_ASSIGNMENT = 'helpdesk.assignment';
const QUEUE_AUDIT = 'helpdesk.audit';
const QUEUE_ERRORS = 'helpdesk.errors';

/**
 * Conecta a RabbitMQ con reintentos (útil si el contenedor todavía está arrancando).
 */
async function connectWithRetry(retries = 10, delayMs = 3000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const connection = await amqp.connect(RABBIT_URL);
      console.log(`[broker] Conectado a RabbitMQ (${RABBIT_URL})`);
      return connection;
    } catch (err) {
      console.log(`[broker] Intento ${attempt}/${retries} falló: ${err.message}. Reintentando en ${delayMs / 1000}s...`);
      await new Promise((res) => setTimeout(res, delayMs));
    }
  }
  throw new Error('No se pudo conectar a RabbitMQ. ¿Está corriendo docker compose?');
}

/**
 * Crea un canal y declara el exchange topic del dominio.
 * Los assert* son idempotentes: si ya existen, no pasa nada.
 */
async function createChannel(connection) {
  const channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  return channel;
}

/**
 * Publica un evento de dominio en el exchange.
 * routingKey ejemplo: "ticket.created.high"
 */
function publishEvent(channel, routingKey, event) {
  const ok = channel.publish(
    EXCHANGE,
    routingKey,
    Buffer.from(JSON.stringify(event)),
    { persistent: true, contentType: 'application/json' }
  );
  console.log(`[broker] Evento publicado -> routingKey="${routingKey}" eventId=${event.eventId}`);
  return ok;
}

module.exports = {
  RABBIT_URL,
  EXCHANGE,
  QUEUE_ASSIGNMENT,
  QUEUE_AUDIT,
  QUEUE_ERRORS,
  connectWithRetry,
  createChannel,
  publishEvent,
};
