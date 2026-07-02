
const fs = require('fs');
const path = require('path');
const {
  connectWithRetry,
  createChannel,
  EXCHANGE,
  QUEUE_AUDIT,
} = require('./lib/broker');

const DATA_DIR = path.join(__dirname, '..', 'data');
const AUDIT_FILE = path.join(DATA_DIR, 'audit-log.jsonl');
const METRICS_FILE = path.join(DATA_DIR, 'metrics.json');

/** Agrega una línea al log de auditoría (formato JSON Lines). */
function appendAudit(entry) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.appendFileSync(AUDIT_FILE, JSON.stringify(entry) + '\n');
}

/** Incrementa el contador de tickets creados para la fecha de hoy. */
function incrementDailyMetric() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  let metrics = {};
  try {
    metrics = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8'));
  } catch {
    metrics = {};
  }
  const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  metrics[today] = (metrics[today] || 0) + 1;
  fs.writeFileSync(METRICS_FILE, JSON.stringify(metrics, null, 2));
  return { today, count: metrics[today] };
}

async function main() {
  const connection = await connectWithRetry();
  const channel = await createChannel(connection);

  // Cola de auditoría: recibe TODO lo que empiece con "ticket."
  await channel.assertQueue(QUEUE_AUDIT, { durable: true });
  await channel.bindQueue(QUEUE_AUDIT, EXCHANGE, 'ticket.#');

  console.log(`[audit] Auditando todos los eventos "ticket.#" en cola "${QUEUE_AUDIT}"...`);

  channel.consume(QUEUE_AUDIT, (msg) => {
    if (!msg) return;

    let event;
    try {
      event = JSON.parse(msg.content.toString());
    } catch {
      console.error('[audit] Mensaje con JSON inválido. Se descarta (ack).');
      channel.ack(msg);
      return;
    }

    const routingKey = msg.fields.routingKey;

    // Registro de auditoría con metadatos del broker
    appendAudit({
      auditedAt: new Date().toISOString(),
      routingKey,
      event,
    });

    console.log(`[audit] Registrado ${event.type} (routingKey=${routingKey}) ticket=${event.payload?.ticketId}`);

    // Parte C: métrica diaria de tickets creados
    if (event.type === 'ticket.created') {
      const { today, count } = incrementDailyMetric();
      console.log(`[audit] Métrica actualizada: ${count} ticket(s) creado(s) el ${today}.`);
    }

    // Ack: el evento quedó auditado correctamente
    channel.ack(msg);
  });

  process.on('SIGINT', async () => {
    console.log('\n[audit] Cerrando conexión con RabbitMQ...');
    await channel.close();
    await connection.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('[audit] Error fatal:', err.message);
  process.exit(1);
});
