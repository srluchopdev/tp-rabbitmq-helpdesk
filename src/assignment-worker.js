
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const {
  connectWithRetry,
  createChannel,
  publishEvent,
  EXCHANGE,
  QUEUE_ASSIGNMENT,
  QUEUE_ERRORS,
} = require('./lib/broker');

const PROCESSED_FILE = path.join(__dirname, '..', 'data', 'processed-events.json');
const AGENTS = ['Ana Gómez', 'Bruno Díaz', 'Carla Pérez', 'Diego Sosa'];

/** Carga el set de eventIds ya procesados desde disco. */
function loadProcessed() {
  try {
    return new Set(JSON.parse(fs.readFileSync(PROCESSED_FILE, 'utf8')));
  } catch {
    return new Set();
  }
}

/** Persiste el set de eventIds procesados. */
function saveProcessed(processedSet) {
  fs.mkdirSync(path.dirname(PROCESSED_FILE), { recursive: true });
  fs.writeFileSync(PROCESSED_FILE, JSON.stringify([...processedSet], null, 2));
}

async function main() {
  const connection = await connectWithRetry();
  const channel = await createChannel(connection);

  // Cola de trabajo del worker de asignación
  await channel.assertQueue(QUEUE_ASSIGNMENT, { durable: true });
  // Binding "clásico" pedido por el TP + binding con prioridad (Parte C)
  await channel.bindQueue(QUEUE_ASSIGNMENT, EXCHANGE, 'ticket.created');
  await channel.bindQueue(QUEUE_ASSIGNMENT, EXCHANGE, 'ticket.created.*');

  // Cola de errores para mensajes que fallan (prioridad critical)
  await channel.assertQueue(QUEUE_ERRORS, { durable: true });

  // Procesar de a un mensaje por vez (fair dispatch)
  channel.prefetch(1);

  const processed = loadProcessed();

  console.log(`[assign] Esperando eventos ticket.created en cola "${QUEUE_ASSIGNMENT}"...`);

  channel.consume(QUEUE_ASSIGNMENT, (msg) => {
    if (!msg) return;

    let event;
    try {
      event = JSON.parse(msg.content.toString());
    } catch {
      console.error('[assign] Mensaje con JSON inválido. Se descarta (ack).');
      channel.ack(msg);
      return;
    }

    const { eventId, payload } = event;
    const { ticketId, priority } = payload;

    console.log(`[assign] Recibido ${event.type} -> ticket=${ticketId} priority=${priority} eventId=${eventId}`);

    // --- Idempotencia (Parte C): si ya lo procesamos, lo descartamos ---
    if (processed.has(eventId)) {
      console.log(`[assign] DUPLICADO detectado (eventId=${eventId}). Se ignora y se hace ack.`);
      channel.ack(msg);
      return;
    }

    // --- Simulación de fallo (Parte C): prioridad critical falla ---
    if (priority === 'critical') {
      console.error(`[assign] FALLO SIMULADO procesando ${ticketId} (priority=critical).`);
      console.error(`[assign] Derivando mensaje a la cola de errores "${QUEUE_ERRORS}".`);

      // Publicamos el mensaje original en la cola de errores con contexto del fallo
      channel.sendToQueue(
        QUEUE_ERRORS,
        Buffer.from(JSON.stringify({
          failedAt: new Date().toISOString(),
          reason: 'Fallo simulado: prioridad critical',
          originalEvent: event,
        })),
        { persistent: true, contentType: 'application/json' }
      );

      // Ack del mensaje original: ya quedó registrado en la cola de errores,
      // no queremos que RabbitMQ lo reintente infinitamente.
      channel.ack(msg);
      return;
    }

    // --- Procesamiento normal: simular asignación ---
    const assignee = AGENTS[Math.floor(Math.random() * AGENTS.length)];

    const assignedEvent = {
      eventId: crypto.randomUUID(),
      type: 'ticket.assigned',
      occurredAt: new Date().toISOString(),
      version: 1,
      payload: { ticketId, assignee, priority },
    };

    publishEvent(channel, 'ticket.assigned', assignedEvent);
    console.log(`[assign] Ticket ${ticketId} asignado a "${assignee}". Evento ticket.assigned publicado.`);

    // Marcamos el evento como procesado y persistimos (idempotencia)
    processed.add(eventId);
    saveProcessed(processed);

    // Ack: el procesamiento terminó bien, RabbitMQ puede eliminar el mensaje
    channel.ack(msg);
  });

  process.on('SIGINT', async () => {
    console.log('\n[assign] Cerrando conexión con RabbitMQ...');
    await channel.close();
    await connection.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('[assign] Error fatal:', err.message);
  process.exit(1);
});
