
const express = require('express');
const crypto = require('crypto');
const { connectWithRetry, createChannel, publishEvent } = require('./lib/broker');

const PORT = process.env.PORT || 3000;
const PRIORITIES = ['low', 'normal', 'high', 'critical'];

let ticketCounter = 0;

async function main() {
  const connection = await connectWithRetry();
  const channel = await createChannel(connection);

  const app = express();
  app.use(express.json());

  app.post('/tickets', (req, res) => {
    const { title, description, priority } = req.body || {};

    // Validación mínima del request
    if (!title || !description || !priority) {
      return res.status(400).json({
        error: 'Faltan campos obligatorios: title, description, priority',
      });
    }
    if (!PRIORITIES.includes(priority)) {
      return res.status(400).json({
        error: `priority inválida. Valores permitidos: ${PRIORITIES.join(', ')}`,
      });
    }

    // Generamos el ticket (en un sistema real iría a una base de datos)
    ticketCounter += 1;
    const ticketId = `TCK-${String(ticketCounter).padStart(3, '0')}`;

    // Contrato de evento sugerido por el TP
    const event = {
      eventId: crypto.randomUUID(),
      type: 'ticket.created',
      occurredAt: new Date().toISOString(),
      version: 1,
      payload: { ticketId, title, description, priority },
    };

    // Parte C: routing por prioridad -> ticket.created.high, .normal, .low, .critical
    const routingKey = `ticket.created.${priority}`;
    publishEvent(channel, routingKey, event);

    console.log(`[api] Ticket ${ticketId} creado (priority=${priority}). Evento ticket.created publicado.`);

    // 202 Accepted: aceptamos la solicitud; el procesamiento sigue de forma asincrónica
    res.status(202).json({
      message: 'Ticket recibido. Será procesado de forma asincrónica.',
      ticketId,
      eventId: event.eventId,
    });
  });

  app.listen(PORT, () => {
    console.log(`[api] api-service escuchando en http://localhost:${PORT}`);
    console.log(`[api] Probar con: curl -X POST http://localhost:${PORT}/tickets -H "Content-Type: application/json" -d '{"title":"No puedo ingresar","description":"Error 403","priority":"high"}'`);
  });

  // Cierre prolijo
  process.on('SIGINT', async () => {
    console.log('\n[api] Cerrando conexión con RabbitMQ...');
    await channel.close();
    await connection.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('[api] Error fatal:', err.message);
  process.exit(1);
});
