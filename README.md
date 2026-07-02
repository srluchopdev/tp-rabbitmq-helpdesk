# TP — Comunicación por eventos con broker (RabbitMQ)

Sistema mínimo de mesa de ayuda orientado a eventos.
**Materia:** Integración de Aplicaciones · ITS Cipolletti

## Arquitectura

```
                        ┌──────────────────────── RabbitMQ ────────────────────────┐
                        │                                                          │
POST /tickets           │   exchange topic: helpdesk.events                        │
     │                  │        │                                                 │
     ▼                  │        ├── ticket.created / ticket.created.* ──► [helpdesk.assignment]
[api-service] ── publica ─►      │                                                 │        │
                        │        └── ticket.# ────────────────────────► [helpdesk.audit]    │
                        │                                                          │        ▼
                        │   [helpdesk.errors] ◄── fallo simulado (critical) ── [assignment-worker]
                        │                                                          │  publica ticket.assigned
                        └──────────────────────────────────────────────────────────┘
[audit-worker] consume helpdesk.audit → data/audit-log.jsonl + data/metrics.json
```

| Componente | Responsabilidad | Evento |
|---|---|---|
| `api-service` | Recibe `POST /tickets` y publica evento | `ticket.created.<priority>` |
| `assignment-worker` | Consume tickets creados y simula asignación | `ticket.assigned` |
| `audit-worker` | Consume todos los eventos y registra historial | `ticket.#` |
| RabbitMQ | Broker: exchange, colas, bindings y routing | `helpdesk.events` |

## Requisitos

- Docker + Docker Compose
- Node.js 18 o superior

## Instalación

```bash
# 1. Levantar RabbitMQ (con panel de Management)
docker compose up -d

# 2. Instalar dependencias
npm install
```

RabbitMQ Management queda disponible en <http://localhost:15672> (usuario `guest`, password `guest`).

## Ejecución

Abrir **tres terminales** (una por servicio):

```bash
# Terminal 1 — API HTTP (producer)
npm run api

# Terminal 2 — Worker de asignación
npm run worker:assign

# Terminal 3 — Worker de auditoría
npm run worker:audit
```

## Prueba del flujo

Crear tickets con distintas prioridades:

```bash
curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -d '{"title":"No puedo ingresar","description":"Error 403","priority":"high"}'

curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -d '{"title":"Consulta por factura","description":"Duplicada","priority":"normal"}'

curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -d '{"title":"Cambiar fondo de pantalla","description":"Estética","priority":"low"}'

# Este dispara el FALLO SIMULADO y va a la cola de errores:
curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -d '{"title":"Servidor caído","description":"Producción no responde","priority":"critical"}'
```

En PowerShell (Windows) usar `Invoke-RestMethod`:

```powershell
Invoke-RestMethod -Uri http://localhost:3000/tickets -Method Post -ContentType "application/json" -Body '{"title":"No puedo ingresar","description":"Error 403","priority":"high"}'
```

### Flujo esperado en consola

```
[api]    Ticket TCK-001 creado (priority=high). Evento ticket.created publicado.
[assign] Recibido ticket.created -> ticket=TCK-001 priority=high ...
[assign] Ticket TCK-001 asignado a "Ana Gómez". Evento ticket.assigned publicado.
[audit]  Registrado ticket.created (routingKey=ticket.created.high) ticket=TCK-001
[audit]  Métrica actualizada: 1 ticket(s) creado(s) el 2026-07-01.
[audit]  Registrado ticket.assigned (routingKey=ticket.assigned) ticket=TCK-001
```

## Topología en RabbitMQ

- **Exchange:** `helpdesk.events` (tipo `topic`, durable)
- **Colas y bindings:**
  - `helpdesk.assignment` ← `ticket.created` y `ticket.created.*`
  - `helpdesk.audit` ← `ticket.#`
  - `helpdesk.errors` ← recibe mensajes fallidos directamente (fallo simulado con `priority=critical`)

Se puede verificar en Management → pestañas **Exchanges** y **Queues**.

## Mejoras implementadas (Parte C)

1. **Routing por prioridad:** la API publica con routing key `ticket.created.<priority>` (`high`, `normal`, `low`, `critical`).
2. **Métricas diarias:** el audit-worker mantiene `data/metrics.json` con el conteo de tickets creados por día (`{"2026-07-01": 4}`).
3. **Fallo simulado:** cuando `priority=critical`, el assignment-worker no procesa el ticket y deriva el mensaje (con motivo y timestamp) a la cola `helpdesk.errors`.
4. **Idempotencia:** el assignment-worker guarda los `eventId` procesados en `data/processed-events.json`; si llega un duplicado, lo detecta, lo loguea y lo descarta.

## Archivos generados en ejecución

| Archivo | Contenido |
|---|---|
| `data/audit-log.jsonl` | Historial completo de eventos (una línea JSON por evento) |
| `data/metrics.json` | Tickets creados por día |
| `data/processed-events.json` | eventIds ya procesados (idempotencia) |

## Notas de diseño

- **Ack manual:** cada worker hace `channel.ack(msg)` solo cuando el procesamiento terminó bien. Si el worker muere en el medio, RabbitMQ reentrega el mensaje.
- **`prefetch(1)`** en el assignment-worker: procesa de a un mensaje por vez (fair dispatch).
- **Mensajes persistentes** (`persistent: true`) y colas/exchange **durables**: los mensajes sobreviven a un reinicio del broker.
- **`202 Accepted`** en la API: comunica que la solicitud fue aceptada pero el procesamiento es asincrónico.
