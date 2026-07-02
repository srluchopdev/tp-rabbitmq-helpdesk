# Parte A — Respuestas conceptuales

**Materia:** Integración de Aplicaciones · ITS Cipolletti
**TP:** Comunicación por eventos con broker (RabbitMQ)

---

## 1. ¿Qué diferencia hay entre una llamada REST y un evento publicado en un broker?

Una llamada REST es **sincrónica y dirigida**: el cliente conoce al servidor, le pide algo puntual (por ejemplo `POST /tickets`) y se queda esperando la respuesta. Si el servidor está caído o tarda, el cliente falla o se bloquea. Además, quien llama decide *qué* tiene que hacer el otro: es una comunicación con acoplamiento fuerte, porque el emisor necesita saber la URL, el contrato y la disponibilidad del receptor.

Un evento publicado en un broker es **asincrónico y desacoplado**: el productor solo informa que algo ocurrió ("se creó un ticket") y lo deposita en el broker. No sabe ni le importa quién lo va a consumir, cuántos consumidores hay, ni si están activos en ese momento. El broker guarda el mensaje en colas y los consumidores lo procesan cuando pueden. Esto permite que productor y consumidor evolucionen, escalen y fallen de forma independiente: si el worker de asignación está caído, los tickets no se pierden, quedan encolados hasta que vuelva.

En resumen: REST es "te pido que hagas esto y espero tu respuesta"; un evento es "aviso que esto pasó, que reaccione quien tenga que reaccionar".

## 2. Definiciones

- **Producer (productor):** el proceso que genera y publica mensajes/eventos hacia el broker. En este TP es el `api-service`, que publica `ticket.created` cuando recibe un POST. El assignment-worker también actúa como producer cuando publica `ticket.assigned`.

- **Broker:** el intermediario de mensajería (RabbitMQ) que recibe los mensajes de los productores, los enruta y los almacena en colas hasta que un consumidor los procese. Desacopla a las partes: nadie habla directamente con nadie, todos hablan con el broker.

- **Exchange:** el componente del broker que **recibe** los mensajes publicados y decide a qué cola(s) enviarlos según su tipo y las reglas de binding. En este TP usamos `helpdesk.events`, de tipo **topic**, que rutea comparando la routing key del mensaje contra patrones (con comodines `*` y `#`).

- **Queue (cola):** el buffer donde el broker almacena los mensajes hasta que un consumidor los tome. Cada cola tiene su propia copia del mensaje si está vinculada al exchange. En el TP: `helpdesk.assignment`, `helpdesk.audit` y `helpdesk.errors`.

- **Routing key:** la "etiqueta de dirección" que el productor le pone al mensaje al publicarlo (por ejemplo `ticket.created.high`). El exchange la compara contra los patrones de los bindings para decidir a qué colas entregar el mensaje.

- **Consumer (consumidor):** el proceso que se suscribe a una cola, recibe los mensajes y los procesa. Al terminar correctamente envía un **ack** (acknowledge) para que el broker elimine el mensaje; si no hay ack, el broker puede reentregarlo. En el TP son el `assignment-worker` y el `audit-worker`.

## 3. ¿Por qué un evento debe representar algo que ya ocurrió y no una orden directa?

Porque el evento es un **hecho del sistema, inmutable y verdadero**: "el ticket TCK-001 fue creado" ya pasó, nadie puede rechazarlo ni discutirlo. Eso tiene varias consecuencias importantes:

1. **Desacoplamiento real:** si el evento fuera una orden ("asigná este ticket"), el productor estaría decidiendo qué debe hacer el consumidor, y volveríamos al acoplamiento de REST pero con un broker en el medio. Al publicar un hecho, cada consumidor decide por sí mismo cómo reaccionar: uno asigna, otro audita, mañana otro podría enviar emails, sin tocar al productor.

2. **Múltiples reacciones:** un hecho puede interesarle a N consumidores distintos. Una orden, en cambio, tiene un destinatario implícito. Nuestro `ticket.created` lo consumen a la vez el worker de asignación y el de auditoría.

3. **Semántica temporal correcta:** los eventos se nombran en pasado (`ticket.created`, `ticket.assigned`) porque describen el estado del dominio tal como quedó. Esto permite reconstruir la historia (auditoría, event sourcing) y reprocesar eventos con seguridad: releer un hecho no cambia el pasado; reejecutar una orden podría duplicar efectos.

Una orden directa es un **comando** ("hacé X"), que tiene un solo responsable y puede ser rechazado. Un evento es una **notificación de algo consumado**, y ese es el cambio de mentalidad que propone el TP: pasar de "un endpoint llama a otro" a "el sistema coordina hechos".
