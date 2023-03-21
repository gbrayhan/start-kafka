const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'library-app-01',
  brokers: ['kafka:19092']
});

const topicName = 'library-events';

const produceEvent = async (eventType, book) => {
  const producer = kafka.producer();
  await producer.connect();

  const bookEvent = {
    eventType,
    ...book
  };

  await producer.send({
    topic: topicName,
    messages: [
      {
        value: JSON.stringify(bookEvent)
      }
    ]
  });

  console.log(`Mensaje enviado al tema ${topicName}:`, bookEvent);
  await producer.disconnect();
};

const consumeEvent = async () => {
  const consumer = kafka.consumer({ groupId: 'library-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: topicName });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsedMessage = JSON.parse(message.value.toString());
      console.log(`Recibido mensaje del tema ${topic}:`, parsedMessage);

      switch (parsedMessage.eventType) {
        case 'book-added':
          console.log('Se ha añadido un nuevo libro:', parsedMessage);
          break;
        case 'book-updated':
          console.log('Se ha actualizado un libro:', parsedMessage);
          break;
        case 'book-removed':
          console.log('Se ha eliminado un libro:', parsedMessage);
          break;
        default:
          console.log('Tipo de evento no reconocido:', parsedMessage.eventType);
      }
    }
  });
};

// Ejemplo de eventos
const bookAdded = {
  id: Math.floor(Math.random() * 1000),
  title: 'Nuevo libro',
  author: 'Autor desconocido'
};

const bookUpdated = {
  id: bookAdded.id,
  title: 'Libro actualizado',
  author: 'Autor actualizado'
};

const bookRemoved = {
  id: bookUpdated.id
};

produceEvent('book-added', bookAdded).catch(console.error);
produceEvent('book-added', bookAdded).catch(console.error);
produceEvent('book-added', bookAdded).catch(console.error);

produceEvent('book-updated', bookUpdated).catch(console.error);
produceEvent('book-removed', bookRemoved).catch(console.error);

consumeEvent().catch(console.error);
