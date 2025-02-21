import amqp from 'amqplib';

const CONCURRENT_JOBS = 1;

const consumeMessages = async () => {
    const queue = "chattings"
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    await channel.assertQueue(queue, { durable: true });
    channel.prefetch(CONCURRENT_JOBS);

    console.log(`[${new Date().toLocaleString()}] Waiting for messages in ${queue} queue...`);

    channel.consume(queue, (msg) => {
        if (msg) {
            const message = JSON.parse(msg.content.toString());
            console.log('Received message:', message);
            channel.ack(msg);
        }
    }, { noAck: false });
};

// Initialize MongoDB connection
// async function initMongoConnection() {
//     if (!isMongoConnected) {
//         try {
//             await mongoose.connect(MONGO_URI);
//             isMongoConnected = true;
//             console.log('Connected to MongoDB');
//         } catch (error) {
//             console.error('MongoDB connection error:', error);
//             process.exit(1); // Exit if MongoDB connection fails
//         }
//     }
// }


consumeMessages().catch(console.error);
