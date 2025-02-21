import amqp from 'amqplib';
import mongoose from 'mongoose';
import { MONGO_URI } from './helpers/envConfig.js';
import Message from './models/message.js';
import Conversation from './models/conversation.js';

let isMongoConnected = false;
let messageQueue:any = [];
const BATCH_SIZE = 20; 
const PROCESS_INTERVAL = 20000;

const consumeMessages = async () => {
    const exchange = "chat-app";
    const queue = "chatting";

    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertExchange(exchange, 'direct', { durable: true });
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, exchange, "chat-db");

    channel.prefetch(2);
    console.log(`[${new Date().toLocaleString()}] Waiting for messages in ${queue} queue...`);

    channel.consume(queue, async (msg) => {
        if (msg) {
            try {
                const message = JSON.parse(msg.content.toString());
                console.log('Received message:', message);

                // Store message in queue
                messageQueue.push({
                    senderId: message.senderId._id,
                    mesId: message.mesId,
                    conversationId: message.conversationId,
                    text: message.text,
                });

                channel.ack(msg);

                if (messageQueue.length >= BATCH_SIZE) {
                    await processMessages();
                }
            } catch (error) {
                console.error("Processing error:", error);
            }
        }
    }, { noAck: false });

    setInterval(async () => {
        if (messageQueue.length > 0) {
            await processMessages()
        }
    }, PROCESS_INTERVAL);
};

async function processMessages() {
    const messagesToInsert = [...messageQueue];
    messageQueue = []; 

    try {
        const savedMessages = await Message.insertMany(messagesToInsert);
        console.log(`Inserted ${savedMessages.length} messages`);

        const conversationUpdates = savedMessages.reduce((acc, msg) => {
            acc[msg.conversationId] = acc[msg.conversationId] || [];
            acc[msg.conversationId].push(msg._id);
            return acc;
        }, {});

        for (const [conversationId, messageIds] of Object.entries(conversationUpdates)) {
            await Conversation.updateOne(
                { _id: conversationId },
                {
                    $push: { messages: { $each: messageIds } },
                    $set: { lastMessage: messageIds[messageIds.length - 1] },
                }
            );
        }
    } catch (error) {
        console.error("Bulk insert error:", error);
    }
}

async function initMongoConnection() {
    if (!isMongoConnected) {
        try {
            await mongoose.connect(MONGO_URI);
            isMongoConnected = true;
            console.log('Connected to MongoDB');
        } catch (error) {
            console.error('MongoDB connection error:', error);
            process.exit(1); 
        }
    }
}
initMongoConnection()

consumeMessages().catch(console.error);
