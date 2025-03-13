import amqp from 'amqplib';
import mongoose from 'mongoose';
import { MONGO_URI } from './helpers/envConfig.js';
import Message from './models/message.js';
import Conversation from './models/conversation.js';
import User from './models/user.js';

let isMongoConnected = false;
let messageQueue: any = [];
const BATCH_SIZE = 20;
const PROCESS_INTERVAL = 5000;

const exchange = "chat-app";

/**
 * Setup RabbitMQ connection and consumers
 */
const consumeMessages = async () => {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertExchange(exchange, 'direct', { durable: true });

    // Declare queues
    await channel.assertQueue('chat-insert', { durable: true });
    await channel.assertQueue('chat-update', { durable: true });

    await channel.bindQueue('chat-insert', exchange, 'insert');
    await channel.bindQueue('chat-update', exchange, 'update');

    console.log("Waiting for messages...");

    channel.prefetch(2);

    channel.consume('chat-insert', async (msg) => {
        if (msg) {
            try {
                const message = JSON.parse(msg.content.toString());
                console.log('Received message for insertion:', message);

                // Store message in batch queue
                messageQueue.push({
                    senderId: message.senderId._id,
                    mesId: message.mesId,
                    conversationId: message.conversationId,
                    text: message.text,
                    type: message.type,
                    imageUrl: message.imageUrl,
                    videoUrl: message.videoUrl,
                    gifUrl: message.gifUrl,
                    document: message.document || {} || null,
                    createdAt: message.createdAt,
                    seenBy: message.seenBy,
                    isSeen: message.isSeen,
                    replyMessage: message.replyMessage
                });

                channel.ack(msg);

                if (messageQueue.length >= BATCH_SIZE) {
                    await processMessages();
                }
            } catch (error) {
                console.error("Message processing error:", error);
            }
        }
    }, { noAck: false });

    channel.consume('chat-update', async (msg) => {
        if (msg) {
            try {
                const updateData = JSON.parse(msg.content.toString());
                const updateParsedData = JSON.parse(updateData);

                if (updateParsedData.actionType === "messageSeen") {
                    const messages = updateParsedData.messages;
                    
                    if (Array.isArray(messages) && messages.length > 0) {
                        const userId = updateParsedData.messageToSeenForUserId;
                
                        if (updateParsedData.isGroup) {
                            // Fetch all messages at once
                            const messageIds = messages.map(m => m.mesId);
                            const allMessages = await Message.find({ mesId: { $in: messageIds } });
                
                            // Fetch conversation data in a single query
                            const conversationIds = [...new Set(allMessages.map(m => m.conversationId))];
                            const conversations = await Conversation.find({ _id: { $in: conversationIds } });
                
                            const conversationMap = new Map(conversations.map(c => [c._id.toString(), c.users]));
                
                            // Update messages
                            const bulkOps = allMessages.map(message => {
                                if (!message.seenBy.includes(userId)) {
                                    message.seenBy.push(userId);
                
                                    const conversationUsers = conversationMap.get(message.conversationId.toString()) || [];
                                    message.isSeen = conversationUsers.length === message.seenBy.length;
                                    
                                    return {
                                        updateOne: {
                                            filter: { _id: message._id },
                                            update: { $set: { seenBy: message.seenBy, isSeen: message.isSeen } }
                                        }
                                    };
                                }
                                return null;
                            }).filter(op => op !== null);
                
                            if (bulkOps.length > 0) {
                                await Message.bulkWrite(bulkOps);
                            }
                
                        } else {
                            await Message.updateMany(
                                { mesId: { $in: messages.map(m => m.mesId) } },
                                {
                                    $addToSet: { seenBy: userId },
                                    $set: { isSeen: true }
                                }
                            );
                        }
                
                        console.log("Messages updated");
                    } else {
                        console.log("No messages to update");
                    }
                }
                

                if (updateParsedData.actionType === "deletion") {

                    if (updateParsedData.action === "deleteForEveryOne") {
                        await Message.updateOne(
                            { mesId: updateParsedData.mesId },
                            { status: "deleted" }
                        );
                        console.log("Message deleted for everyone");
                    }

                    if (updateParsedData.action === "deleteForMe") {
                        // Mark message as deleted for the user
                        await Message.updateOne(
                            { mesId: updateParsedData.mesId },
                            { $addToSet: { deletedfor: updateParsedData.userId } }
                        );
                        console.log("Message deleted for user");
                    }


                }

                if (updateParsedData.actionType === "clearChat") {
                    const messages = updateParsedData.messages;
                    if (Array.isArray(messages) && messages.length > 0) {
                        await Message.updateMany(
                            { mesId: { $in: messages } },
                            {
                                $addToSet: { deletedfor: updateParsedData.userId }
                            }
                        );
                        console.log("Messages updated");
                    } else {
                        console.log("No messages to update");
                    }
                }

                if (updateParsedData.actionType === "blockOrUnblock") {

                    console.log(`updateParsedData`, updateParsedData)

                    const query = updateParsedData.action === "block" ?
                        {
                            $addToSet: {
                                blockedConversations: updateParsedData.roomId
                            }
                        } : {
                            $pull: {
                                blockedConversations: updateParsedData.roomId
                            }
                        }

                    await User.findByIdAndUpdate(updateParsedData.userId, query)

                    console.log(`User ${updateParsedData.userId} ${updateParsedData.action} conversation ${updateParsedData.roomId}`)
                }

                channel.ack(msg);
            } catch (error) {
                console.error("Update processing error:", error);
            }
        }
    }, { noAck: false });

    setInterval(async () => {
        if (messageQueue.length > 0) {
            await processMessages();
        }
    }, PROCESS_INTERVAL);
};

/**
 * Processes and inserts messages in bulk
 */
async function processMessages() {
    const messagesToInsert = [...messageQueue];
    messageQueue = [];

    try {
        const savedMessages = await Message.insertMany(messagesToInsert);
        console.log(`Inserted ${savedMessages.length} messages`);

        // Update conversation last message
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

/**
 * MongoDB Connection
 */
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

initMongoConnection();
consumeMessages().catch(console.error);
