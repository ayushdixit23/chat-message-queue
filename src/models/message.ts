import mongoose from "mongoose";

const messageSchema = new mongoose.Schema(
    {
        senderId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "User",
            required: true,
        },
        mesId: { type: String, unique: true, required: true },
        conversationId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "Conversation",
            required: true,
        },
        type: { type: String, default: "text", enum: ["text", "image", "video", "gif", "document"] },
        text: { type: String, required: false },
        imageUrl: { type: String, required: false },
        videoUrl: { type: String, required: false },
        gifUrl: { type: String, required: false },
        document: {
            url: { type: String, required: false },
            name: { type: String, required: false },
            size: { type: String, required: false },
        },
        deletedfor: [{ type: mongoose.Schema.Types.ObjectId, ref: "User" }],
        status: { type: String, default: "active", enum: ["active", "deleted"] },
        seenBy: { type: [mongoose.Schema.Types.ObjectId], ref: "User", default: [] },
        isSeen: { type: Boolean, default: false },
        createdAt: { type: Date },
    }
);

const Message = mongoose.model("Message", messageSchema);
export default Message;
