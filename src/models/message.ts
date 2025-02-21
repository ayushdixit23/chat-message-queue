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
        type: { type: String, default: "text", enum: ["text", "image", "video", "gif"] },
        text: { type: String, required: false },
        imageUrl: { type: String, required: false },
        videoUrl: { type: String, required: false },
        gifUrl: { type: String, required: false },
        deletedfor: [{ type: mongoose.Schema.Types.ObjectId, ref: "User" }],
        status: { type: String, default: "active", enum: ["active", "deleted"] },
    },
    { timestamps: true }
);

const Message = mongoose.model("Message", messageSchema);
export default Message;
