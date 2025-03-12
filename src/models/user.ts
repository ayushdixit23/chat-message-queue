import mongoose from "mongoose";

const userSchema = new mongoose.Schema({
  fullName: { type: String, required: true },
  userName: { type: String, required: true, unique: true },
  profilePic: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  password: { type: String },
  isGoogleUser: { type: Boolean, default: false },
  googleId: { type: String, sparse: true },
  bio: { type: String, default: "Hey there, i am using Lets chat!" },
  friends: [{ type: mongoose.Schema.Types.ObjectId, ref: "User" }],
  sentFriendRequests: [{ type: mongoose.Schema.Types.ObjectId, ref: "User" }],
  conversation: [{ type: mongoose.Schema.Types.ObjectId, ref: "Conversation" }],
  blockedConversations:[{ type: mongoose.Schema.Types.ObjectId, ref: "Conversation" }],
});

const User = mongoose.model("User", userSchema);

export default User;
