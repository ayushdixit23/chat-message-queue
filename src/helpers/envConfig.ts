import dotenv from 'dotenv';

dotenv.config();

export const NODE_ENV = process.env.NODE_ENV
export const MONGO_URI = process.env.MONGO_URI || ""
export const AWS_ACCESS_KEY = process.env.AWS_ACCESS_KEY || "";
export const AWS_SECRET_KEY = process.env.AWS_SECRET_KEY || "";
export const AWS_REGION = process.env.AWS_REGION || "";
export const BUCKET_NAME = process.env.BUCKET_NAME || "";
export const CLOUDFRONT_URL = process.env.CLOUDFRONT_URL || ""