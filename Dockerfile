# Use the official Node.js Alpine image as a base image
FROM node:16-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json first to install dependencies
COPY package*.json ./

# Install the app dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Run the build step (assuming your package.json has a "build" script)
RUN npm run build

# Start the app
CMD ["npm", "start"]
