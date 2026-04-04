FROM node:18-alpine
WORKDIR /app
COPY package.json server.js ./
RUN npm install --production
EXPOSE 8080
CMD ["node", "server.js"]
