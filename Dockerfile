FROM node:14-alpine

WORKDIR /app

COPY app.js /app
COPY package.json /app

RUN npm install


EXPOSE 3000

CMD node app.js
