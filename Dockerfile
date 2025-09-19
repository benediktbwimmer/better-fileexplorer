FROM node:20-slim AS runtime

WORKDIR /app

COPY package.json package-lock.json* ./

RUN npm install --omit=dev && npm cache clean --force

COPY . .

ENV PORT=4174
ENV START_PATH=/workspace

RUN mkdir -p "$START_PATH"

EXPOSE 4174

CMD ["npm", "start"]
