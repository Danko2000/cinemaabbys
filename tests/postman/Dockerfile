# tests/postman/Dockerfile
FROM node:18

WORKDIR /app

COPY package*.json ./
RUN npm install --legacy-peer-deps

# 🔹 Копируем шаблоны ДО запуска тестов
COPY reports/templates/ /app/reports/templates/

COPY . .

CMD ["npm", "run", "test:docker"]