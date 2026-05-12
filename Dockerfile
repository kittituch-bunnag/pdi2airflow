# Stage 1: build React frontend
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Python runtime
FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/
COPY run.py ./
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Optional project-wide config (kettle.properties / repositories.xml)
RUN mkdir -p config

EXPOSE 8765

CMD ["python", "run.py", "--no-browser", "--host", "0.0.0.0"]
