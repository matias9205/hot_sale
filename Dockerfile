FROM python:3.11-slim

# 🔧 Instalación de dependencias de sistema
RUN apt-get update && \
    apt-get install -y gnupg curl apt-transport-https wget unzip fonts-liberation libnss3 libgconf-2-4 libxi6 libxcb1 libxcomposite1 libasound2 libxrandr2 libxss1 libxtst6 libxdamage1 libx11-xcb1 libdbus-glib-1-2 libgtk-3-0

# 🖥️ Instalación de Google Chrome para Selenium (headless)
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome-stable_current_amd64.deb || apt-get -fy install && \
    rm google-chrome-stable_current_amd64.deb

# 🧮 Herramientas para SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools unixodbc

# 📍 Definir directorio de trabajo
WORKDIR /app

# 📦 Instalación de dependencias Python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 🔁 Copiar el resto del código
COPY . .

# 🌐 Variables de entorno
ENV SQL_SERVER_USER=sa \
    SQL_SERVER_PASS=francia92 \
    SQL_SERVER_HOST=DESKTOP-GOCIPJK \
    SQL_SERVER_DB=HotSale \
    MONGODB_DB=ETL_Mercado_Libre \
    MONGO_URI=mongodb://mongo:27017/

# 🚀 Ejecutar proceso ETL
ENTRYPOINT ["bash", "./run_etl.sh"]