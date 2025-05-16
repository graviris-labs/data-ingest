FROM python:3.9-slim

# Install Chrome and dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver using the newer method
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1) \
    && wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$(google-chrome --version | awk '{print $3}')/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    || wget -q "https://storage.googleapis.com/chrome-for-testing-public/$(google-chrome --version | awk '{print $3}')/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    || { \
        MAJOR_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1); \
        LATEST_DRIVER_URL=$(wget -qO- "https://googlechromelabs.github.io/chrome-for-testing/latest-patch-versions-per-channel.json" | grep -o "\"https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/[^\"]*linux64/chromedriver-linux64.zip\"" | head -1 | tr -d '"'); \
        wget -q "$LATEST_DRIVER_URL" -O /tmp/chromedriver.zip; \
    } \
    && unzip /tmp/chromedriver.zip -d /tmp/ \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/ \
    && rm -rf /tmp/chromedriver.zip /tmp/chromedriver-linux64 \
    && chmod +x /usr/local/bin/chromedriver

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create necessary directories
RUN mkdir -p data/db logs

# Make the entrypoint script executable
RUN chmod +x entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
