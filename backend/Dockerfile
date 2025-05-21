FROM python:3.12.6

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean

RUN apt-get update && \
    apt-get install -y xvfb && \
    apt-get clean

COPY . .

CMD ["sh", "-c", "Xvfb :1 -screen 0 1024x768x16 & export DISPLAY=:1 && python main.py"]