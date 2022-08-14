FROM python:3.8-slim-buster
WORKDIR /usr/src/app
COPY ./requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY . .
EXPOSE 8100
CMD ["python", "main.py"]


