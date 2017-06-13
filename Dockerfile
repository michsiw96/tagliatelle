FROM python:3
ADD receiver.py /
RUN pip install redis
RUN pip install pika
CMD ["python", "./receiver.py"]
