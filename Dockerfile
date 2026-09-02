FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . .
RUN pip install flask
RUN pip install .
RUN python -m nxrunner install nxrunner

EXPOSE 7070

CMD ["python", "-m", "nwebclient.runner", "--rest"]
# CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]
