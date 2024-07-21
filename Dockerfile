# syntax=docker/dockerfile:1
FROM python:3.12-bookworm

WORKDIR /notion-calendar
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

# CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=8080"]
CMD [ "waitress-serve", "--port=8080", "--call", "notion_calendar:create_app" ]
