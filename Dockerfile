FROM python:3.10-slim
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN python -m pip install --upgrade pip
RUN python -m pip install --no-cache-dir -r requirements.txt
EXPOSE 8000
CMD ["gunicorn", "--bind" , ":8000", "--workers", "1", "main:flask_app"]
