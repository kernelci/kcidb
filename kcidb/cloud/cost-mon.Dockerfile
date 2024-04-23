FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim
RUN apt-get install -y libpq-dev
WORKDIR /app
COPY . ./
RUN pip3 install --break-system-packages .
ENV PYTHONUNBUFFERED True
ENTRYPOINT ["kcidb/cloud/cost-mon"]
CMD ["[]"]
